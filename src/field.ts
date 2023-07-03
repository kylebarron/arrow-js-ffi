// @ts-nocheck

import * as arrow from "apache-arrow";

const UTF8_DECODER = new TextDecoder("utf-8");
const formatMapping: Record<string, arrow.DataType | undefined> = {
  n: new arrow.Null(),
  b: new arrow.Bool(),
  c: new arrow.Int8(),
  C: new arrow.Uint8(),
  s: new arrow.Int16(),
  S: new arrow.Uint16(),
  i: new arrow.Int32(),
  I: new arrow.Uint32(),
  l: new arrow.Int64(),
  L: new arrow.Uint64(),
  e: new arrow.Float16(),
  f: new arrow.Float32(),
  g: new arrow.Float64(),
  z: new arrow.Binary(),
  // Z: Type.LargeBinary,
  u: new arrow.Utf8(),
  // U: Type.LargeUtf8,

  // TODO: support time, nested types, etc
};

/**
 * Parse Field from Arrow C Data Interface
 */
export function parseField(buffer: ArrayBuffer, ptr: number) {
  const dataView = new DataView(buffer);

  const formatPtr = dataView.getUint32(ptr, true);
  const formatString = parseNullTerminatedString(dataView, formatPtr);
  const namePtr = dataView.getUint32(ptr + 4, true);
  const metadataPtr = dataView.getUint32(ptr + 8, true);

  const name = parseNullTerminatedString(dataView, namePtr);
  const metadata = parseNullTerminatedString(dataView, metadataPtr);

  // Extra 4 to be 8-byte aligned
  const flags = dataView.getBigInt64(ptr + 16, true);
  const nChildren = dataView.getBigInt64(ptr + 24, true);

  const ptrToChildrenPtrs = dataView.getUint32(ptr + 32, true);
  const childrenFields: arrow.Field[] = new Array(Number(nChildren));
  for (let i = 0; i < nChildren; i++) {
    childrenFields[i] = parseField(
      buffer,
      dataView.getUint32(ptrToChildrenPtrs + i * 4, true)
    );
  }

  // console.log('childrenFields', childrenFields)

  const primitiveType = formatMapping[formatString];
  if (primitiveType) {
    return new arrow.Field(name, primitiveType, undefined);
  }

  // struct
  if (formatString === "+s") {
    const type = new arrow.Struct(childrenFields);
    return new arrow.Field(name, type);
  }

  if (formatString[0] === "+") {
    const fixedSizeListMatch = /\+w:(\d+)/.exec(formatString);
    if (fixedSizeListMatch) {
      const type = new arrow.FixedSizeList(
        parseInt(fixedSizeListMatch[1]),
        childrenFields[0]
      );
      return new arrow.Field(name, type, undefined);
    }
  }

  throw new Error(`Unsupported format: ${formatString}`);
}

/** Parse a null-terminated C-style string */
function parseNullTerminatedString(
  dataView: DataView,
  ptr: number,
  maxBytesToRead: number = Infinity
): string {
  const maxPtr = Math.min(ptr + maxBytesToRead, dataView.byteLength);
  let end = ptr;
  while (end < maxPtr && dataView.getUint8(end) !== 0) {
    end += 1;
  }

  return UTF8_DECODER.decode(new Uint8Array(dataView.buffer, ptr, end - ptr));
}
