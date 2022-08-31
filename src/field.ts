import * as arrow from "apache-arrow";

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

  const format = parseFormat(dataView, ptr);
  const namePtr = dataView.getInt32(ptr + 4, true);
  const metadataPtr = dataView.getInt32(ptr + 8, true);

  const name = parseNullTerminatedString(dataView, namePtr);
  const metadata = parseNullTerminatedString(dataView, metadataPtr);

  // Extra 4 to be 8-byte aligned
  const flags = dataView.getBigInt64(ptr + 16, true);
  const nChildren = dataView.getBigInt64(ptr + 24, true);

  // TODO: parse children and

  return arrow.Field.new(name, format, undefined);
}

function parseFormat(dataView: DataView, ptr: number): arrow.DataType {
  const formatPtr = dataView.getInt32(ptr, true);
  const format = parseNullTerminatedString(dataView, formatPtr);

  const staticType = formatMapping[format];
  if (staticType) {
    return staticType;
  }

  throw new Error(`Unsupported format: ${format}`);
}

function parseNullTerminatedString(
  dataView: DataView,
  ptr: number,
  maxBytesToRead: number = Infinity
) {
  const maxPtr = Math.min(ptr + maxBytesToRead, dataView.byteLength);
  let end = ptr;
  while (end < maxPtr && dataView.getUint8(end) !== 0) {
    end += 1;
  }

  return new TextDecoder("utf-8").decode(
    new Uint8Array(dataView.buffer, ptr, end - ptr)
  );
}
