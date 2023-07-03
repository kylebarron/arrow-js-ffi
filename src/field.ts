// @ts-nocheck

import * as arrow from "apache-arrow";
import { assert } from "./vector";

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
  const metadata = parseMetadata(dataView, metadataPtr);

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

  const primitiveType = formatMapping[formatString];
  if (primitiveType) {
    return new arrow.Field(name, primitiveType, undefined, metadata);
  }

  // struct
  if (formatString === "+s") {
    const type = new arrow.Struct(childrenFields);
    return new arrow.Field(name, type, undefined, metadata);
  }

  // list
  if (formatString === "+l") {
    assert(childrenFields.length === 1);
    const type = new arrow.List(childrenFields[0]);
    return new arrow.Field(name, type, undefined, metadata);
  }

  // FixedSizeBinary
  if (formatString.slice(0, 2) === "w:") {
    // The size of the binary is the integer after the colon
    const byteWidth = parseInt(formatString.slice(2));
    const type = new arrow.FixedSizeBinary(byteWidth);
    return new arrow.Field(name, type, undefined, metadata);
  }

  // FixedSizeList
  if (formatString.slice(0, 3) === "+w:") {
    assert(childrenFields.length === 1);
    // The size of the list is the integer after the colon
    const innerSize = parseInt(formatString.slice(3));
    const type = new arrow.FixedSizeList(innerSize, childrenFields[0]);
    return new arrow.Field(name, type, undefined, metadata);
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

/**
 * Parse field metadata
 *
 * The metadata format is described here:
 * https://arrow.apache.org/docs/format/CDataInterface.html#c.ArrowSchema.metadata
 */
function parseMetadata(
  dataView: DataView,
  ptr: number
): Map<string, string> | null {
  const numEntries = dataView.getInt32(ptr, true);
  if (numEntries === 0) {
    return null;
  }

  const metadata: Map<string, string> = new Map();

  ptr += 4;
  for (let i = 0; i < numEntries; i++) {
    const keyByteLength = dataView.getInt32(ptr, true);
    ptr += 4;
    const key = UTF8_DECODER.decode(
      new Uint8Array(dataView.buffer, ptr, keyByteLength)
    );
    ptr += keyByteLength;

    const valueByteLength = dataView.getInt32(ptr, true);
    ptr += 4;
    const value = UTF8_DECODER.decode(
      new Uint8Array(dataView.buffer, ptr, valueByteLength)
    );
    ptr += valueByteLength;

    metadata.set(key, value);
  }

  return metadata;
}
