import { ArrowArray, ArrowSchema } from "../nanoarrow";
import { ArrowStaticType } from "../nanoarrow/schema";

const PRIMITIVE_DATA_TYPES: string[] = [
  ArrowStaticType.Bool,
  ArrowStaticType.Int8,
  ArrowStaticType.Uint8,
  ArrowStaticType.Int16,
  ArrowStaticType.Uint16,
  ArrowStaticType.Int32,
  ArrowStaticType.Uint32,
  ArrowStaticType.Int64,
  ArrowStaticType.Uint64,
  ArrowStaticType.Float16,
  ArrowStaticType.Float32,
  ArrowStaticType.Float64,
  ArrowStaticType.DateDay,
  ArrowStaticType.DateMillisecond,
  ArrowStaticType.TimeSecond,
  ArrowStaticType.TimeMillisecond,
  ArrowStaticType.TimeMicrosecond,
  ArrowStaticType.TimeNanosecond,
  ArrowStaticType.DurationSecond,
  ArrowStaticType.DurationMillisecond,
  ArrowStaticType.DurationMicrosecond,
  ArrowStaticType.DurationNanosecond,
  ArrowStaticType.IntervalYearMonth,
  ArrowStaticType.IntervalDayTime,
  ArrowStaticType.IntervalMonthDayNanosecond,
];
const VARIABLE_BINARY_STRING_DATA_TYPES: string[] = [
  ArrowStaticType.Binary,
  ArrowStaticType.LargeBinary,
  ArrowStaticType.String,
  ArrowStaticType.LargeString,
];

/**
 * Parse an [`ArrowArray`](https://arrow.apache.org/docs/format/CDataInterface.html#the-arrowarray-structure) C FFI struct into an `ArrowArray` instance.
 *
 * - `buffer` (`ArrayBuffer`): The [`WebAssembly.Memory`](https://developer.mozilla.org/en-US/docs/WebAssembly/JavaScript_interface/Memory) instance to read from.
 * - `ptr` (`number`): The numeric pointer in `buffer` where the C struct is located.
 * - `schema` (`ArrowSchema`): The type of the vector to parse. This is the result of `parseArrowSchema`.
 * - `copy` (`boolean`): If `true`, will _copy_ data across the Wasm boundary, allowing you to delete the copy on the Wasm side. If `false`, the resulting `arrow.Vector` objects will be _views_ on Wasm memory. This requires careful usage as the arrays will become invalid if the memory region in Wasm changes.
 *
 */
export function parseArrowArray(
  buffer: ArrayBuffer,
  ptr: number,
  schema: ArrowSchema,
  copy: boolean = false
): ArrowArray {
  const dataView = new DataView(buffer);

  const length = Number(dataView.getBigInt64(ptr, true));
  const nullCount = Number(dataView.getBigInt64(ptr + 8, true));
  // TODO: if copying to a JS owned buffer, should this offset always be 0?
  const offset = Number(dataView.getBigInt64(ptr + 16, true));
  const nBuffers = Number(dataView.getBigInt64(ptr + 24, true));
  const nChildren = Number(dataView.getBigInt64(ptr + 32, true));

  const ptrToBufferPtrs = dataView.getUint32(ptr + 40, true);
  const bufferPtrs = new Uint32Array(Number(nBuffers));
  for (let i = 0; i < nBuffers; i++) {
    bufferPtrs[i] = dataView.getUint32(ptrToBufferPtrs + i * 4, true);
  }

  const ptrToChildrenPtrs = dataView.getUint32(ptr + 44, true);
  const children: ArrowArray[] = new Array(Number(nChildren));
  for (let i = 0; i < nChildren; i++) {
    children[i] = parseArrowArray(
      buffer,
      dataView.getUint32(ptrToChildrenPtrs + i * 4, true),
      schema.children[i],
      copy
    );
  }

  const buffers = parseBuffers(buffer, bufferPtrs, length, schema.format, copy);

  return {
    length,
    nullCount,
    offset,
    buffers,
    children,
    dictionary: null,
  };
}

function parseBuffers(
  buffer: ArrayBuffer,
  bufferPtrs: Uint32Array,
  length: number,
  format: string,
  copy: boolean
): (Uint8Array | null)[] {
  if (isNullDataType(format)) {
    return [];
  }

  // Special case this from the primitive data types because its data values are bit packed
  if (isBoolDataType(format)) {
    const [validityPtr, dataPtr] = bufferPtrs;
    const validity = getBitPackedArray(buffer, validityPtr, length, copy);
    const data = getBitPackedArray(buffer, dataPtr, length, copy);
    return [validity, data];
  }

  if (isPrimitiveDataType(format)) {
    const [validityPtr, dataPtr] = bufferPtrs;
    const validity = getBitPackedArray(buffer, validityPtr, length, copy);
    const data = getDataArray(buffer, dataPtr, length, format, copy);
    return [validity, data];
  }

  // This includes string
  if (isVariableBinaryDataType(format)) {
    const [validityPtr, offsetsPtr, dataPtr] = bufferPtrs;
    const validity = getBitPackedArray(buffer, validityPtr, length, copy);
    const offsets = getOffsetsArray(buffer, offsetsPtr, length, format, copy);
    const data = getDataArray(buffer, dataPtr, length, format, copy);
    return [validity, offsets, data];
  }

  if (isListDataType(format)) {
    const [validityPtr, offsetsPtr] = bufferPtrs;
    const validity = getBitPackedArray(buffer, validityPtr, length, copy);
    const offsets = getOffsetsArray(buffer, offsetsPtr, length, format, copy);
    return [validity, offsets];
  }

  if (isFixedSizeListDataType(format) || isStructDataType(format)) {
    const [validityPtr] = bufferPtrs;
    const validity = getBitPackedArray(buffer, validityPtr, length, copy);
    return [validity];
  }

  // TODO: unions

  // TODO:
  throw new Error("unimplemented");
}

function isNullDataType(format: string): boolean {
  return format === ArrowStaticType.Null;
}

function isBoolDataType(format: string): boolean {
  return format === ArrowStaticType.Bool;
}

function isPrimitiveDataType(format: string): boolean {
  // TODO: include decimals, timestamps with timezone
  return PRIMITIVE_DATA_TYPES.includes(format);
}

function isVariableBinaryDataType(format: string): boolean {
  return VARIABLE_BINARY_STRING_DATA_TYPES.includes(format);
}

function isListDataType(format: string): boolean {
  return (
    format === ArrowStaticType.List || format === ArrowStaticType.LargeList
  );
}

function isFixedSizeListDataType(format: string): boolean {
  return format.startsWith("+w:");
}

function isStructDataType(format: string): boolean {
  return format === ArrowStaticType.Struct;
}

function getBitPackedArray(
  buffer: ArrayBuffer,
  ptr: number,
  length: number,
  copy: boolean
): Uint8Array | null {
  const validityByteLength = Math.ceil(length / 8);
  if (ptr === 0 || validityByteLength === 0) {
    return null;
  }

  return copy
    ? new Uint8Array(copyBuffer(buffer, ptr, validityByteLength))
    : new Uint8Array(buffer, ptr, validityByteLength);
}

function getDataArray(
  buffer: ArrayBuffer,
  ptr: number,
  length: number,
  format: string,
  copy: boolean
): Uint8Array | null {
  const byteLength = length * getByteWidth(format);
  if (byteLength === 0) {
    return null;
  }

  return copy
    ? new Uint8Array(copyBuffer(buffer, ptr, byteLength))
    : new Uint8Array(buffer, ptr, byteLength);
}

function getOffsetsArray(
  buffer: ArrayBuffer,
  ptr: number,
  length: number,
  format: string,
  copy: boolean
): Uint8Array | null {
  const byteLength = length * getOffsetsByteWidth(format);
  if (byteLength === 0) {
    return null;
  }

  return copy
    ? new Uint8Array(copyBuffer(buffer, ptr, byteLength))
    : new Uint8Array(buffer, ptr, byteLength);
}

function getByteWidth(format: string): number {
  // TODO: duration, interval, decimal
  switch (format) {
    case ArrowStaticType.Int8:
    case ArrowStaticType.Uint8:
      return 1;

    case ArrowStaticType.Int16:
    case ArrowStaticType.Uint16:
    case ArrowStaticType.Float16:
      return 2;

    case ArrowStaticType.Int32:
    case ArrowStaticType.Uint32:
    case ArrowStaticType.Float32:
    case ArrowStaticType.DateDay:
    case ArrowStaticType.TimeSecond:
    case ArrowStaticType.TimeMillisecond:
      return 4;

    case ArrowStaticType.Int64:
    case ArrowStaticType.Uint64:
    case ArrowStaticType.Float64:
    case ArrowStaticType.DateMillisecond:
    case ArrowStaticType.TimeMicrosecond:
    case ArrowStaticType.TimeNanosecond:
      return 8;

    default:
      break;
  }

  assertUnreachable();
}

function getOffsetsByteWidth(format: string): number {
  switch (format) {
    case ArrowStaticType.List:
    case ArrowStaticType.Binary:
    case ArrowStaticType.String:
      return 4;
    case ArrowStaticType.LargeList:
    case ArrowStaticType.LargeBinary:
    case ArrowStaticType.LargeString:
      return 8;
  }

  assertUnreachable();
}

/** Copy existing buffer into new buffer */
function copyBuffer(
  buffer: ArrayBuffer,
  ptr: number,
  byteLength: number
): ArrayBuffer {
  const newBuffer = new ArrayBuffer(byteLength);
  const newBufferView = new Uint8Array(newBuffer);
  const existingView = new Uint8Array(buffer, ptr, byteLength);
  newBufferView.set(existingView);
  return newBuffer;
}

export function assert(a: boolean): void {
  if (!a) throw new Error(`assertion failed`);
}

function assertUnreachable(): never {
  throw new Error("unreachable");
}
