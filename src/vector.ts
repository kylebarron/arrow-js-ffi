// // @ts-nocheck

import * as arrow from "apache-arrow";
import { DataType, Int, Type, Vector } from "apache-arrow";
import { DataProps } from "apache-arrow/data";

type ParsedBuffers = {
  nullBitmap?: Uint8Array | null;
  data?: any;
  valueOffsets?: any;
};

const PRIMITIVE_TYPES = [
  Type.Bool,
  Type.Int,
  Type.Int8,
  Type.Int16,
  Type.Int32,
  Type.Int64,
  Type.Uint8,
  Type.Uint16,
  Type.Uint32,
  Type.Uint64,
  Type.Float,
  Type.Float16,
  Type.Float32,
  Type.Float64,
];

const VARIABLE_BINARY_TYPES = [Type.Binary, Type.Utf8];

/**
 * Parse vector from FFI
 */
export function parseVector<T extends DataType>(
  buffer: ArrayBuffer,
  ptr: number,
  dataType: T
): Vector<T> {
  const dataView = new DataView(buffer);

  const length = dataView.getBigInt64(ptr, true);
  const nullCount = dataView.getBigInt64(ptr + 8, true);
  const offset = dataView.getBigInt64(ptr + 16, true);
  const nBuffers = dataView.getBigInt64(ptr + 24, true);
  const nChildren = dataView.getBigInt64(ptr + 32, true);

  const ptrToBufferPtrs = dataView.getUint32(ptr + 40, true);
  const bufferPtrs = new Uint32Array(Number(nBuffers));
  for (let i = 0; i < nBuffers; i++) {
    bufferPtrs[i] = dataView.getUint32(ptrToBufferPtrs + i * 4, true);
  }

  const buffers = parseBuffers(dataView, bufferPtrs, Number(length), dataType);

  const arrowData = arrow.makeData({
    type: dataType,
    offset: Number(offset),
    length: Number(length),
    nullCount: Number(nullCount),
    ...buffers,
  });

  return arrow.makeVector(arrowData);
}

/**
 * [parseBuffers description]
 *
 * @return  {[type]}  [return description]
 */
function parseBuffers(
  dataView: DataView,
  bufferPtrs: Uint32Array,
  length: number,
  dataType: DataType,
  copy: boolean = false
): ParsedBuffers {
  if (PRIMITIVE_TYPES.includes(dataType.typeId)) {
    const validityPtr = bufferPtrs[0];
    // TODO: parse validity bitmaps
    const nullBitmap = validityPtr === 0 ? null : null;

    const dataPtr = bufferPtrs[1];
    const data = copy
      ? new dataType.ArrayType(copyBuffer(dataView.buffer, dataPtr, length))
      : new dataType.ArrayType(dataView.buffer, dataPtr, length);
    return {
      nullBitmap,
      data,
    };
  }

  if (VARIABLE_BINARY_TYPES.includes(dataType.typeId)) {
    const [validityPtr, offsetsPtr, dataPtr] = bufferPtrs;
    // TODO: parse validity bitmaps
    const nullBitmap = validityPtr === 0 ? null : null;

    // const valueOffsets = new Int32Array(dataView.buffer, offsetsPtr, length);
    const valueOffsets = new Int32Array([0, 1, 2, 3, 4])
    const data = new dataType.ArrayType(dataView.buffer, dataPtr, length);

    return {
      nullBitmap,
      valueOffsets,
      data,
    };
  }

  throw new Error("Not implemented");
}

/** Copy existing buffer into new buffer */
function copyBuffer(
  buffer: ArrayBuffer,
  ptr: number,
  length: number
): ArrayBuffer {
  const newBuffer = new ArrayBuffer(length);
  const newBufferView = new Uint8Array(newBuffer);
  const existingView = new Uint8Array(buffer, ptr, length);

  for (let i = 0; i < length; i++) {
    newBufferView[i] = existingView[i];
  }

  return newBuffer;
}
