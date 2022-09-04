import * as arrow from "apache-arrow";
import { DataType } from "apache-arrow";

type NullBitmap = Uint8Array | null | undefined;

export function parseVector<T extends DataType>(
  buffer: ArrayBuffer,
  ptr: number,
  dataType: T,
  copy: boolean = false
): arrow.Vector<T> {
  const data = parseData(buffer, ptr, dataType, copy);
  return arrow.makeVector(data);
}

function parseData<T extends DataType>(
  buffer: ArrayBuffer,
  ptr: number,
  dataType: T,
  copy: boolean = false
): arrow.Data<T> {
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
  const children = new Array(Number(nChildren));
  for (let i = 0; i < nChildren; i++) {
    children[i] = parseVector(
      buffer,
      dataView.getUint32(ptrToChildrenPtrs + i * 4, true),
      dataType.children[i].type
    );
  }

  if (DataType.isNull(dataType)) {
    return arrow.makeData({
      type: dataType,
      offset,
      length,
    });
  }

  if (DataType.isInt(dataType)) {
    const [validityPtr, dataPtr] = bufferPtrs;
    const nullBitmap = parseNullBitmap(validityPtr);
    console.log("dataPtr", dataPtr);
    console.log("length", length);

    const data = copy
      ? new dataType.ArrayType(copyBuffer(dataView.buffer, dataPtr, length))
      : new dataType.ArrayType(dataView.buffer, dataPtr, length);
    console.log("data", data);
    return arrow.makeData({
      type: dataType,
      offset,
      length,
      nullCount,
      data,
      nullBitmap,
    });
  }

  if (DataType.isFloat(dataType)) {
    const [validityPtr, dataPtr] = bufferPtrs;
    const nullBitmap = parseNullBitmap(validityPtr);
    const data = copy
      ? new dataType.ArrayType(copyBuffer(dataView.buffer, dataPtr, length))
      : new dataType.ArrayType(dataView.buffer, dataPtr, length);
    return arrow.makeData({
      type: dataType,
      offset,
      length,
      nullCount,
      data,
      nullBitmap,
    });
  }

  if (DataType.isBool(dataType)) {
    const [validityPtr, dataPtr] = bufferPtrs;
    const nullBitmap = parseNullBitmap(validityPtr);
    const data = copy
      ? new dataType.ArrayType(copyBuffer(dataView.buffer, dataPtr, length))
      : new dataType.ArrayType(dataView.buffer, dataPtr, length);
    return arrow.makeData({
      type: dataType,
      offset,
      length,
      nullCount,
      data,
      nullBitmap,
    });
  }

  if (DataType.isDecimal(dataType)) {
    const [validityPtr, dataPtr] = bufferPtrs;
    const nullBitmap = parseNullBitmap(validityPtr);
    const data = copy
      ? new dataType.ArrayType(copyBuffer(dataView.buffer, dataPtr, length))
      : new dataType.ArrayType(dataView.buffer, dataPtr, length);
    return arrow.makeData({
      type: dataType,
      offset,
      length,
      nullCount,
      data,
      nullBitmap,
    });
  }

  if (DataType.isDate(dataType)) {
    const [validityPtr, dataPtr] = bufferPtrs;
    const nullBitmap = parseNullBitmap(validityPtr);
    const data = copy
      ? new dataType.ArrayType(copyBuffer(dataView.buffer, dataPtr, length))
      : new dataType.ArrayType(dataView.buffer, dataPtr, length);
    return arrow.makeData({
      type: dataType,
      offset,
      length,
      nullCount,
      data,
      nullBitmap,
    });
  }

  if (DataType.isTime(dataType)) {
    const [validityPtr, dataPtr] = bufferPtrs;
    const nullBitmap = parseNullBitmap(validityPtr);
    const data = copy
      ? new dataType.ArrayType(copyBuffer(dataView.buffer, dataPtr, length))
      : new dataType.ArrayType(dataView.buffer, dataPtr, length);
    return arrow.makeData({
      type: dataType,
      offset,
      length,
      nullCount,
      data,
      nullBitmap,
    });
  }

  if (DataType.isTimestamp(dataType)) {
    const [validityPtr, dataPtr] = bufferPtrs;
    const nullBitmap = parseNullBitmap(validityPtr);
    const data = copy
      ? new dataType.ArrayType(copyBuffer(dataView.buffer, dataPtr, length))
      : new dataType.ArrayType(dataView.buffer, dataPtr, length);
    return arrow.makeData({
      type: dataType,
      offset,
      length,
      nullCount,
      data,
      nullBitmap,
    });
  }

  if (DataType.isInterval(dataType)) {
    const [validityPtr, dataPtr] = bufferPtrs;
    const nullBitmap = parseNullBitmap(validityPtr);
    const data = copy
      ? new dataType.ArrayType(copyBuffer(dataView.buffer, dataPtr, length))
      : new dataType.ArrayType(dataView.buffer, dataPtr, length);
    return arrow.makeData({
      type: dataType,
      offset,
      length,
      nullCount,
      data,
      nullBitmap,
    });
  }

  if (DataType.isBinary(dataType)) {
    const [validityPtr, offsetsPtr, dataPtr] = bufferPtrs;
    const nullBitmap = parseNullBitmap(validityPtr);
    const valueOffsets = new Int32Array(
      dataView.buffer,
      offsetsPtr,
      length + 1
    );
    const data = copy
      ? new dataType.ArrayType(copyBuffer(dataView.buffer, dataPtr, length))
      : new dataType.ArrayType(dataView.buffer, dataPtr, length);
    return arrow.makeData({
      type: dataType,
      offset,
      length,
      nullCount,
      nullBitmap,
      valueOffsets,
      data,
    });
  }

  if (DataType.isUtf8(dataType)) {
    const [validityPtr, offsetsPtr, dataPtr] = bufferPtrs;
    const nullBitmap = parseNullBitmap(validityPtr);
    const valueOffsets = new Int32Array(
      dataView.buffer,
      offsetsPtr,
      length + 1
    );
    const data = copy
      ? new dataType.ArrayType(copyBuffer(dataView.buffer, dataPtr, length))
      : new dataType.ArrayType(dataView.buffer, dataPtr, length);
    return arrow.makeData({
      type: dataType,
      offset,
      length,
      nullCount,
      nullBitmap,
      valueOffsets,
      data,
    });
  }

  if (DataType.isFixedSizeBinary(dataType)) {
    const [validityPtr, dataPtr] = bufferPtrs;
    const nullBitmap = parseNullBitmap(validityPtr);
    const data = copy
      ? new dataType.ArrayType(copyBuffer(dataView.buffer, dataPtr, length))
      : new dataType.ArrayType(dataView.buffer, dataPtr, length);
    return arrow.makeData({
      type: dataType,
      offset,
      length,
      nullCount,
      nullBitmap,
      data,
    });
  }

  if (DataType.isList(dataType)) {
    assert(nChildren === 1);
    const [validityPtr, offsetsPtr] = bufferPtrs;
    const nullBitmap = parseNullBitmap(validityPtr);
    const valueOffsets = new Int32Array(
      dataView.buffer,
      offsetsPtr,
      length + 1
    );
    return arrow.makeData({
      type: dataType,
      offset,
      length,
      nullCount,
      nullBitmap,
      valueOffsets,
      child: children[0],
    });
  }

  if (DataType.isFixedSizeList(dataType)) {
    assert(nChildren === 1);
    const [validityPtr] = bufferPtrs;
    const nullBitmap = parseNullBitmap(validityPtr);
    return arrow.makeData({
      type: dataType,
      offset,
      length,
      nullCount,
      nullBitmap,
      child: children[0],
    });
  }

  if (DataType.isStruct(dataType)) {
    const [validityPtr] = bufferPtrs;
    const nullBitmap = parseNullBitmap(validityPtr);
    return arrow.makeData({
      type: dataType,
      offset,
      length,
      nullCount,
      nullBitmap,
      children,
    });
  }

  // TODO: sparse union, dense union, dictionary
  throw new Error(`Unsupported type ${dataType}`);
}

function parseNullBitmap(validityPtr: number): NullBitmap {
  // TODO: parse validity bitmaps
  const nullBitmap = validityPtr === 0 ? null : null;
  return nullBitmap;
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

function assert(a: boolean): void {
  if (!a) throw new Error(`assertion failed`);
}
