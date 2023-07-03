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
  const children: arrow.Vector[] = new Array(Number(nChildren));
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
    const nullBitmap = parseNullBitmap(dataView.buffer, validityPtr, copy);
    const byteLength = (length * dataType.bitWidth) / 8;
    const data = copy
      ? new dataType.ArrayType(copyBuffer(dataView.buffer, dataPtr, byteLength))
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

  if (DataType.isFloat(dataType)) {
    const [validityPtr, dataPtr] = bufferPtrs;
    const nullBitmap = parseNullBitmap(dataView.buffer, validityPtr, copy);
    // bitwidth doesn't exist on float types I guess
    const byteLength = length * dataType.ArrayType.BYTES_PER_ELEMENT;
    const data = copy
      ? new dataType.ArrayType(copyBuffer(dataView.buffer, dataPtr, byteLength))
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
    const nullBitmap = parseNullBitmap(dataView.buffer, validityPtr, copy);
    const byteLength = length * 1;
    const data = copy
      ? new dataType.ArrayType(copyBuffer(dataView.buffer, dataPtr, byteLength))
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
    const nullBitmap = parseNullBitmap(dataView.buffer, validityPtr, copy);
    const byteLength = (length * dataType.bitWidth) / 8;
    const data = copy
      ? new dataType.ArrayType(copyBuffer(dataView.buffer, dataPtr, byteLength))
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
    const nullBitmap = parseNullBitmap(dataView.buffer, validityPtr, copy);

    // What's the bitwidth of date type?
    if (copy) {
      throw new Error("Not yet implemented");
    }
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
    const nullBitmap = parseNullBitmap(dataView.buffer, validityPtr, copy);
    const byteLength = (length * dataType.bitWidth) / 8;
    const data = copy
      ? new dataType.ArrayType(copyBuffer(dataView.buffer, dataPtr, byteLength))
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
    const nullBitmap = parseNullBitmap(dataView.buffer, validityPtr, copy);

    // What's the bitwidth here?
    if (copy) {
      throw new Error("Not yet implemented");
    }

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
    const nullBitmap = parseNullBitmap(dataView.buffer, validityPtr, copy);

    // What's the bitwidth here?
    if (copy) {
      throw new Error("Not yet implemented");
    }
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
    const nullBitmap = parseNullBitmap(dataView.buffer, validityPtr, copy);

    const valueOffsets = copy
      ? new Int32Array(
          copyBuffer(dataView.buffer, offsetsPtr, (length + 1) * 4)
        )
      : new Int32Array(dataView.buffer, offsetsPtr, length + 1);
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
    const nullBitmap = parseNullBitmap(dataView.buffer, validityPtr, copy);

    const valueOffsets = copy
      ? new Int32Array(
          copyBuffer(dataView.buffer, offsetsPtr, (length + 1) * 4)
        )
      : new Int32Array(dataView.buffer, offsetsPtr, length + 1);
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
    const nullBitmap = parseNullBitmap(dataView.buffer, validityPtr, copy);
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
    const nullBitmap = parseNullBitmap(dataView.buffer, validityPtr, copy);
    const valueOffsets = copy
      ? new Int32Array(
          copyBuffer(dataView.buffer, offsetsPtr, (length + 1) * 4)
        )
      : new Int32Array(dataView.buffer, offsetsPtr, length + 1);

    assert(children[0].data.length === 1);
    let childData = children[0].data[0];

    return arrow.makeData({
      type: dataType,
      offset,
      length,
      nullCount,
      nullBitmap,
      valueOffsets,
      child: childData,
    });
  }

  if (DataType.isFixedSizeList(dataType)) {
    assert(nChildren === 1);
    const [validityPtr] = bufferPtrs;
    const nullBitmap = parseNullBitmap(dataView.buffer, validityPtr, copy);

    assert(children[0].data.length === 1);
    let childData = children[0].data[0];

    return arrow.makeData({
      type: dataType,
      offset,
      length,
      nullCount,
      nullBitmap,
      child: childData,
    });
  }

  if (DataType.isStruct(dataType)) {
    const [validityPtr] = bufferPtrs;
    const nullBitmap = parseNullBitmap(dataView.buffer, validityPtr, copy);

    let childData = children.map(child => {
        assert(child.data.length === 1);
        return child.data[0];
    })

    return arrow.makeData({
      type: dataType,
      offset,
      length,
      nullCount,
      nullBitmap,
      children: childData,
    });
  }

  // TODO: sparse union, dense union, dictionary
  throw new Error(`Unsupported type ${dataType}`);
}

function parseNullBitmap(
  buffer: ArrayBuffer,
  validityPtr: number,
  copy: boolean
): NullBitmap {
  // TODO: parse validity bitmaps
  const nullBitmap = validityPtr === 0 ? null : null;
  return nullBitmap;
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

function assert(a: boolean): void {
  if (!a) throw new Error(`assertion failed`);
}
