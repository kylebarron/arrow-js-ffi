import * as arrow from "apache-arrow";
import { DataType } from "apache-arrow";
import { LargeList, isLargeList } from "./types";

type NullBitmap = Uint8Array | null | undefined;

/**
Parse an [`ArrowArray`](https://arrow.apache.org/docs/format/CDataInterface.html#the-arrowarray-structure) C FFI struct into an [`arrow.Vector`](https://arrow.apache.org/docs/js/classes/Arrow_dom.Vector.html) instance. Multiple `Vector` instances can be joined to make an [`arrow.Table`](https://arrow.apache.org/docs/js/classes/Arrow_dom.Table.html).

- `buffer` (`ArrayBuffer`): The [`WebAssembly.Memory`](https://developer.mozilla.org/en-US/docs/WebAssembly/JavaScript_interface/Memory) instance to read from.
- `ptr` (`number`): The numeric pointer in `buffer` where the C struct is located.
- `dataType` (`arrow.DataType`): The type of the vector to parse. This is retrieved from `field.type` on the result of `parseField`.
- `copy` (`boolean`, default: `true`): If `true`, will _copy_ data across the Wasm boundary, allowing you to delete the copy on the Wasm side. If `false`, the resulting `arrow.Vector` objects will be _views_ on Wasm memory. This requires careful usage as the arrays will become invalid if the memory region in Wasm changes.

#### Example

```ts
const WASM_MEMORY: WebAssembly.Memory = ...
const copiedVector = parseVector(WASM_MEMORY.buffer, arrayPtr, field.type);
// Make zero-copy views instead of copying array contents
const viewedVector = parseVector(WASM_MEMORY.buffer, arrayPtr, field.type, false);
 */
export function parseVector<T extends DataType>(
  buffer: ArrayBuffer,
  ptr: number,
  dataType: T,
  copy: boolean = true,
): arrow.Vector<T> {
  const data = parseData(buffer, ptr, dataType, copy);
  return arrow.makeVector(data);
}

/**
Parse an [`ArrowArray`](https://arrow.apache.org/docs/format/CDataInterface.html#the-arrowarray-structure) C FFI struct into an [`arrow.Data`](https://arrow.apache.org/docs/js/classes/Arrow_dom.Data.html) instance. Multiple `Data` instances can be joined to make an [`arrow.Vector`](https://arrow.apache.org/docs/js/classes/Arrow_dom.Vector.html).

- `buffer` (`ArrayBuffer`): The [`WebAssembly.Memory`](https://developer.mozilla.org/en-US/docs/WebAssembly/JavaScript_interface/Memory) instance to read from.
- `ptr` (`number`): The numeric pointer in `buffer` where the C struct is located.
- `dataType` (`arrow.DataType`): The type of the vector to parse. This is retrieved from `field.type` on the result of `parseField`.
- `copy` (`boolean`, default: `true`): If `true`, will _copy_ data across the Wasm boundary, allowing you to delete the copy on the Wasm side. If `false`, the resulting `arrow.Data` objects will be _views_ on Wasm memory. This requires careful usage as the arrays will become invalid if the memory region in Wasm changes.

#### Example

```ts
const WASM_MEMORY: WebAssembly.Memory = ...
const copiedData = parseData(WASM_MEMORY.buffer, arrayPtr, field.type);
// Make zero-copy views instead of copying array contents
const viewedData = parseData(WASM_MEMORY.buffer, arrayPtr, field.type, false);
```
 */
export function parseData<T extends DataType>(
  buffer: ArrayBuffer,
  ptr: number,
  dataType: T,
  copy: boolean = true,
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
  const dictionaryPtr = dataView.getUint32(ptr + 48, true);

  const children: arrow.Data[] = new Array(Number(nChildren));
  for (let i = 0; i < nChildren; i++) {
    children[i] = parseData(
      buffer,
      dataView.getUint32(ptrToChildrenPtrs + i * 4, true),
      dataType.children[i].type,
      copy,
    );
  }

  // Special case for handling dictionary-encoded arrays
  if (dictionaryPtr !== 0) {
    const dictionaryType = dataType as unknown as arrow.Dictionary;

    // the parent structure points to the index data, the ArrowArray.dictionary
    // points to the dictionary values array.
    const indicesType = dictionaryType.indices;
    const dictionaryIndices = parseDataContent({
      dataType: indicesType,
      dataView,
      copy,
      length,
      nullCount,
      offset,
      nChildren,
      children,
      bufferPtrs,
    });

    const valueType = dictionaryType.dictionary.type;
    const dictionaryValues = parseData(buffer, dictionaryPtr, valueType, copy);

    // @ts-expect-error we're casting to dictionary type
    return arrow.makeData({
      type: dictionaryType,
      // TODO: double check that this offset should be set on both the values
      // and indices of the dictionary array
      offset,
      length,
      nullCount,
      nullBitmap: dictionaryIndices.nullBitmap,
      // Note: Here we need to pass in the _raw TypedArray_ not a Data instance
      data: dictionaryIndices.values,
      dictionary: arrow.makeVector(dictionaryValues),
    });
  } else {
    return parseDataContent({
      dataType,
      dataView,
      copy,
      length,
      nullCount,
      offset,
      nChildren,
      children,
      bufferPtrs,
    });
  }
}

function parseDataContent<T extends DataType>({
  dataType,
  dataView,
  copy,
  length,
  nullCount,
  offset,
  nChildren,
  children,
  bufferPtrs,
}: {
  dataType: T;
  dataView: DataView;
  copy: boolean;
  length: number;
  nullCount: number;
  offset: number;
  nChildren: number;
  children: arrow.Data[];
  bufferPtrs: Uint32Array;
}): arrow.Data<T> {
  if (DataType.isNull(dataType)) {
    return arrow.makeData({
      type: dataType,
      offset,
      length,
    });
  }

  if (DataType.isInt(dataType)) {
    const [validityPtr, dataPtr] = bufferPtrs;
    const nullBitmap = parseNullBitmap(
      dataView.buffer,
      validityPtr,
      length,
      copy,
    );
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
    const nullBitmap = parseNullBitmap(
      dataView.buffer,
      validityPtr,
      length,
      copy,
    );
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
    const nullBitmap = parseNullBitmap(
      dataView.buffer,
      validityPtr,
      length,
      copy,
    );

    // Boolean arrays are bit-packed. This means the byte length should be the number of elements,
    // rounded up to the nearest byte to account for the remainder.
    const byteLength = Math.ceil(length / 8);

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
    const nullBitmap = parseNullBitmap(
      dataView.buffer,
      validityPtr,
      length,
      copy,
    );
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
    const nullBitmap = parseNullBitmap(
      dataView.buffer,
      validityPtr,
      length,
      copy,
    );

    let byteWidth = getDateByteWidth(dataType);
    const data = copy
      ? new dataType.ArrayType(
          copyBuffer(dataView.buffer, dataPtr, length * byteWidth),
        )
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
    const nullBitmap = parseNullBitmap(
      dataView.buffer,
      validityPtr,
      length,
      copy,
    );
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
    const nullBitmap = parseNullBitmap(
      dataView.buffer,
      validityPtr,
      length,
      copy,
    );

    let byteWidth = getTimeByteWidth(dataType);
    const data = copy
      ? new dataType.ArrayType(
          copyBuffer(dataView.buffer, dataPtr, length * byteWidth),
        )
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

  if (DataType.isDuration(dataType)) {
    const [validityPtr, dataPtr] = bufferPtrs;
    const nullBitmap = parseNullBitmap(
      dataView.buffer,
      validityPtr,
      length,
      copy,
    );

    let byteWidth = getTimeByteWidth(dataType);
    const data = copy
      ? new dataType.ArrayType(
          copyBuffer(dataView.buffer, dataPtr, length * byteWidth),
        )
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
    const nullBitmap = parseNullBitmap(
      dataView.buffer,
      validityPtr,
      length,
      copy,
    );

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
    const nullBitmap = parseNullBitmap(
      dataView.buffer,
      validityPtr,
      length,
      copy,
    );

    const valueOffsets = copy
      ? new Int32Array(
          copyBuffer(
            dataView.buffer,
            offsetsPtr,
            (length + 1) * Int32Array.BYTES_PER_ELEMENT,
          ),
        )
      : new Int32Array(dataView.buffer, offsetsPtr, length + 1);

    // The length described in pointer is the number of elements. The last element in `valueOffsets`
    // stores the maximum offset in the buffer and thus the byte length
    const byteLength = valueOffsets[valueOffsets.length - 1];

    const data = copy
      ? new dataType.ArrayType(copyBuffer(dataView.buffer, dataPtr, byteLength))
      : new dataType.ArrayType(dataView.buffer, dataPtr, byteLength);
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

  if (DataType.isLargeBinary(dataType)) {
    const [validityPtr, offsetsPtr, dataPtr] = bufferPtrs;
    const nullBitmap = parseNullBitmap(
      dataView.buffer,
      validityPtr,
      length,
      copy,
    );

    // The original value offsets are an Int64Array, which Arrow JS does not yet support natively
    const originalValueOffsets = new BigInt64Array(
      dataView.buffer,
      offsetsPtr,
      length + 1,
    );

    // Copy the Int64Array to an Int32Array
    const valueOffsets = new Int32Array(length + 1);
    for (let i = 0; i < originalValueOffsets.length; i++) {
      valueOffsets[i] = Number(originalValueOffsets[i]);
    }

    // The length described in pointer is the number of elements. The last element in `valueOffsets`
    // stores the maximum offset in the buffer and thus the byte length
    const byteLength = valueOffsets[valueOffsets.length - 1];

    const data = copy
      ? new Uint8Array(copyBuffer(dataView.buffer, dataPtr, byteLength))
      : new Uint8Array(dataView.buffer, dataPtr, byteLength);

    // @ts-expect-error The return type is inferred wrong because we're coercing from a LargeBinary
    // to a Binary
    return arrow.makeData({
      type: new arrow.Binary(),
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
    const nullBitmap = parseNullBitmap(
      dataView.buffer,
      validityPtr,
      length,
      copy,
    );

    const valueOffsets = copy
      ? new Int32Array(
          copyBuffer(
            dataView.buffer,
            offsetsPtr,
            (length + 1) * Int32Array.BYTES_PER_ELEMENT,
          ),
        )
      : new Int32Array(dataView.buffer, offsetsPtr, length + 1);

    // The length described in pointer is the number of elements. The last element in `valueOffsets`
    // stores the maximum offset in the buffer and thus the byte length
    const byteLength = valueOffsets[valueOffsets.length - 1];

    const data = copy
      ? new dataType.ArrayType(copyBuffer(dataView.buffer, dataPtr, byteLength))
      : new dataType.ArrayType(dataView.buffer, dataPtr, byteLength);
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

  if (DataType.isLargeUtf8(dataType)) {
    const [validityPtr, offsetsPtr, dataPtr] = bufferPtrs;
    const nullBitmap = parseNullBitmap(
      dataView.buffer,
      validityPtr,
      length,
      copy,
    );

    // The original value offsets are an Int64Array, which Arrow JS does not yet support natively
    const originalValueOffsets = new BigInt64Array(
      dataView.buffer,
      offsetsPtr,
      length + 1,
    );

    // Copy the Int64Array to an Int32Array
    const valueOffsets = new Int32Array(length + 1);
    for (let i = 0; i < originalValueOffsets.length; i++) {
      valueOffsets[i] = Number(originalValueOffsets[i]);
    }

    // The length described in pointer is the number of elements. The last element in `valueOffsets`
    // stores the maximum offset in the buffer and thus the byte length
    const byteLength = valueOffsets[valueOffsets.length - 1];

    const data = copy
      ? new Uint8Array(copyBuffer(dataView.buffer, dataPtr, byteLength))
      : new Uint8Array(dataView.buffer, dataPtr, byteLength);

    // @ts-expect-error The return type is inferred wrong because we're coercing from a LargeUtf8 to
    // a Utf8
    return arrow.makeData({
      type: new arrow.Utf8(),
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
    const nullBitmap = parseNullBitmap(
      dataView.buffer,
      validityPtr,
      length,
      copy,
    );
    const data = copy
      ? new dataType.ArrayType(
          copyBuffer(dataView.buffer, dataPtr, length * dataType.byteWidth),
        )
      : new dataType.ArrayType(
          dataView.buffer,
          dataPtr,
          length * dataType.byteWidth,
        );
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
    const nullBitmap = parseNullBitmap(
      dataView.buffer,
      validityPtr,
      length,
      copy,
    );
    const valueOffsets = copy
      ? new Int32Array(
          copyBuffer(
            dataView.buffer,
            offsetsPtr,
            (length + 1) * Int32Array.BYTES_PER_ELEMENT,
          ),
        )
      : new Int32Array(dataView.buffer, offsetsPtr, length + 1);

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

  if (isLargeList(dataType)) {
    dataType;
    assert(nChildren === 1);
    const [validityPtr, offsetsPtr] = bufferPtrs;
    const nullBitmap = parseNullBitmap(
      dataView.buffer,
      validityPtr,
      length,
      copy,
    );

    // The original value offsets are an Int64Array, which Arrow JS does not yet support natively
    const originalValueOffsets = new BigInt64Array(
      dataView.buffer,
      offsetsPtr,
      length + 1,
    );

    // Copy the Int64Array to an Int32Array
    const valueOffsets = new Int32Array(length + 1);
    for (let i = 0; i < originalValueOffsets.length; i++) {
      valueOffsets[i] = Number(originalValueOffsets[i]);
    }

    // @ts-expect-error The return type is inferred wrong because we're coercing from a LargeList to
    // a List
    return arrow.makeData({
      type: new arrow.List((dataType as LargeList).children[0]),
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
    const nullBitmap = parseNullBitmap(
      dataView.buffer,
      validityPtr,
      length,
      copy,
    );

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
    const nullBitmap = parseNullBitmap(
      dataView.buffer,
      validityPtr,
      length,
      copy,
    );

    return arrow.makeData({
      type: dataType,
      offset,
      length,
      nullCount,
      nullBitmap,
      children,
    });
  }

  if (DataType.isMap(dataType)) {
    assert(nChildren === 1);
    const [validityPtr, offsetsPtr] = bufferPtrs;
    const nullBitmap = parseNullBitmap(
      dataView.buffer,
      validityPtr,
      length,
      copy,
    );
    const valueOffsets = copy
      ? new Int32Array(
          copyBuffer(
            dataView.buffer,
            offsetsPtr,
            (length + 1) * Int32Array.BYTES_PER_ELEMENT,
          ),
        )
      : new Int32Array(dataView.buffer, offsetsPtr, length + 1);

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

  if (DataType.isDenseUnion(dataType)) {
    const [typeIdsPtr, offsetsPtr] = bufferPtrs;

    const valueOffsets = copy
      ? new Int32Array(
          copyBuffer(
            dataView.buffer,
            offsetsPtr,
            (length + 1) * Int32Array.BYTES_PER_ELEMENT,
          ),
        )
      : new Int32Array(dataView.buffer, offsetsPtr, length + 1);

    const typeIds = copy
      ? new Int8Array(
          copyBuffer(
            dataView.buffer,
            typeIdsPtr,
            (length + 1) * Int8Array.BYTES_PER_ELEMENT,
          ),
        )
      : new Int8Array(dataView.buffer, typeIdsPtr, length + 1);

    return arrow.makeData({
      type: dataType,
      offset,
      length,
      nullCount,
      typeIds,
      children,
      valueOffsets,
    });
  }

  if (DataType.isSparseUnion(dataType)) {
    const [typeIdsPtr] = bufferPtrs;

    const typeIds = copy
      ? new Int8Array(
          copyBuffer(
            dataView.buffer,
            typeIdsPtr,
            (length + 1) * Int8Array.BYTES_PER_ELEMENT,
          ),
        )
      : new Int8Array(dataView.buffer, typeIdsPtr, length + 1);

    return arrow.makeData({
      type: dataType,
      offset,
      length,
      nullCount,
      typeIds,
      children,
    });
  }

  throw new Error(`Unsupported type ${dataType}`);
}

function getDateByteWidth(type: arrow.Date_): number {
  switch (type.unit) {
    case arrow.DateUnit.DAY:
      return 4;
    case arrow.DateUnit.MILLISECOND:
      return 8;
  }
  assertUnreachable();
}

function getTimeByteWidth(
  type: arrow.Time | arrow.Timestamp | arrow.Duration,
): number {
  switch (type.unit) {
    case arrow.TimeUnit.SECOND:
    case arrow.TimeUnit.MILLISECOND:
      return 4;
    case arrow.TimeUnit.MICROSECOND:
    case arrow.TimeUnit.NANOSECOND:
      return 8;
  }
  assertUnreachable();
}

function parseNullBitmap(
  buffer: ArrayBuffer,
  validityPtr: number,
  length: number,
  copy: boolean,
): NullBitmap {
  if (validityPtr === 0) {
    return null;
  }

  // Each value takes up one bit
  const byteLength = (length >> 3) + 1;

  if (copy) {
    return new Uint8Array(copyBuffer(buffer, validityPtr, byteLength));
  } else {
    return new Uint8Array(buffer, validityPtr, byteLength);
  }
}

/** Copy existing buffer into new buffer */
function copyBuffer(
  buffer: ArrayBuffer,
  ptr: number,
  byteLength: number,
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
  throw new Error();
}
