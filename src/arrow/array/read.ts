import * as arrow from "apache-arrow";
import { ArrowArray } from "../../nanoarrow";
import { assert } from "../../vector";

export function readArrayArrowJS(data: arrow.Data): ArrowArray {
  const nanoarrowChildren: ArrowArray[] = [];
  for (const arrowJsChild of data.children) {
    const nanoarrowChild = readArrayArrowJS(arrowJsChild);
    nanoarrowChildren.push(nanoarrowChild);
  }

  let nanoarrowDictionary: ArrowArray | null = null;
  if (data.dictionary) {
    const dictionaryVector = data.dictionary;
    assert(dictionaryVector.data.length === 1);
    nanoarrowDictionary = readArrayArrowJS(dictionaryVector.data[0]);
  }

  const buffers = constructNanoarrowBuffers(data);

  return {
    length: data.length,
    nullCount: data.nullCount,
    offset: data.offset,
    buffers,
    children: nanoarrowChildren,
    dictionary: nanoarrowDictionary,
  };
}

function constructNanoarrowBuffers(data: arrow.Data): (Uint8Array | null)[] {
  switch (data.type.typeId) {
    // Null type has no buffers
    case arrow.Type.Null:
      return [];

    // Primitive type has two buffers: validity and data
    case arrow.Type.Int:
    case arrow.Type.Float:
    case arrow.Type.Bool:
    case arrow.Type.Decimal:
    case arrow.Type.Date:
    case arrow.Type.Time:
    case arrow.Type.Timestamp:
    case arrow.Type.Interval:
    case arrow.Type.Int8:
    case arrow.Type.Int16:
    case arrow.Type.Int32:
    case arrow.Type.Int64:
    case arrow.Type.Uint8:
    case arrow.Type.Uint16:
    case arrow.Type.Uint32:
    case arrow.Type.Uint64:
    case arrow.Type.Float16:
    case arrow.Type.Float32:
    case arrow.Type.Float64:
    case arrow.Type.DateDay:
    case arrow.Type.DateMillisecond:
    case arrow.Type.TimestampSecond:
    case arrow.Type.TimestampMillisecond:
    case arrow.Type.TimestampMicrosecond:
    case arrow.Type.TimestampNanosecond:
    case arrow.Type.TimeSecond:
    case arrow.Type.TimeMillisecond:
    case arrow.Type.TimeMicrosecond:
    case arrow.Type.TimeNanosecond:
    case arrow.Type.IntervalDayTime:
    case arrow.Type.IntervalYearMonth: {
      const validity = data.nullBitmap;
      const values = data.values;
      const uint8Values = new Uint8Array(
        values.buffer,
        values.buffer.byteOffset,
        values.buffer.byteLength
      );
      return [validity, uint8Values];
    }

    // Variable binary has three buffers: validity, offsets, data
    case arrow.Type.Binary: {
      const validity = data.nullBitmap;
      const offsets = data.valueOffsets;
      const uint8Offsets = new Uint8Array(
        offsets.buffer,
        // @ts-expect-error Property 'byteOffset' does not exist on type 'ArrayBufferLike'.
        offsets.buffer.byteOffset || 0,
        offsets.buffer.byteLength
      );
      const values = data.values;
      const uint8Values = new Uint8Array(
        values.buffer,
        values.buffer.byteOffset,
        values.buffer.byteLength
      );
      return [validity, uint8Offsets, uint8Values];
    }

    // List has two buffers: validity and offsets
    case arrow.Type.List:
    case arrow.Type.Utf8: {
      const validity = data.nullBitmap;
      const offsets = data.valueOffsets;
      const uint8Offsets = new Uint8Array(
        offsets.buffer,
        // @ts-expect-error Property 'byteOffset' does not exist on type 'ArrayBufferLike'.
        offsets.buffer.byteOffset || 0,
        offsets.buffer.byteLength
      );
      return [validity, uint8Offsets];
    }

    // Fixed-size List, Fixed-size Binary, Struct have one buffer: validity
    case arrow.Type.FixedSizeList:
    case arrow.Type.FixedSizeBinary:
    case arrow.Type.Struct:
      return [data.nullBitmap];

    // Sparse Union has one buffer: type ids
    case arrow.Type.SparseUnion: {
      const typeIds = data.typeIds;
      const uint8TypeIds = new Uint8Array(
        typeIds.buffer,
        typeIds.buffer.byteOffset || 0,
        typeIds.buffer.byteLength
      );
      return [uint8TypeIds];
    }

    // Dense Union has two buffers: type ids and offsets
    case arrow.Type.DenseUnion: {
      const typeIds = data.typeIds;
      const uint8TypeIds = new Uint8Array(
        typeIds.buffer,
        typeIds.buffer.byteOffset || 0,
        typeIds.buffer.byteLength
      );
      const offsets = data.valueOffsets;
      const uint8Offsets = new Uint8Array(
        offsets.buffer,
        // @ts-expect-error Property 'byteOffset' does not exist on type 'ArrayBufferLike'.
        offsets.buffer.byteOffset || 0,
        offsets.buffer.byteLength
      );
      return [uint8TypeIds, uint8Offsets];
    }

    case arrow.Type.Union: {
      // TODO:
      assert(false);
    }

    default:
      break;
  }

  assert(false);
  // @ts-expect-error
  return;
}
