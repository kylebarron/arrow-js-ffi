import * as arrow from "apache-arrow";
import { parseField } from "./field";
import { parseData } from "./vector";

/**
 * Parse an [`ArrowArray`](https://arrow.apache.org/docs/format/CDataInterface.html#the-arrowarray-structure) C FFI struct _plus_ an [`ArrowSchema`](https://arrow.apache.org/docs/format/CDataInterface.html#the-arrowschema-structure) C FFI struct into an `arrow.RecordBatch` instance. Note that the underlying array and field **must** be a `Struct` type. In essence a `Struct` array is used to mimic a `RecordBatch` while only being one array.
 *
 * - `buffer` (`ArrayBuffer`): The [`WebAssembly.Memory`](https://developer.mozilla.org/en-US/docs/WebAssembly/JavaScript_interface/Memory) instance to read from.
 * - `arrayPtr` (`number`): The numeric pointer in `buffer` where the _array_ C struct is located.
 * - `schemaPtr` (`number`): The numeric pointer in `buffer` where the _field_ C struct is located.
 * - `copy` (`boolean`): If `true`, will _copy_ data across the Wasm boundary, allowing you to delete the copy on the Wasm side. If `false`, the resulting `arrow.Vector` objects will be _views_ on Wasm memory. This requires careful usage as the arrays will become invalid if the memory region in Wasm changes.
 *
 * ```ts
 * const WASM_MEMORY: WebAssembly.Memory = ...
 * // Pass `true` to copy arrays across the boundary instead of creating views.
 * const recordBatch = parseRecordBatch(WASM_MEMORY.buffer, arrayPtr, fieldPtr, true);
 * ```
 */
export function parseRecordBatch(
  buffer: ArrayBuffer,
  arrayPtr: number,
  schemaPtr: number,
  copy: boolean = false,
): arrow.RecordBatch {
  const field = parseField(buffer, schemaPtr);
  if (!isStructField(field)) {
    throw new Error("Expected struct");
  }

  const data = parseData(buffer, arrayPtr, field.type, copy);
  const outSchema = unpackStructField(field);
  return new arrow.RecordBatch(outSchema, data);
}

function isStructField(field: arrow.Field): field is arrow.Field<arrow.Struct> {
  return field.typeId == arrow.Type.Struct;
}

function unpackStructField(field: arrow.Field<arrow.Struct>): arrow.Schema {
  const fields = field.type.children;
  const metadata = field.metadata;
  // TODO: support dictionaries parameter for dictionary-encoded arrays
  return new arrow.Schema(fields, metadata);
}
