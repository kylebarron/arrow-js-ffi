import * as arrow from "apache-arrow";
import { TypeMap } from "apache-arrow/type";
import { parseSchema } from "./schema";
import { parseData } from "./vector";

/**
Parse an Arrow Table object from WebAssembly memory to an Arrow JS `Table`.

This expects an array of [`ArrowArray`](https://arrow.apache.org/docs/format/CDataInterface.html#the-arrowarray-structure) C FFI structs _plus_ an [`ArrowSchema`](https://arrow.apache.org/docs/format/CDataInterface.html#the-arrowschema-structure) C FFI struct. Note that the underlying array and field pointers **must** be a `Struct` type. In essence a `Struct` array is used to mimic each `RecordBatch` while only being one array.

- `buffer` (`ArrayBuffer`): The [`WebAssembly.Memory`](https://developer.mozilla.org/en-US/docs/WebAssembly/JavaScript_interface/Memory) instance to read from.
- `arrayPtrs` (`number[]`): An array of numeric pointers describing the location in `buffer` where the _array_ C struct is located that represents each record batch.
- `schemaPtr` (`number`): The numeric pointer in `buffer` where the _field_ C struct is located.
- `copy` (`boolean`, default: `true`): If `true`, will _copy_ data across the Wasm boundary, allowing you to delete the copy on the Wasm side. If `false`, the resulting `arrow.Vector` objects will be _views_ on Wasm memory. This requires careful usage as the arrays will become invalid if the memory region in Wasm changes.
 */
export function parseTable<T extends TypeMap>(
  buffer: ArrayBuffer,
  arrayPtrs: number[] | Uint32Array,
  schemaPtr: number,
  copy: boolean = true,
): arrow.Table<T> {
  const schema = parseSchema(buffer, schemaPtr);

  const batches: arrow.RecordBatch<T>[] = [];
  for (let i = 0; i < arrayPtrs.length; i++) {
    const structData = parseData(
      buffer,
      arrayPtrs[i],
      new arrow.Struct(schema.fields),
      copy,
    );
    const recordBatch = new arrow.RecordBatch(schema, structData);
    batches.push(recordBatch);
  }

  return new arrow.Table(schema, batches);
}
