import * as arrow from "apache-arrow";
import { parseSchema } from "./schema";
import { parseData } from "./vector";

/**
Parse an [`ArrowArray`](https://arrow.apache.org/docs/format/CDataInterface.html#the-arrowarray-structure) C FFI struct _plus_ an [`ArrowSchema`](https://arrow.apache.org/docs/format/CDataInterface.html#the-arrowschema-structure) C FFI struct into an `arrow.RecordBatch` instance. Note that the underlying array and field **must** be a `Struct` type. In essence a `Struct` array is used to mimic a `RecordBatch` while only being one array.

- `buffer` (`ArrayBuffer`): The [`WebAssembly.Memory`](https://developer.mozilla.org/en-US/docs/WebAssembly/JavaScript_interface/Memory) instance to read from.
- `arrayPtr` (`number`): The numeric pointer in `buffer` where the _array_ C struct is located.
- `schemaPtr` (`number`): The numeric pointer in `buffer` where the _field_ C struct is located.
- `copy` (`boolean`, default: `true`): If `true`, will _copy_ data across the Wasm boundary, allowing you to delete the copy on the Wasm side. If `false`, the resulting `arrow.Vector` objects will be _views_ on Wasm memory. This requires careful usage as the arrays will become invalid if the memory region in Wasm changes.

#### Example

```ts
const WASM_MEMORY: WebAssembly.Memory = ...
const copiedRecordBatch = parseRecordBatch(
    WASM_MEMORY.buffer,
    arrayPtr,
    fieldPtr
);
// Pass `false` to view arrays across the boundary instead of creating copies.
const viewedRecordBatch = parseRecordBatch(
    WASM_MEMORY.buffer,
    arrayPtr,
    fieldPtr,
    false
);
```
 */
export function parseRecordBatch(
  buffer: ArrayBuffer,
  arrayPtr: number,
  schemaPtr: number,
  copy: boolean = true,
): arrow.RecordBatch {
  const schema = parseSchema(buffer, schemaPtr);
  const data = parseData(
    buffer,
    arrayPtr,
    new arrow.Struct(schema.fields),
    copy,
  );
  return new arrow.RecordBatch(schema, data);
}
