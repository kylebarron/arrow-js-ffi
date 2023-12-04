import * as arrow from "apache-arrow";
import { parseField } from "./field";

/**
Parse an [`ArrowSchema`](https://arrow.apache.org/docs/format/CDataInterface.html#the-arrowschema-structure) C FFI struct into an `arrow.Schema` instance. Note that the underlying field **must** be a `Struct` type. In essence a `Struct` field is used to mimic a `Schema` while only being one field.

- `buffer` (`ArrayBuffer`): The [`WebAssembly.Memory`](https://developer.mozilla.org/en-US/docs/WebAssembly/JavaScript_interface/Memory) instance to read from.
- `ptr` (`number`): The numeric pointer in `buffer` where the C struct is located.

```js
const WASM_MEMORY: WebAssembly.Memory = ...
const schema = parseSchema(WASM_MEMORY.buffer, fieldPtr);
```
 */
export function parseSchema(buffer: ArrayBuffer, ptr: number): arrow.Schema {
  const field = parseField(buffer, ptr);
  if (!isStructField(field)) {
    throw new Error("Expected struct");
  }

  return unpackStructField(field);
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
