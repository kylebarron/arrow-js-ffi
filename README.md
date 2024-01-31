# arrow-js-ffi

Interpret [Arrow](https://arrow.apache.org/) memory across the WebAssembly boundary without serialization.

## Why?

Arrow is a high-performance memory layout for analytical programs. Since Arrow's memory layout is defined to be the same in every implementation, programs that use Arrow in WebAssembly are using the same exact layout that [Arrow JS](https://arrow.apache.org/docs/js/) implements! This means we can use plain [`ArrayBuffer`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/ArrayBuffer)s to move highly structured data back and forth to WebAssembly memory, entirely avoiding serialization.

I wrote an [interactive blog post](https://observablehq.com/@kylebarron/zero-copy-apache-arrow-with-webassembly) that goes into more detail on why this is useful and how this library implements Arrow's [C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html) in JavaScript.

## Usage

This package exports two functions, `parseField` for parsing the `ArrowSchema` struct into an `arrow.Field` and `parseVector` for parsing the `ArrowArray` struct into an `arrow.Vector`.

### `parseField`

Parse an [`ArrowSchema`](https://arrow.apache.org/docs/format/CDataInterface.html#the-arrowschema-structure) C FFI struct into an `arrow.Field` instance. The `Field` is necessary for later using `parseVector` below.

- `buffer` (`ArrayBuffer`): The [`WebAssembly.Memory`](https://developer.mozilla.org/en-US/docs/WebAssembly/JavaScript_interface/Memory) instance to read from.
- `ptr` (`number`): The numeric pointer in `buffer` where the C struct is located.

```js
const WASM_MEMORY: WebAssembly.Memory = ...
const field = parseField(WASM_MEMORY.buffer, fieldPtr);
```

### `parseSchema`

Parse an [`ArrowSchema`](https://arrow.apache.org/docs/format/CDataInterface.html#the-arrowschema-structure) C FFI struct into an `arrow.Schema` instance. Note that the underlying field **must** be a `Struct` type. In essence a `Struct` field is used to mimic a `Schema` while only being one field.

- `buffer` (`ArrayBuffer`): The [`WebAssembly.Memory`](https://developer.mozilla.org/en-US/docs/WebAssembly/JavaScript_interface/Memory) instance to read from.
- `ptr` (`number`): The numeric pointer in `buffer` where the C struct is located.

```js
const WASM_MEMORY: WebAssembly.Memory = ...
const schema = parseSchema(WASM_MEMORY.buffer, fieldPtr);
```

### `parseData`

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

### `parseVector`

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
```

### `parseRecordBatch`

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

### `parseTable`

Parse an Arrow Table object from WebAssembly memory to an Arrow JS `Table`.

This expects an array of [`ArrowArray`](https://arrow.apache.org/docs/format/CDataInterface.html#the-arrowarray-structure) C FFI structs _plus_ an [`ArrowSchema`](https://arrow.apache.org/docs/format/CDataInterface.html#the-arrowschema-structure) C FFI struct. Note that the underlying array and field pointers **must** be a `Struct` type. In essence a `Struct` array is used to mimic each `RecordBatch` while only being one array.

- `buffer` (`ArrayBuffer`): The [`WebAssembly.Memory`](https://developer.mozilla.org/en-US/docs/WebAssembly/JavaScript_interface/Memory) instance to read from.
- `arrayPtrs` (`number[]`): An array of numeric pointers describing the location in `buffer` where the _array_ C struct is located that represents each record batch.
- `schemaPtr` (`number`): The numeric pointer in `buffer` where the _field_ C struct is located.
- `copy` (`boolean`, default: `true`): If `true`, will _copy_ data across the Wasm boundary, allowing you to delete the copy on the Wasm side. If `false`, the resulting `arrow.Vector` objects will be _views_ on Wasm memory. This requires careful usage as the arrays will become invalid if the memory region in Wasm changes.

#### Example

```ts
const WASM_MEMORY: WebAssembly.Memory = ...
const table = parseRecordBatch(
    WASM_MEMORY.buffer,
    arrayPtrs,
    schemaPtr,
    true
);
```

## Type Support

Most of the unsupported types should be pretty straightforward to implement; they just need some testing.

### Primitive Types

- [x] Null
- [x] Boolean
- [x] Int8
- [x] Uint8
- [x] Int16
- [x] Uint16
- [x] Int32
- [x] Uint32
- [x] Int64
- [x] Uint64
- [x] Float16
- [x] Float32
- [x] Float64

### Binary & String

- [x] Binary
- [x] Large Binary (Not implemented by Arrow JS but supported by downcasting to `Binary`.)
- [x] String
- [x] Large String (Not implemented by Arrow JS but supported by downcasting to `String`.)
- [x] Fixed-width Binary

### Decimal

- [ ] Decimal128 (failing a test)
- [ ] Decimal256 (failing a test)

### Temporal Types

- [x] Date32
- [x] Date64
- [x] Time32
- [x] Time64
- [x] Timestamp (with timezone)
- [x] Duration
- [ ] Interval

### Nested Types

- [x] List
- [x] Large List (Not implemented by Arrow JS but supported by downcasting to `List`.)
- [x] Fixed-size List
- [x] Struct
- [ ] Map
- [x] Dense Union
- [x] Sparse Union
- [ ] Dictionary-encoded arrays

### Extension Types

- [x] Field metadata is preserved.

## TODO:

- Call the release callback on the C structs. This requires figuring out how to call C function pointers from JS.
