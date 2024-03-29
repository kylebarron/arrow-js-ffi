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
const table = parseTable(
    WASM_MEMORY.buffer,
    arrayPtrs,
    schemaPtr,
    true
);
```

## Memory Management

TL;dr: As of version 0.4, `arrow-js-ffi` **does not release any WebAssembly resources**. You must free Arrow resources when you're done with them to avoid memory leaks. This library does not currently provide helpers to deallocate that memory; instead look for `free` methods exposed by Emscripten or wasm-bindgen.

Memory management between WebAssembly's own memory and JavaScript memory can be tricky. The Arrow C Data Interface includes [prescriptions for memory management](https://arrow.apache.org/docs/format/CDataInterface.html#memory-management) but those recommendations are designed for situations where two programs share the same memory space. Applying it to WebAssembly-JavaScript interop is imperfect because WebAssembly memory is sandboxed in a separate memory space.

The C Data Interface [instructs consumers to call the release callback](https://arrow.apache.org/docs/format/CDataInterface.html#release-callback-semantics-for-consumers), which deallocates the referenced memory. However in our case, we can't call the release callback in all situations because the lifetime of views on the referenced Arrow data would outlive the lifetime of the data. Even when a user passes `copy=true`, where the data is copied into JS memory, it's still uncertain whether to release the underlying resources because the user might _want_ to still do something with their Wasm table data.

A future release of `arrow-js-ffi` may include standalone functions to release Arrow data, which users can call manually once they know they're done with the data. But even in this case, freeing the _underlying array_ will not free any wrapper structs allocated by Emscripten or wasm-bindgen. If the free method on those structs is called later, it would lead to a double-free.

If you have thoughts on memory management, open an issue!

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
- [x] Large Binary (Supported natively by Arrow JS as of v15)
- [x] String
- [x] Large String (Supported natively by Arrow JS as of v15)
- [x] Fixed-width Binary

### Decimal

- [ ] Decimal128 (failing a test, this may be [#37920])
- [ ] Decimal256 (failing a test, this may be [#37920])

[#37920]: https://github.com/apache/arrow/issues/37920

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
- [x] Map (though not yet tested, see [#97](https://github.com/kylebarron/arrow-js-ffi/issues/97))
- [x] Dense Union
- [x] Sparse Union
- [x] Dictionary-encoded arrays

### Extension Types

- [x] Field metadata is preserved.

## TODO:

- Call the release callback on the C structs. This requires figuring out how to call C function pointers from JS.
