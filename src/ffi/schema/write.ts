/**
 * NOTE: all of the functions in this file take a `WebAssembly.Memory` object as
 * input, not an `ArrayBuffer` object. This is because when a `Memory` instance
 * grows, its `buffer` is detached!!
 *
 * So any time you call `malloc`, you cannot use any old
 * `WebAssembly.Memory.buffer` instances!
 *
 * https://developer.mozilla.org/en-US/docs/WebAssembly/JavaScript_interface/Memory/grow#detachment_upon_growing
 */

import { ArrowSchema } from "../../nanoarrow";
import { Malloc } from "../../types";
import { writeUint32Array } from "../../util";

const UTF8_ENCODER = new TextEncoder();
const ARROW_SCHEMA_STRUCT_BYTE_SIZE = 48;

/**
 * Write an `ArrowSchema` object into WebAssembly memory according to Arrow C
 * Data Interface
 *
 * - `schema`: The `ArrowSchema` object to write.
 * - `buffer`: The WebAssembly memory space in which to write the array.
 * - `malloc`: A function that reserves WebAssembly memory. The function should
 *   take one argument, the length of the buffer to reserve, and return one
 *   number, the numeric pointer within the memory space marking the beginning
 *   of this array.
 *
 * @return The numeric pointer within `buffer` where the `ArrowSchema` C Data Interface struct is located.
 */
export function writeSchemaFFI(
  schema: ArrowSchema,
  memory: WebAssembly.Memory,
  malloc: Malloc
): number {
  const formatPtr = writeNullTerminatedString(schema.format, memory, malloc);
  const namePtr = writeNullTerminatedString(schema.name, memory, malloc);
  // TODO: write metadata
  const metadataPtr = 0;

  const childrenPtrs = new Uint32Array(schema.children.length);
  for (let i = 0; i < schema.children.length; i++) {
    const child = schema.children[i];
    const childPtr = writeSchemaFFI(child, memory, malloc);
    childrenPtrs[i] = childPtr;
  }
  const ptrToChildrenPtrs = writeUint32Array(childrenPtrs, memory, malloc);

  let dictionaryPtr = 0;
  if (schema.dictionary !== null) {
    dictionaryPtr = writeSchemaFFI(schema.dictionary, memory, malloc);
  }

  const schemaStructPtr = malloc(ARROW_SCHEMA_STRUCT_BYTE_SIZE);
  const dstDataView = new DataView(
    memory.buffer,
    schemaStructPtr,
    ARROW_SCHEMA_STRUCT_BYTE_SIZE
  );
  dstDataView.setUint32(0, formatPtr, true);
  dstDataView.setUint32(4, namePtr, true);
  dstDataView.setUint32(8, metadataPtr, true);
  dstDataView.setBigInt64(16, schema.flags, true);
  dstDataView.setBigInt64(24, BigInt(schema.children.length), true);
  dstDataView.setUint32(32, ptrToChildrenPtrs, true);
  dstDataView.setUint32(36, dictionaryPtr, true);

  // TODO: this sets the release callback to the null pointer.
  // I assume this will probably break in the wasm side, and we'll need to find
  // a workaround
  dstDataView.setUint32(40, 0, true);
  dstDataView.setUint32(44, 0, true);

  return schemaStructPtr;
}

/**
 * Write a null-terminated string into WebAssembly memory
 */
function writeNullTerminatedString(
  s: string,
  memory: WebAssembly.Memory,
  malloc: Malloc
): number {
  // TODO(perf): Right now we're doing an _exact_ malloc by first encoding the
  // string into a standalone Uint8Array, and then malloc-ing room for that
  // exact length. This isn't ideal for performance, because we're making a
  // superfluous copy of the string.
  //
  // Instead, the TextEncoder has an `encodeInto` API, which is ideal for
  // writing strings directly into Wasm memory. The drawback is that you don't
  // necessarily know the length of a string, and you have to follow some
  // heuristics.
  //
  // In the Arrow case, the only large string that we may have is the field
  // metadata, so this extra copy should have relatively low perf impacts
  //
  // This whole page is great:
  // https://developer.mozilla.org/en-US/docs/Web/API/TextEncoder/encodeInto

  const utf8Buffer = UTF8_ENCODER.encode(s);

  // + 1 because null-terminated
  const byteLength = utf8Buffer.length + 1;
  const ptr = malloc(byteLength);
  const destinationView = new Uint8Array(memory.buffer, ptr, byteLength);

  // Copy encoded string
  destinationView.set(utf8Buffer);

  // Set null termination
  destinationView[byteLength - 1] = 0;

  return ptr;
}
