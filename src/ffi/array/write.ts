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

import { ArrowArray } from "../../nanoarrow";
import { Malloc } from "../../types";
import { writeUint32Array } from "../../util";

const ARROW_ARRAY_STRUCT_BYTE_SIZE = 60;

/**
 * Write an `ArrowArray` object into WebAssembly memory according to Arrow C
 * Data Interface
 *
 * - `array`: The array to write.
 * - `buffer`: The WebAssembly memory space in which to write the array.
 * - `malloc`: A function that reserves WebAssembly memory. The function should
 *   take one argument, the length of the buffer to reserve, and return one
 *   number, the numeric pointer within the memory space marking the beginning
 *   of this array.
 *
 * @return The numeric pointer within `buffer` where the `ArrowArray` C Data Interface struct is located.
 */
export function writeArrayFFI(
  array: ArrowArray,
  memory: WebAssembly.Memory,
  malloc: Malloc
): number {
  const bufferPtrs = new Uint32Array(array.buffers.length);
  for (let i = 0; i < bufferPtrs.length; i++) {
    const buffer = array.buffers[i];
    const bufferPtr = writeBuffer(buffer, memory, malloc);
    bufferPtrs[i] = bufferPtr;
  }
  const ptrToBufferPtrs = writeUint32Array(bufferPtrs, memory, malloc);

  const childrenPtrs = new Uint32Array(array.children.length);
  for (let i = 0; i < array.children.length; i++) {
    const child = array.children[i];
    const childPtr = writeArrayFFI(child, memory, malloc);
    childrenPtrs[i] = childPtr;
  }
  const ptrToChildrenPtrs = writeUint32Array(childrenPtrs, memory, malloc);

  let dictionaryPtr = 0;
  if (array.dictionary !== null) {
    dictionaryPtr = writeArrayFFI(array.dictionary, memory, malloc);
  }

  // Write the actual struct
  const arrayStructPtr = malloc(ARROW_ARRAY_STRUCT_BYTE_SIZE);
  const destinationInt64View = new BigInt64Array(
    memory.buffer,
    arrayStructPtr,
    ARROW_ARRAY_STRUCT_BYTE_SIZE
  );
  destinationInt64View[0] = BigInt(array.length);
  destinationInt64View[1] = BigInt(array.nullCount);
  destinationInt64View[2] = BigInt(array.offset);
  destinationInt64View[3] = BigInt(array.buffers.length);
  destinationInt64View[4] = BigInt(array.children.length);

  const destinationInt32View = new Int32Array(
    memory.buffer,
    arrayStructPtr + 40,
    ARROW_ARRAY_STRUCT_BYTE_SIZE - 40
  );
  destinationInt32View[0] = ptrToBufferPtrs;
  destinationInt32View[1] = ptrToChildrenPtrs;
  destinationInt32View[2] = dictionaryPtr;
  // TODO: this sets the release callback to the null pointer.
  // I assume this will probably break in the wasm side, and we'll need to find
  // a workaround
  destinationInt32View[3] = 0;
  destinationInt32View[4] = 0;

  return arrayStructPtr;
}

function writeBuffer(
  buffer: Uint8Array | null,
  memory: WebAssembly.Memory,
  malloc: Malloc
): number {
  if (buffer === null || buffer.byteLength === 0) {
    return 0;
  }

  const ptr = malloc(buffer.length);
  const destinationView = new Uint8Array(memory.buffer, ptr, buffer.length);
  destinationView.set(buffer);
  return ptr;
}
