import { Malloc } from "./types";

// TODO: what to do if arr has length 0?
export function writeUint32Array(
  arr: Uint32Array,
  memory: WebAssembly.Memory,
  malloc: Malloc
): number {
  const byteLength = arr.length * Uint32Array.BYTES_PER_ELEMENT;
  const ptr = malloc(byteLength);
  const destinationView = new Uint32Array(memory.buffer, ptr, arr.length);
  destinationView.set(arr);
  return ptr;
}
