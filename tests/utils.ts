import { readFileSync } from "fs";
import * as arrow from "apache-arrow";
import * as wasm from "rust-arrow-ffi";

export function loadIPCTableFromDisk(path: string): arrow.Table {
  const buffer = readFileSync(path);
  return arrow.tableFromIPC(buffer);
}

/** Put an Arrow Table in Wasm memory and expose it via FFI */
export function arrowTableToFFI(table: arrow.Table): wasm.FFIArrowTable {
  return wasm.arrowIPCToFFI(arrow.tableToIPC(table, "file"));
}

export function arraysEqual<T>(
  arr1: ArrayLike<T>,
  arr2: ArrayLike<T>
): boolean {
  if (arr1.length !== arr2.length) {
    return false;
  }

  for (let i = 0; i < arr1.length; i++) {
    if (arr1[i] !== arr2[i]) {
      return false;
    }
  }

  return true;
}
