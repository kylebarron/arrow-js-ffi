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

/** Put an Arrow Table in Wasm memory and expose it via an FFI RecordBatch */
export function arrowTableToFFIRecordBatch(
  table: arrow.Table
): wasm.FFIArrowRecordBatch {
  return wasm.arrowIPCToFFIRecordBatch(arrow.tableToIPC(table, "file"));
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

export function validityEqual(v1: arrow.Vector, v2: arrow.Vector): boolean {
  if (v1.length !== v2.length) {
    return false;
  }

  if (v1.data.length !== v2.data.length) {
    console.log("todo: support different data lengths");
    return false;
  }
  for (let i = 0; i < v1.data.length; i++) {
    const d1 = v1.data[i];
    const d2 = v2.data[i];
    // Check that null bitmaps have same length
    if (d1 !== null && d2 !== null) {
      if (d1.nullBitmap.length !== d2.nullBitmap.length) {
        return false;
      }
    }
  }

  for (let i = 0; i < v1.length; i++) {
    if (v1.isValid(i) !== v2.isValid(i)) {
      return false;
    }
  }

  return true;
}
