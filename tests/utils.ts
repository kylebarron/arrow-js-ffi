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
