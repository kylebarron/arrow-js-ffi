import { readFileSync } from "fs";
import * as arrow from "apache-arrow";
import * as arrowWasm from "arrow-wasm-arrow2";

export function loadIPCTableFromDisk(path: string): arrow.Table {
  const buffer = readFileSync(path);
  return arrow.tableFromIPC(buffer);
}

/** Put an Arrow Table in Wasm memory and expose it via FFI */
export function arrowTableToFFI(table: arrow.Table): arrowWasm.FFITable {
  return arrowWasm.Table.fromIPCStream(
    arrow.tableToIPC(table, "stream")
  ).intoFFI();
}

/** Put an Arrow Table in Wasm memory and expose it via an FFI RecordBatch */
export function arrowTableToFFIRecordBatch(
  table: arrow.Table
): arrowWasm.FFIRecordBatch {
  const wasmTable = arrowWasm.Table.fromIPCStream(
    arrow.tableToIPC(table, "stream")
  );
  if (wasmTable.numBatches !== 1) {
    throw new Error(`expected only one batch, got ${wasmTable.numBatches}`);
  }
  const wasmRecordBatch = wasmTable.recordBatch(0);
  if (!wasmRecordBatch) {
    throw new Error("wasm record batch is undefined");
  }
  return wasmRecordBatch.intoFFI();
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
