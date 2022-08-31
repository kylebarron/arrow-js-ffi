import { readFileSync } from "fs";
import * as arrow from "apache-arrow";
import * as parquet from "parquet-wasm/node/arrow2";

export function loadIPCTableFromDisk(path: string): arrow.Table {
  const buffer = readFileSync(path);
  return arrow.tableFromIPC(buffer);
}

/** Put an Arrow Table in Wasm memory and expose it via FFI */
export function arrowTableToFFI(table: arrow.Table): parquet.FFIArrowTable {
  const parquetBuffer = parquet.writeParquet(arrow.tableToIPC(table, "file"));
  return parquet._readParquetFFI(parquetBuffer);
}
