import * as test from "tape";
import * as parquet from "parquet-wasm/node/arrow2";
import { loadIPCTableFromDisk, arrowTableToFFI } from "./utils";

// @ts-expect-error
const WASM_MEMORY = parquet.__wasm.memory;

test("read file", (t) => {
  const table = loadIPCTableFromDisk("tests/data.arrow");
  t.ok(table);

  t.end();
});
