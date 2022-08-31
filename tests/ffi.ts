import * as test from "tape";
import * as parquet from "parquet-wasm/node/arrow2";
import * as arrow from "apache-arrow";
import { loadIPCTableFromDisk, arrowTableToFFI } from "./utils";
import { parseField } from "../src";

// @ts-expect-error
const WASM_MEMORY = parquet.__wasm.memory;

test("read file", (t) => {
  const table = loadIPCTableFromDisk("tests/data.arrow");
  const ffiTable = arrowTableToFFI(table);

  const numFields = ffiTable.schemaLength();
  const fieldPtr = ffiTable.schemaAddr(0);
  const field = parseField(WASM_MEMORY.buffer, fieldPtr);

  t.equals(field.name, "str");
  t.equals(field.typeId, new arrow.Utf8().typeId);
  t.equals(field.nullable, false);

  t.end();
});
