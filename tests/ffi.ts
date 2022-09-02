import * as test from "tape";
import * as arrow from "apache-arrow";
import * as wasm from "rust-arrow-ffi";
import { loadIPCTableFromDisk, arrowTableToFFI } from "./utils";
import { parseField, parseVector } from "../src";

wasm.setPanicHook();

// @ts-expect-error
const WASM_MEMORY = wasm.__wasm.memory;

test("read file", (t) => {
  const table = loadIPCTableFromDisk("tests/data.arrow");
  const ffiTable = arrowTableToFFI(table);

  const numFields = ffiTable.schemaLength();
  const fieldPtr = ffiTable.schemaAddr(0);
  const field = parseField(WASM_MEMORY.buffer, fieldPtr);

  t.equals(field.name, "str");
  t.equals(field.typeId, new arrow.Utf8().typeId);
  t.equals(field.nullable, false);

  const arrayPtr = ffiTable.arrayAddr(0, 0);
  t.end();
});
