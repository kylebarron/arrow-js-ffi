import { describe, it, expect } from "vitest";
import * as wasm from "rust-arrow-ffi";
import { arrowTableToFFI, loadIPCTableFromDisk } from "./utils";
import { releaseSchema } from "../src";

wasm.setPanicHook();

const WASM_MEMORY = wasm.wasmMemory();

describe("test release", (t) => {
  it("should release schema", () => {
    const TEST_TABLE = loadIPCTableFromDisk("tests/table.arrow");
    const FFI_TABLE = arrowTableToFFI(TEST_TABLE);

    const funcTable = wasm._functionTable();
    const schemaAddr = FFI_TABLE.schemaAddr(0);
    releaseSchema(WASM_MEMORY.buffer, schemaAddr, funcTable);
  });
});
