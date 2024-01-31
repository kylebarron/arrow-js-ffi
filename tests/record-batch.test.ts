import { describe, it, expect } from "vitest";
import * as wasm from "rust-arrow-ffi";
import {
  arraysEqual,
  loadIPCTableFromDisk,
  arrowTableToFFIRecordBatch,
} from "./utils";
import { parseRecordBatch } from "../src";
import { readFileSync } from "fs";

wasm.setPanicHook();

const WASM_MEMORY = wasm.wasmMemory()

const TEST_TABLE = loadIPCTableFromDisk("tests/table.arrow");
const FFI_RECORD_BATCH = arrowTableToFFIRecordBatch(TEST_TABLE);

describe("record batch", (t) => {
  function test(copy: boolean) {
    const newRecordBatch = parseRecordBatch(
      WASM_MEMORY.buffer,
      FFI_RECORD_BATCH.array_addr(),
      FFI_RECORD_BATCH.field_addr(),
      copy
    );

    if (TEST_TABLE.batches.length !== 1) {
      throw new Error("Expected one batch from IPC table");
    }
    let recordBatch = TEST_TABLE.batches[0];

    expect(
      arraysEqual(recordBatch.schema.names, newRecordBatch.schema.names)
    ).toBeTruthy();
    // Note: the schema metadata here is just an empty Map
    expect(recordBatch.schema.metadata).toStrictEqual(
      newRecordBatch.schema.metadata
    );
    expect(recordBatch.nullCount).toStrictEqual(newRecordBatch.nullCount);
    expect(recordBatch.numCols).toStrictEqual(newRecordBatch.numCols);
    expect(recordBatch.numRows).toStrictEqual(newRecordBatch.numRows);
  }

  it("copy=false", () => test(false));
  it("copy=true", () => test(true));
});

describe("record batch with large types", (t) => {
  // Note that this list should be kept in sync with the Python generation script, but it shouldn't
  // fail if not in sync
  const expectedColumnNames = ["large_binary", "large_string", "large_list"];

  function test(copy: boolean) {
    const tableBuffer = readFileSync("tests/large_table.arrow");
    let ffiRecordBatch = wasm.arrowIPCToFFIRecordBatch(tableBuffer);

    const newRecordBatch = parseRecordBatch(
      WASM_MEMORY.buffer,
      ffiRecordBatch.array_addr(),
      ffiRecordBatch.field_addr(),
      copy
    );

    for (let i = 0; i < expectedColumnNames.length; i++) {
      const element = expectedColumnNames[i];
      expect(
        newRecordBatch.schema.fields[i].name,
        "Column names should be equal"
      ).toStrictEqual(element);
    }

    expect(newRecordBatch.numCols).toStrictEqual(3);
    expect(newRecordBatch.numRows).toStrictEqual(3);
  }

  it("copy=false", () => test(false));
  it("copy=true", () => test(true));
});
