import { describe, it, expect } from "vitest";
import * as wasm from "rust-arrow-ffi";
import { arrowTableToFFI, arraysEqual, loadIPCTableFromDisk } from "./utils";
import {
  ArrowArray,
  ArrowSchema,
  parseField,
  parseVector,
  readArrayFFI,
  readSchemaFFI,
  writeArrayFFI,
  writeSchemaFFI,
} from "../src";
import { Type } from "../src/types";
import { ArrowStaticType } from "../src/nanoarrow/schema";

wasm.setPanicHook();

const WASM_MEMORY: WebAssembly.Memory = wasm._memory();

describe("write-read round trip", (t) => {
  it("test", () => {
    const values = new Uint8Array([0, 1, 2, 3]);
    const array: ArrowArray = {
      length: 4,
      nullCount: 0,
      offset: 0,
      buffers: [null, values],
      children: [],
      dictionary: null,
    };
    const schema: ArrowSchema = {
      format: ArrowStaticType.Uint8,
      name: "hello world!",
      metadata: null,
      flags: 0n,
      children: [],
      dictionary: null,
    };
    const schemaPtr = writeSchemaFFI(schema, WASM_MEMORY, wasm.malloc);
    const arrayPtr = writeArrayFFI(array, WASM_MEMORY, wasm.malloc);

    const newSchema = readSchemaFFI(WASM_MEMORY.buffer, schemaPtr);
    const newArray = readArrayFFI(
      WASM_MEMORY.buffer,
      arrayPtr,
      newSchema,
      true
    );

    console.log(newSchema);
    console.log(newArray);

    expect(schema.format).toStrictEqual(newSchema.format);
    expect(schema.name).toStrictEqual(newSchema.name);
    expect(schema.metadata).toStrictEqual(newSchema.metadata);
    expect(schema.flags).toStrictEqual(newSchema.flags);

    expect(array.length).toStrictEqual(newArray.length);
    expect(array.nullCount).toStrictEqual(newArray.nullCount);
    expect(array.offset).toStrictEqual(newArray.offset);
    expect(array.buffers[0]).toStrictEqual(newArray.buffers[0]);
    expect(array.buffers[1]).toStrictEqual(newArray.buffers[1]);
  });
});

describe("write-read round trip", (t) => {
  it("test", () => {
    const values = new Uint8Array([0, 1, 2, 3]);
    const array: ArrowArray = {
      length: 4,
      nullCount: 0,
      offset: 0,
      buffers: [null, values],
      children: [],
      dictionary: null,
    };
    const schema: ArrowSchema = {
      format: ArrowStaticType.Uint8,
      name: "hello world!",
      metadata: null,
      flags: 0n,
      children: [],
      dictionary: null,
    };
    const schemaPtr = writeSchemaFFI(schema, WASM_MEMORY, wasm.malloc);
    const arrayPtr = writeArrayFFI(array, WASM_MEMORY, wasm.malloc);

    let wasmArray = wasm.FFIArrowArray.newUnsafe(arrayPtr);
    let wasmSchema = wasm.FFIArrowField.newUnsafe(schemaPtr);

    wasmArray.import(wasmSchema)
  });
});
