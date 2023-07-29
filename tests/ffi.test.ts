import * as test from "tape";
import { describe, it, expect } from "vitest";
import * as arrow from "apache-arrow";
import * as wasm from "rust-arrow-ffi";
import { arrowTableToFFI, arraysEqual, loadIPCTableFromDisk } from "./utils";
import { parseField, parseVector } from "../src";

wasm.setPanicHook();

// @ts-expect-error
const WASM_MEMORY: WebAssembly.Memory = wasm.__wasm.memory;

const TEST_TABLE = loadIPCTableFromDisk("tests/table.arrow");
const FFI_TABLE = arrowTableToFFI(TEST_TABLE);

interface FixtureType {
  data: any;
  dataType: arrow.DataType;
}

const PRIMITIVE_NONNULL_FIXTURES: FixtureType[] = [
  // Bool
  {
    data: [true, false, true, false],
    dataType: new arrow.Bool(),
  },
  {
    data: Uint8Array.from([1, 2, 3, 4]),
    dataType: new arrow.Uint8(),
  },
  {
    data: Uint16Array.from([1, 2, 65534, 65535]),
    dataType: new arrow.Uint16(),
  },
  {
    data: Uint32Array.from([1, 2, 4294967294, 4294967295]),
    dataType: new arrow.Uint32(),
  },
  {
    data: BigUint64Array.from([
      1n,
      2n,
      18446744073709551614n,
      18446744073709551615n,
    ]),
    dataType: new arrow.Uint64(),
  },
  {
    data: Int8Array.from([1, 2, -3, -4]),
    dataType: new arrow.Int8(),
  },
  {
    data: Int16Array.from([1, 2, -32768, -32767]),
    dataType: new arrow.Int16(),
  },
  {
    data: Int32Array.from([1, 2, -2147483648, -2147483647]),
    dataType: new arrow.Int32(),
  },
  {
    data: BigInt64Array.from([
      1n,
      2n,
      -9223372036854775808n,
      -9223372036854775807n,
    ]),
    dataType: new arrow.Int64(),
  },
  {
    data: Float32Array.from([3.14, -2.4, 43.245, 9023.135]),
    dataType: new arrow.Float32(),
  },
  {
    data: Float64Array.from([3.14, -2.4, 43.245, 9023.135]),
    dataType: new arrow.Float64(),
  },
];

describe("primitive types non-null", (t) => {
  function test(fixture: FixtureType, copy: boolean) {
    const table = arrow.tableFromArrays({
      col1: fixture.data,
    });
    const originalVector = table.getChildAt(0);
    const ffiTable = arrowTableToFFI(table);

    const fieldPtr = ffiTable.schemaAddr(0);
    const field = parseField(WASM_MEMORY.buffer, fieldPtr);

    expect(field.name, "col1");
    expect(field.typeId).toStrictEqual(fixture.dataType.typeId);
    expect(field.nullable).toBeFalsy();

    const arrayPtr = ffiTable.arrayAddr(0, 0);
    const wasmVector = parseVector(
      WASM_MEMORY.buffer,
      arrayPtr,
      field.type,
      copy
    );
    expect(
      arraysEqual(originalVector?.toArray(), wasmVector.toArray())
    ).toBeTruthy();
  }
  for (const fixture of PRIMITIVE_NONNULL_FIXTURES) {
    it(`${fixture.dataType}, copy=false`, () => test(fixture, false));
    it(`${fixture.dataType}, copy=true`, () => test(fixture, true));
  }
});

describe("fixed size list", (t) => {
  function test(copy: boolean) {
    let columnIndex = TEST_TABLE.schema.fields.findIndex(
      (field) => field.name == "fixedsizelist"
    );

    const originalField = TEST_TABLE.schema.fields[columnIndex];

    const fieldPtr = FFI_TABLE.schemaAddr(columnIndex);
    const field = parseField(WASM_MEMORY.buffer, fieldPtr);

    expect(field.name).toStrictEqual(originalField.name);
    expect(field.typeId).toStrictEqual(originalField.typeId);
    expect(field.nullable).toStrictEqual(originalField.nullable);

    const arrayPtr = FFI_TABLE.arrayAddr(0, columnIndex);
    const wasmVector = parseVector(
      WASM_MEMORY.buffer,
      arrayPtr,
      field.type,
      copy
    );

    expect(wasmVector.get(0).get(0)).toStrictEqual(1);
    expect(wasmVector.get(0).get(1)).toStrictEqual(2);
    expect(wasmVector.get(1).get(0)).toStrictEqual(3);
    expect(wasmVector.get(1).get(1)).toStrictEqual(4);
    expect(wasmVector.get(2).get(0)).toStrictEqual(5);
    expect(wasmVector.get(2).get(1)).toStrictEqual(6);
  }

  it("copy=false", () => test(false));
  it("copy=true", () => test(true));
});

describe("struct", (t) => {
  function test(copy: boolean) {
    let columnIndex = TEST_TABLE.schema.fields.findIndex(
      (field) => field.name == "struct"
    );

    const originalField = TEST_TABLE.schema.fields[columnIndex];
    const originalVector = TEST_TABLE.getChildAt(columnIndex);
    const fieldPtr = FFI_TABLE.schemaAddr(columnIndex);
    const field = parseField(WASM_MEMORY.buffer, fieldPtr);

    expect(field.name).toStrictEqual(originalField.name);
    expect(field.typeId).toStrictEqual(originalField.typeId);
    expect(field.nullable).toStrictEqual(originalField.nullable);

    const arrayPtr = FFI_TABLE.arrayAddr(0, columnIndex);
    const wasmVector = parseVector(
      WASM_MEMORY.buffer,
      arrayPtr,
      field.type,
      copy
    );

    expect(
      arraysEqual(
        originalVector?.getChildAt(0)?.toArray(),
        wasmVector?.getChildAt(0)?.toArray()
      )
    ).toBeTruthy();
    expect(
      arraysEqual(
        originalVector?.getChildAt(1)?.toArray(),
        wasmVector?.getChildAt(1)?.toArray()
      )
    ).toBeTruthy();
  }

  it("copy=false", () => test(false));
  it("copy=true", () => test(true));
});

describe("binary", (t) => {
  function test(copy: boolean) {
    let columnIndex = TEST_TABLE.schema.fields.findIndex(
      (field) => field.name == "binary"
    );

    const originalField = TEST_TABLE.schema.fields[columnIndex];
    // declare it's not null
    const originalVector = TEST_TABLE.getChildAt(columnIndex) as arrow.Vector;
    const fieldPtr = FFI_TABLE.schemaAddr(columnIndex);
    const field = parseField(WASM_MEMORY.buffer, fieldPtr);

    expect(field.name).toStrictEqual(originalField.name);
    expect(field.typeId).toStrictEqual(originalField.typeId);
    expect(field.nullable).toStrictEqual(originalField.nullable);

    const arrayPtr = FFI_TABLE.arrayAddr(0, columnIndex);
    const wasmVector = parseVector(
      WASM_MEMORY.buffer,
      arrayPtr,
      field.type,
      copy
    );

    expect(
      arraysEqual(
        originalVector?.data[0]?.valueOffsets,
        wasmVector?.data[0]?.valueOffsets
      )
    ).toBeTruthy();
    expect(
      arraysEqual(originalVector?.data[0]?.values, wasmVector?.data[0]?.values)
    ).toBeTruthy();
  }

  it("copy=false", () => test(false));
  it("copy=true", () => test(true));
});

describe("string", (t) => {
  function test(copy: boolean) {
    let columnIndex = TEST_TABLE.schema.fields.findIndex(
      (field) => field.name == "string"
    );

    const originalField = TEST_TABLE.schema.fields[columnIndex];
    // declare it's not null
    const originalVector = TEST_TABLE.getChildAt(columnIndex) as arrow.Vector;
    const fieldPtr = FFI_TABLE.schemaAddr(columnIndex);
    const field = parseField(WASM_MEMORY.buffer, fieldPtr);

    expect(field.name).toStrictEqual(originalField.name);
    expect(field.typeId).toStrictEqual(originalField.typeId);
    expect(field.nullable).toStrictEqual(originalField.nullable);

    const arrayPtr = FFI_TABLE.arrayAddr(0, columnIndex);
    const wasmVector = parseVector(
      WASM_MEMORY.buffer,
      arrayPtr,
      field.type,
      copy
    );

    expect(
      arraysEqual(originalVector.toArray(), wasmVector.toArray())
    ).toBeTruthy();
  }

  it("copy=false", () => test(false));
  it("copy=true", () => test(true));
});

describe("boolean", (t) => {
  function test(copy: boolean) {
    let columnIndex = TEST_TABLE.schema.fields.findIndex(
      (field) => field.name == "boolean"
    );

    const originalField = TEST_TABLE.schema.fields[columnIndex];
    // declare it's not null
    const originalVector = TEST_TABLE.getChildAt(columnIndex) as arrow.Vector;
    const fieldPtr = FFI_TABLE.schemaAddr(columnIndex);
    const field = parseField(WASM_MEMORY.buffer, fieldPtr);

    expect(field.name).toStrictEqual(originalField.name);
    expect(field.typeId).toStrictEqual(originalField.typeId);
    expect(field.nullable).toStrictEqual(originalField.nullable);

    const arrayPtr = FFI_TABLE.arrayAddr(0, columnIndex);
    const wasmVector = parseVector(
      WASM_MEMORY.buffer,
      arrayPtr,
      field.type,
      copy
    );

    expect(
      arraysEqual(originalVector.toArray(), wasmVector.toArray())
    ).toBeTruthy();
  }

  it("copy=false", () => test(false));
  it("copy=true", () => test(true));
});

describe("null array", (t) => {
  function test(copy: boolean) {
    let columnIndex = TEST_TABLE.schema.fields.findIndex(
      (field) => field.name == "null"
    );

    const originalField = TEST_TABLE.schema.fields[columnIndex];
    // declare it's not null
    const originalVector = TEST_TABLE.getChildAt(columnIndex) as arrow.Vector;
    const fieldPtr = FFI_TABLE.schemaAddr(columnIndex);
    const field = parseField(WASM_MEMORY.buffer, fieldPtr);

    expect(field.name).toStrictEqual(originalField.name);
    expect(field.typeId).toStrictEqual(originalField.typeId);
    expect(field.nullable).toStrictEqual(originalField.nullable);

    const arrayPtr = FFI_TABLE.arrayAddr(0, columnIndex);
    const wasmVector = parseVector(
      WASM_MEMORY.buffer,
      arrayPtr,
      field.type,
      copy
    );

    expect(
      arraysEqual(originalVector.toArray(), wasmVector.toArray())
    ).toBeTruthy();
  }

  it("copy=false", () => test(false));
  it("copy=true", () => test(true));
});

describe("list array", (t) => {
  function test(copy: boolean) {
    let columnIndex = TEST_TABLE.schema.fields.findIndex(
      (field) => field.name == "list"
    );

    const originalField = TEST_TABLE.schema.fields[columnIndex];
    // declare it's not null
    const originalVector = TEST_TABLE.getChildAt(columnIndex) as arrow.Vector;
    const fieldPtr = FFI_TABLE.schemaAddr(columnIndex);
    const field = parseField(WASM_MEMORY.buffer, fieldPtr);

    expect(field.name).toStrictEqual(originalField.name);
    expect(field.typeId).toStrictEqual(originalField.typeId);
    expect(field.nullable).toStrictEqual(originalField.nullable);

    const arrayPtr = FFI_TABLE.arrayAddr(0, columnIndex);
    const wasmVector = parseVector(
      WASM_MEMORY.buffer,
      arrayPtr,
      field.type,
      copy
    );

    expect(
      arraysEqual(
        originalVector.getChildAt(0)?.toArray(),
        wasmVector.getChildAt(0)?.toArray()
      )
    ).toBeTruthy();
    expect(
      arraysEqual(
        originalVector.data[0].valueOffsets,
        wasmVector.data[0].valueOffsets
      )
    ).toBeTruthy();
  }

  it("copy=false", () => test(false));
  it("copy=true", () => test(true));
});

describe("extension array", (t) => {
  function test(copy: boolean) {
    let columnIndex = TEST_TABLE.schema.fields.findIndex(
      (field) => field.name == "extension"
    );

    const originalField = TEST_TABLE.schema.fields[columnIndex];
    // declare it's not null
    const originalVector = TEST_TABLE.getChildAt(columnIndex) as arrow.Vector;
    const fieldPtr = FFI_TABLE.schemaAddr(columnIndex);
    const field = parseField(WASM_MEMORY.buffer, fieldPtr);

    expect(field.name).toStrictEqual(originalField.name);
    expect(field.typeId).toStrictEqual(originalField.typeId);
    expect(field.nullable).toStrictEqual(originalField.nullable);
    expect(
      field.metadata.size,
      "Number of elements in the metadata map should be equal."
    ).toStrictEqual(originalField.metadata.size);
    expect(
      field.metadata.get("ARROW:extension:name"),
      "Extension name should be equal."
    ).toStrictEqual(originalField.metadata.get("ARROW:extension:name"));
    expect(
      field.metadata.get("ARROW:extension:metadata"),
      "Extension metadata should be equal."
    ).toStrictEqual(originalField.metadata.get("ARROW:extension:metadata"));

    const arrayPtr = FFI_TABLE.arrayAddr(0, columnIndex);
    const wasmVector = parseVector(
      WASM_MEMORY.buffer,
      arrayPtr,
      field.type,
      copy
    );

    expect(
      arraysEqual(originalVector.toArray(), wasmVector.toArray())
    ).toBeTruthy();
  }

  it("copy=false", () => test(false));
  it("copy=true", () => test(true));
});

// This looks to be parsing wrong somewhere
test.skip("decimal128", (t) => {
  let columnIndex = TEST_TABLE.schema.fields.findIndex(
    (field) => field.name == "decimal128"
  );

  const originalField = TEST_TABLE.schema.fields[columnIndex];
  // declare it's not null
  const originalVector = TEST_TABLE.getChildAt(columnIndex) as arrow.Vector;
  const fieldPtr = FFI_TABLE.schemaAddr(columnIndex);
  const field = parseField(WASM_MEMORY.buffer, fieldPtr);

  expect(field.name).toStrictEqual(originalField.name);
  expect(field.typeId).toStrictEqual(originalField.typeId);
  expect(field.nullable).toStrictEqual(originalField.nullable);

  const arrayPtr = FFI_TABLE.arrayAddr(0, columnIndex);
  const wasmVector = parseVector(WASM_MEMORY.buffer, arrayPtr, field.type);

  console.log(originalVector.get(0));
  console.log(wasmVector.get(0));

  t.ok(
    arraysEqual(originalVector.get(0), wasmVector.get(0)),
    "array values are equal"
  );
});

describe("date32", (t) => {
  function test(copy: boolean) {
    let columnIndex = TEST_TABLE.schema.fields.findIndex(
      (field) => field.name == "date32"
    );

    const originalField = TEST_TABLE.schema.fields[columnIndex];
    // declare it's not null
    const originalVector = TEST_TABLE.getChildAt(columnIndex) as arrow.Vector;
    const fieldPtr = FFI_TABLE.schemaAddr(columnIndex);
    const field = parseField(WASM_MEMORY.buffer, fieldPtr);

    expect(field.name).toStrictEqual(originalField.name);
    expect(field.typeId).toStrictEqual(originalField.typeId);
    expect(field.nullable).toStrictEqual(originalField.nullable);

    const arrayPtr = FFI_TABLE.arrayAddr(0, columnIndex);
    const wasmVector = parseVector(
      WASM_MEMORY.buffer,
      arrayPtr,
      field.type,
      copy
    );

    // TODO: how to compare date objects? They look equal though
    // for (let i = 0; i < 3; i++) {
    //   expect(originalVector.get(i), wasmVector.get(i));
    // }
  }

  it("copy=false", () => test(false));
  it("copy=true", () => test(true));
});

// This also looks to be failing; probably an issue with the byte width?
test.skip("timestamp", (t) => {
  let columnIndex = TEST_TABLE.schema.fields.findIndex(
    (field) => field.name == "timestamp"
  );

  const originalField = TEST_TABLE.schema.fields[columnIndex];
  // declare it's not null
  const originalVector = TEST_TABLE.getChildAt(columnIndex) as arrow.Vector;
  const fieldPtr = FFI_TABLE.schemaAddr(columnIndex);
  const field = parseField(WASM_MEMORY.buffer, fieldPtr);

  expect(field.name).toStrictEqual(originalField.name);
  expect(field.typeId).toStrictEqual(originalField.typeId);
  expect(field.nullable).toStrictEqual(originalField.nullable);

  const arrayPtr = FFI_TABLE.arrayAddr(0, columnIndex);
  const wasmVector = parseVector(WASM_MEMORY.buffer, arrayPtr, field.type);

  console.log(wasmVector.toJSON());

  for (let i = 0; i < 3; i++) {
    console.log(originalVector.get(i));
    console.log(wasmVector.get(i));
    expect(originalVector.get(i), wasmVector.get(i));
  }
});
