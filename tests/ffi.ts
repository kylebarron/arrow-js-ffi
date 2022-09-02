import * as test from "tape";
import * as arrow from "apache-arrow";
import * as wasm from "rust-arrow-ffi";
import { arrowTableToFFI, arraysEqual } from "./utils";
import { parseField, parseVector } from "../src";

wasm.setPanicHook();

// @ts-expect-error
const WASM_MEMORY = wasm.__wasm.memory;

interface FixtureType {
  data: any;
  dataType: arrow.DataType;
}

const PRIMITIVE_TYPE_FIXTURES: FixtureType[] = [
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
];

test("primitive types non-null", (t) => {
  for (const fixture of PRIMITIVE_TYPE_FIXTURES) {
    const table = arrow.tableFromArrays({
      col1: fixture.data,
    });
    const originalVector = table.getChildAt(0);
    const ffiTable = arrowTableToFFI(table);

    const fieldPtr = ffiTable.schemaAddr(0);
    const field = parseField(WASM_MEMORY.buffer, fieldPtr);

    t.equals(field.name, "col1");
    t.equals(field.typeId, fixture.dataType.typeId);
    t.equals(field.nullable, false);

    const arrayPtr = ffiTable.arrayAddr(0, 0);
    const wasmVector = parseVector(WASM_MEMORY.buffer, arrayPtr, field.type);
    t.ok(arraysEqual(originalVector?.toArray(), wasmVector.toArray()));
  }
  t.end();
});






  const fieldPtr = ffiTable.schemaAddr(0);
  const field = parseField(WASM_MEMORY.buffer, fieldPtr);

  t.equals(field.name, "str");
  t.equals(field.typeId, new arrow.Utf8().typeId);
  t.equals(field.nullable, false);

  const arrayPtr = ffiTable.arrayAddr(0, 0);
  t.end();
});
