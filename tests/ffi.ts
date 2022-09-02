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

test("primitive types non-null", (t) => {
  for (const fixture of PRIMITIVE_NONNULL_FIXTURES) {
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

// test.skip("utf8 non-null", (t) => {
//   const table = arrow.tableFromArrays({
//     col1: ["a", "b", "c", "d"],
//   });
//   const ffiTable = arrowTableToFFI(table);
//   console.log("test");
//   const fieldPtr = ffiTable.schemaAddr(0);
//   const field = parseField(WASM_MEMORY.buffer, fieldPtr);

//   t.equals(field.name, "col1");
//   t.equals(field.typeId, new arrow.Utf8().typeId);
//   t.equals(field.nullable, false);

//   const arrayPtr = ffiTable.arrayAddr(0, 0);
//   const wasmVector = parseVector(WASM_MEMORY.buffer, arrayPtr, field.type);
//   t.equals(wasmVector, table.getChildAt(0));

//   console.log("table", table);
//   console.log("table", table.schema.fields);
//   console.log(table.toString());

//   t.end();

//   // const builder = arrow.makeBuilder({
//   //   type: new arrow.Utf8(),
//   //   nullValues: null
//   // });
//   // builder.append("a");
//   // builder.append("b");
//   // builder.append("c");
//   // builder.append("d");

//   // const vector = builder.finish().toVector();
//   // const schema = new arrow.Schema([new arrow.Field("col1", new arrow.Utf8(), false)]);

//   // // new arrow.RecordBatch()
//   // // arrow.makeData<arrow.Struct>()
//   // // @ts-ignore
//   // const recordBatchData = arrow.makeData<arrow.Struct>({ type: new arrow.Struct(), children: [vector.data] });
//   // const recordBatch = new arrow.RecordBatch(
//   //   schema,
//   //   recordBatchData
//   // );
//   // const table = new arrow.Table(schema, recordBatch);
//   // console.log('table', table.schema.fields)

//   // const ffiTable = arrowTableToFFI(table);
//   // const fieldPtr = ffiTable.schemaAddr(0);
//   // const field = parseField(WASM_MEMORY.buffer, fieldPtr);

//   // t.equals(field.name, "col1");
//   // t.equals(field.typeId, new arrow.Utf8().typeId);
//   // t.equals(field.nullable, false);

//   // const arrayPtr = ffiTable.arrayAddr(0, 0);
//   // const wasmVector = parseVector(WASM_MEMORY.buffer, arrayPtr, field.type);
//   // console.log(wasmVector.toString())

//   // t.end();
//   // new arrow.RecordBatch(schema, )
// });
