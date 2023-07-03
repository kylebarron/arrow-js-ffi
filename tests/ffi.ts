import * as test from "tape";
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

test("primitive types non-null with copy", (t) => {
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
    const wasmVector = parseVector(
      WASM_MEMORY.buffer,
      arrayPtr,
      field.type,
      true
    );
    t.ok(arraysEqual(originalVector?.toArray(), wasmVector.toArray()));
  }
  t.end();
});

test("fixed size list", (t) => {
  let columnIndex = TEST_TABLE.schema.fields.findIndex(
    (field) => field.name == "fixedsizelist"
  );

  const originalField = TEST_TABLE.schema.fields[columnIndex];

  const fieldPtr = FFI_TABLE.schemaAddr(columnIndex);
  const field = parseField(WASM_MEMORY.buffer, fieldPtr);

  t.equals(field.name, originalField.name, "Field name should be equal.");
  t.equals(field.typeId, originalField.typeId, "Type id should be equal.");

  const arrayPtr = FFI_TABLE.arrayAddr(0, columnIndex);
  const wasmVector = parseVector(WASM_MEMORY.buffer, arrayPtr, field.type);

  t.equals(wasmVector.get(0).get(0), 1);
  t.equals(wasmVector.get(0).get(1), 2);
  t.equals(wasmVector.get(1).get(0), 3);
  t.equals(wasmVector.get(1).get(1), 4);
  t.equals(wasmVector.get(2).get(0), 5);
  t.equals(wasmVector.get(2).get(1), 6);

  t.end();
});

test("struct", (t) => {
  let columnIndex = TEST_TABLE.schema.fields.findIndex(
    (field) => field.name == "struct"
  );

  const originalField = TEST_TABLE.schema.fields[columnIndex];
  const originalVector = TEST_TABLE.getChildAt(columnIndex);
  const fieldPtr = FFI_TABLE.schemaAddr(columnIndex);
  const field = parseField(WASM_MEMORY.buffer, fieldPtr);

  t.equals(field.name, originalField.name, "Field name should be equal.");
  t.equals(field.typeId, originalField.typeId, "Type id should be equal.");

  const arrayPtr = FFI_TABLE.arrayAddr(0, columnIndex);
  const wasmVector = parseVector(WASM_MEMORY.buffer, arrayPtr, field.type);

  t.ok(
    arraysEqual(
      originalVector?.getChildAt(0)?.toArray(),
      wasmVector?.getChildAt(0)?.toArray()
    ),
    "x child is equal"
  );
  t.ok(
    arraysEqual(
      originalVector?.getChildAt(1)?.toArray(),
      wasmVector?.getChildAt(1)?.toArray()
    ),
    "y child is equal"
  );

  t.end();
});

test("binary", (t) => {
  let columnIndex = TEST_TABLE.schema.fields.findIndex(
    (field) => field.name == "binary"
  );

  const originalField = TEST_TABLE.schema.fields[columnIndex];
  // declare it's not null
  const originalVector = TEST_TABLE.getChildAt(columnIndex) as arrow.Vector;
  const fieldPtr = FFI_TABLE.schemaAddr(columnIndex);
  const field = parseField(WASM_MEMORY.buffer, fieldPtr);

  t.equals(field.name, originalField.name, "Field name should be equal.");
  t.equals(field.typeId, originalField.typeId, "Type id should be equal.");

  const arrayPtr = FFI_TABLE.arrayAddr(0, columnIndex);
  const wasmVector = parseVector(WASM_MEMORY.buffer, arrayPtr, field.type);

  t.ok(
    arraysEqual(
      originalVector?.data[0]?.valueOffsets,
      wasmVector?.data[0]?.valueOffsets
    ),
    "valueOffsets are equal"
  );
  t.ok(
    arraysEqual(originalVector?.data[0]?.values, wasmVector?.data[0]?.values),
    "values are equal"
  );

  t.end();
});

test("string", (t) => {
  let columnIndex = TEST_TABLE.schema.fields.findIndex(
    (field) => field.name == "string"
  );

  const originalField = TEST_TABLE.schema.fields[columnIndex];
  // declare it's not null
  const originalVector = TEST_TABLE.getChildAt(columnIndex) as arrow.Vector;
  const fieldPtr = FFI_TABLE.schemaAddr(columnIndex);
  const field = parseField(WASM_MEMORY.buffer, fieldPtr);

  t.equals(field.name, originalField.name, "Field name should be equal.");
  t.equals(field.typeId, originalField.typeId, "Type id should be equal.");

  const arrayPtr = FFI_TABLE.arrayAddr(0, columnIndex);
  const wasmVector = parseVector(WASM_MEMORY.buffer, arrayPtr, field.type);

  t.ok(
    arraysEqual(originalVector.toArray(), wasmVector.toArray()),
    "string data are equal"
  );

  t.end();
});

test("boolean", (t) => {
  let columnIndex = TEST_TABLE.schema.fields.findIndex(
    (field) => field.name == "boolean"
  );

  const originalField = TEST_TABLE.schema.fields[columnIndex];
  // declare it's not null
  const originalVector = TEST_TABLE.getChildAt(columnIndex) as arrow.Vector;
  const fieldPtr = FFI_TABLE.schemaAddr(columnIndex);
  const field = parseField(WASM_MEMORY.buffer, fieldPtr);

  t.equals(field.name, originalField.name, "Field name should be equal.");
  t.equals(field.typeId, originalField.typeId, "Type id should be equal.");

  const arrayPtr = FFI_TABLE.arrayAddr(0, columnIndex);
  const wasmVector = parseVector(WASM_MEMORY.buffer, arrayPtr, field.type);

  t.ok(
    arraysEqual(originalVector.toArray(), wasmVector.toArray()),
    "boolean data are equal"
  );

  t.end();
});

test("null array", (t) => {
  let columnIndex = TEST_TABLE.schema.fields.findIndex(
    (field) => field.name == "null"
  );

  const originalField = TEST_TABLE.schema.fields[columnIndex];
  // declare it's not null
  const originalVector = TEST_TABLE.getChildAt(columnIndex) as arrow.Vector;
  const fieldPtr = FFI_TABLE.schemaAddr(columnIndex);
  const field = parseField(WASM_MEMORY.buffer, fieldPtr);

  t.equals(field.name, originalField.name, "Field name should be equal.");
  t.equals(field.typeId, originalField.typeId, "Type id should be equal.");

  const arrayPtr = FFI_TABLE.arrayAddr(0, columnIndex);
  const wasmVector = parseVector(WASM_MEMORY.buffer, arrayPtr, field.type);

  t.ok(
    arraysEqual(originalVector.toArray(), wasmVector.toArray()),
    "null arrays are equal"
  );

  t.end();
});

test("list array", (t) => {
  let columnIndex = TEST_TABLE.schema.fields.findIndex(
    (field) => field.name == "list"
  );

  const originalField = TEST_TABLE.schema.fields[columnIndex];
  // declare it's not null
  const originalVector = TEST_TABLE.getChildAt(columnIndex) as arrow.Vector;
  const fieldPtr = FFI_TABLE.schemaAddr(columnIndex);
  const field = parseField(WASM_MEMORY.buffer, fieldPtr);

  t.equals(field.name, originalField.name, "Field name should be equal.");
  t.equals(field.typeId, originalField.typeId, "Type id should be equal.");

  const arrayPtr = FFI_TABLE.arrayAddr(0, columnIndex);
  const wasmVector = parseVector(WASM_MEMORY.buffer, arrayPtr, field.type);

  t.ok(
    arraysEqual(
      originalVector.getChildAt(0)?.toArray(),
      wasmVector.getChildAt(0)?.toArray()
    ),
    "underlying values are equal"
  );
  t.ok(
    arraysEqual(
      originalVector.data[0].valueOffsets,
      wasmVector.data[0].valueOffsets
    ),
    "values offsets are equal"
  );

  t.end();
});

  // console.log(originalVector.getChildAt(0)?.toArray());
  // console.log(wasmVector.toJSON());

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
