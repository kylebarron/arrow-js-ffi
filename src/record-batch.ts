import * as arrow from "apache-arrow";
import { parseField } from "./field";
import { parseData } from "./vector";

export function parseRecordBatch(
  buffer: ArrayBuffer,
  arrayPtr: number,
  schemaPtr: number,
  copy: boolean = false
): arrow.RecordBatch {
  const field = parseField(buffer, schemaPtr);
  if (!isStructField(field)) {
    throw new Error("Expected struct");
  }

  const data = parseData(buffer, arrayPtr, field.type, copy);
  const outSchema = unpackStructField(field);
  return new arrow.RecordBatch(outSchema, data);
}

function isStructField(field: arrow.Field): field is arrow.Field<arrow.Struct> {
  return field.typeId == arrow.Type.Struct;
}

function unpackStructField(field: arrow.Field<arrow.Struct>): arrow.Schema {
  const fields = field.type.children;
  const metadata = field.metadata;
  // TODO: support dictionaries parameter for dictionary-encoded arrays
  return new arrow.Schema(fields, metadata);
}
