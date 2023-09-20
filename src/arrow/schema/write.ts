// Convert a nanoarrow `ArrowSchema` to an Arrow JS Field

import { ArrowSchema } from "../../nanoarrow";
import * as arrow from "apache-arrow";

/**
 * Convert an `ArrowSchema` object into an Arrow JS Field
 *
 * - `schema`: The `ArrowSchema` object to convert.
 *
 * @return An Arrow JS Field.
 */
export function convertSchemaToArrowJS(schema: ArrowSchema): arrow.Field {
  const type = convertFormatStringToDataType(schema.format);
  const nullable = true;
  const metadata = new Map();
  return new arrow.Field(schema.name, type, nullable, metadata);
}

function convertFormatStringToDataType(format: string): arrow.DataType {
  return new arrow.Uint16();
}
