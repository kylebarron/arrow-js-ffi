import { ArrowSchema } from "../../nanoarrow/schema";

const UTF8_DECODER = new TextDecoder("utf-8");

/**
 * Parse an [`ArrowSchema`](https://arrow.apache.org/docs/format/CDataInterface.html#the-arrowschema-structure) C FFI struct into an `ArrowSchema` instance.
 *
 * This always copies from the underlying C FFI struct.
 *
 * - `buffer` (`ArrayBuffer`): The [`WebAssembly.Memory`](https://developer.mozilla.org/en-US/docs/WebAssembly/JavaScript_interface/Memory) instance to read from.
 * - `ptr` (`number`): The numeric pointer in `buffer` where the C struct is located.
 */
export function readSchemaFFI(
  buffer: ArrayBuffer,
  ptr: number
): ArrowSchema {
  const dataView = new DataView(buffer);

  const formatPtr = dataView.getUint32(ptr, true);
  const formatString = parseNullTerminatedString(dataView, formatPtr);

  const namePtr = dataView.getUint32(ptr + 4, true);
  const name = parseNullTerminatedString(dataView, namePtr);

  const metadataPtr = dataView.getUint32(ptr + 8, true);
  const metadata = parseMetadata(dataView, metadataPtr);

  // Extra 4 to be 8-byte aligned
  const flags = dataView.getBigInt64(ptr + 16, true);
  const nChildren = dataView.getBigInt64(ptr + 24, true);

  const ptrToChildrenPtrs = dataView.getUint32(ptr + 32, true);
  const childrenFields: ArrowSchema[] = new Array(Number(nChildren));
  for (let i = 0; i < nChildren; i++) {
    childrenFields[i] = readSchemaFFI(
      buffer,
      dataView.getUint32(ptrToChildrenPtrs + i * 4, true)
    );
  }

  return {
    format: formatString,
    name,
    metadata,
    flags,
    children: childrenFields,
    dictionary: null,
  };
}

/** Parse a null-terminated C-style string */
function parseNullTerminatedString(
  dataView: DataView,
  ptr: number,
  maxBytesToRead: number = Infinity
): string {
  const maxPtr = Math.min(ptr + maxBytesToRead, dataView.byteLength);
  let end = ptr;
  while (end < maxPtr && dataView.getUint8(end) !== 0) {
    end += 1;
  }

  return UTF8_DECODER.decode(new Uint8Array(dataView.buffer, ptr, end - ptr));
}

/**
 * Parse field metadata
 *
 * The metadata format is described here:
 * https://arrow.apache.org/docs/format/CDataInterface.html#c.ArrowSchema.metadata
 */
function parseMetadata(
  dataView: DataView,
  ptr: number
): Map<string, string> | null {
  const numEntries = dataView.getInt32(ptr, true);
  if (numEntries === 0) {
    return null;
  }

  const metadata: Map<string, string> = new Map();

  ptr += 4;
  for (let i = 0; i < numEntries; i++) {
    const keyByteLength = dataView.getInt32(ptr, true);
    ptr += 4;
    const key = UTF8_DECODER.decode(
      new Uint8Array(dataView.buffer, ptr, keyByteLength)
    );
    ptr += keyByteLength;

    const valueByteLength = dataView.getInt32(ptr, true);
    ptr += 4;
    const value = UTF8_DECODER.decode(
      new Uint8Array(dataView.buffer, ptr, valueByteLength)
    );
    ptr += valueByteLength;

    metadata.set(key, value);
  }

  return metadata;
}
