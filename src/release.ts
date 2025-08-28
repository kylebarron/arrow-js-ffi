export function releaseSchema(
  buffer: ArrayBuffer,
  ptr: number,
  funcTable: WebAssembly.Table,
): void {
  const dataView = new DataView(buffer);
  const releasePtr = dataView.getUint32(ptr + 40, true);
  const releaseCallback = funcTable.get(releasePtr);
  releaseCallback();
}
