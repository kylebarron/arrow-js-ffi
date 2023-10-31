mod error;
mod ffi;
use arrow2::io::ipc::read::{read_file_metadata, FileReader as IPCFileReader};
use std::io::Cursor;

use crate::error::WasmResult;
use crate::ffi::{FFIArrowChunk, FFIArrowRecordBatch, FFIArrowSchema, FFIArrowTable};
use wasm_bindgen::prelude::*;

#[wasm_bindgen(js_name = arrowIPCToFFI)]
pub fn arrow_ipc_to_ffi(arrow_file: &[u8]) -> WasmResult<FFIArrowTable> {
    // Create IPC reader
    let mut input_file = Cursor::new(arrow_file);
    let stream_metadata = read_file_metadata(&mut input_file)?;
    let arrow_ipc_reader = IPCFileReader::new(input_file, stream_metadata.clone(), None, None);
    let schema = &stream_metadata.schema;

    let ffi_schema: FFIArrowSchema = schema.into();
    let mut ffi_chunks: Vec<FFIArrowChunk> = vec![];

    // Iterate over reader chunks, storing each in memory to be used for FFI
    for maybe_chunk in arrow_ipc_reader {
        let chunk = maybe_chunk?;
        ffi_chunks.push(chunk.into());
    }

    Ok((ffi_schema, ffi_chunks).into())
}

#[wasm_bindgen(js_name = arrowIPCToFFIRecordBatch)]
pub fn arrow_ipc_to_ffi_record_batch(
    arrow_file: &[u8],
    chunk_idx: Option<usize>,
) -> WasmResult<FFIArrowRecordBatch> {
    // Create IPC reader
    let mut input_file = Cursor::new(arrow_file);
    let stream_metadata = read_file_metadata(&mut input_file)?;
    let arrow_ipc_reader = IPCFileReader::new(input_file, stream_metadata.clone(), None, None);
    let schema = &stream_metadata.schema;

    // Take the nth chunk and store it in memory to be used for FFI
    if let Some(maybe_chunk) = arrow_ipc_reader.into_iter().nth(chunk_idx.unwrap_or(0)) {
        let chunk = maybe_chunk?;
        Ok(FFIArrowRecordBatch::from_chunk(chunk, schema.clone()))
    } else {
        Err(JsError::new("Index out of range"))
    }
}

#[wasm_bindgen(js_name = setPanicHook)]
pub fn set_panic_hook() {
    // When the `console_error_panic_hook` feature is enabled, we can call the
    // `set_panic_hook` function at least once during initialization, and then
    // we will get better error messages if our code ever panics.
    //
    // For more details see
    // https://github.com/rustwasm/console_error_panic_hook#readme
    console_error_panic_hook::set_once();
}
