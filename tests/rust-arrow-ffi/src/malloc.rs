use wasm_bindgen::prelude::*;

/// Allocate memory into the module's linear memory
/// and return the offset to the start of the block.
/// Taken from
/// https://radu-matei.com/blog/practical-guide-to-wasm-memory/#passing-arrays-to-rust-webassembly-modules
#[wasm_bindgen]
pub fn malloc(len: usize) -> *mut u64 {
    // Note: we "fake" an array of u64, not u8, so that the Rust allocator will align on 8 bytes.
    let u64_len = (len as f64 / 8.0).ceil() as usize;

    // create a new mutable buffer with capacity `len`
    let mut buf = Vec::with_capacity(u64_len);

    // take a mutable pointer to the buffer
    let ptr = buf.as_mut_ptr();

    // take ownership of the memory block and
    // ensure that its destructor is not
    // called when the object goes out of scope
    // at the end of the function
    std::mem::forget(buf);

    // return the pointer so the runtime
    // can write data at this offset
    ptr
}
