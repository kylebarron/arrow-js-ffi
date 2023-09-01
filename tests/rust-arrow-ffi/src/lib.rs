mod error;
mod ffi;
pub mod ipc;
pub mod malloc;
use wasm_bindgen::prelude::*;

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

/// Returns a handle to this wasm instance's `WebAssembly.Memory`
#[wasm_bindgen(js_name = _memory)]
pub fn memory() -> JsValue {
    wasm_bindgen::memory()
}

/// Returns a handle to this wasm instance's `WebAssembly.Table` which is the indirect function
/// table used by Rust
#[wasm_bindgen(js_name = _functionTable)]
pub fn function_table() -> JsValue {
    wasm_bindgen::function_table()
}
