# Changelog

## [0.4.3] - 2025-08-28

- Fix reading 64-bit Timestamp and Duration types https://github.com/kylebarron/arrow-js-ffi/pull/130

## [0.4.2] - 2024-04-18

- Allow `Uint32Array` type in `parseTable`.
- Ensure `apache-arrow` is marked as an external library in Rollup

## [0.4.1] - 2024-01-31

- Add clean and build to `prepublishOnly`. 0.4.0 had accidentally been published with stale code.

## [0.4.0] - 2024-01-31

**Yanked**

- Support for null bitmaps, union arrays, duration arrays, dictionary-encoded arrays, map arrays.
- Set `copy` to `true` by default. To create zero-copy views, pass `copy=false`.
- Bump `apache-arrow` peer dependency to v15.
- Add `parseSchema` by @kylebarron in https://github.com/kylebarron/arrow-js-ffi/pull/72
- Fix passing down `copy` when parsing children by @kylebarron in https://github.com/kylebarron/arrow-js-ffi/pull/44
- Automated publishing via CI by @kylebarron in https://github.com/kylebarron/arrow-js-ffi/pull/92
- memory management doc by @kylebarron in https://github.com/kylebarron/arrow-js-ffi/pull/98

**Full Changelog**: https://github.com/kylebarron/arrow-js-ffi/compare/v0.3.0...v0.4.0

## [0.3.0] - 2023-08-15

- Add tests copying data across boundary by @kylebarron in https://github.com/kylebarron/arrow-js-ffi/pull/32
- Support "large" types with int64 offsets by @kylebarron in https://github.com/kylebarron/arrow-js-ffi/pull/33
- Parse record batch from FFI by @kylebarron in https://github.com/kylebarron/arrow-js-ffi/pull/37 and https://github.com/kylebarron/arrow-js-ffi/pull/39

## [0.2.0] - 2023-07-09

- Revamped bundling ([#30](https://github.com/kylebarron/arrow-js-ffi/pull/30))

## [0.1.0] - 2023-07-03

- Initial release
- Working `parseField` and `parseVector` for many vector data types.
