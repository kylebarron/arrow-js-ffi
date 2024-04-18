import terser from "@rollup/plugin-terser";
import typescript from "@rollup/plugin-typescript";
import dts from "rollup-plugin-dts";

const input = "./src/index.ts";
const sourcemap = true;
const external = ["apache-arrow"];

export default [
  {
    input,
    output: {
      file: "dist/arrow-js-ffi.es.mjs",
      format: "es",
      sourcemap,
    },
    plugins: [typescript()],
    external,
  },
  {
    input,
    output: {
      file: "dist/index.d.ts",
      format: "es",
    },
    plugins: [dts()],
    external,
  },
  {
    input,
    output: {
      file: "dist/arrow-js-ffi.cjs",
      format: "cjs",
      sourcemap,
    },
    plugins: [typescript()],
    external,
  },
  {
    input,
    output: {
      file: "dist/arrow-js-ffi.umd.js",
      format: "umd",
      name: "arrowJsFFI",
      sourcemap,
      globals: {
        "apache-arrow": "Arrow",
      },
    },
    plugins: [typescript(), terser()],
    external,
  },
];
