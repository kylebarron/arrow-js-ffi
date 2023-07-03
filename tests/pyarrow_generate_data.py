import pyarrow as pa
import pyarrow.feather as feather


def fixed_size_list_array() -> pa.Array:
    coords = pa.array([1, 2, 3, 4, 5, 6], type=pa.uint8())
    return pa.FixedSizeListArray.from_arrays(coords, 2)


def struct_array() -> pa.Array:
    x = pa.array([1, 2, 3], type=pa.float64())
    y = pa.array([5, 6, 7], type=pa.float64())
    return pa.StructArray.from_arrays([x, y], ["x", "y"])


def table() -> pa.Table:
    return pa.table(
        {
            "fixedsizelist": fixed_size_list_array(),
            "struct": struct_array(),
        }
    )


def main():
    feather.write_feather(table(), "table.arrow", compression="uncompressed")


if __name__ == "__main__":
    main()
