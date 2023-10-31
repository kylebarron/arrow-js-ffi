from datetime import date, datetime
from decimal import Decimal

import numpy as np
import pyarrow as pa
import pyarrow.feather as feather


def fixed_size_list_array() -> pa.Array:
    coords = pa.array([1, 2, 3, 4, 5, 6], type=pa.uint8())
    return pa.FixedSizeListArray.from_arrays(coords, 2)


def struct_array() -> pa.Array:
    x = pa.array([1, 2, 3], type=pa.float64())
    y = pa.array([5, 6, 7], type=pa.float64())
    return pa.StructArray.from_arrays([x, y], ["x", "y"])


def binary_array() -> pa.Array:
    arr = pa.array(np.array([b"a", b"ab", b"abc"]))
    assert isinstance(arr, pa.BinaryArray)
    return arr


def large_binary_array() -> pa.Array:
    # For some reason you can't pass pa.large_binary() directly into pa.array, you need
    # to upcast from an existing binary array
    small_arr = pa.array(np.array([b"a", b"ab", b"abc"]), type=pa.binary())
    large_arr = small_arr.cast(pa.large_binary())
    assert isinstance(large_arr, pa.LargeBinaryArray)
    return large_arr


def fixed_size_binary_array() -> pa.Array:
    # TODO: don't know how to construct this with pyarrow?
    arr = pa.array(np.array([b"a", b"b", b"c"]))
    assert isinstance(arr, pa.FixedSizeBinaryArray)
    return arr


def string_array() -> pa.Array:
    arr = pa.StringArray.from_pandas(["a", "foo", "barbaz"])
    assert isinstance(arr, pa.StringArray)
    return arr


def large_string_array() -> pa.Array:
    arr = pa.array(["a", "foo", "barbaz"], type=pa.large_string())
    assert isinstance(arr, pa.LargeStringArray)
    return arr


def boolean_array() -> pa.Array:
    arr = pa.BooleanArray.from_pandas([True, False, True])
    assert isinstance(arr, pa.BooleanArray)
    return arr


def null_array() -> pa.Array:
    arr = pa.NullArray.from_pandas([None, None, None])
    assert isinstance(arr, pa.NullArray)
    return arr


def list_array() -> pa.Array:
    values = pa.array([1, 2, 3, 4, 5, 6], type=pa.uint8())
    offsets = pa.array([0, 1, 3, 6], type=pa.int32())
    arr = pa.ListArray.from_arrays(offsets, values)
    assert isinstance(arr, pa.ListArray)
    return arr


def large_list_array() -> pa.Array:
    values = pa.array([1, 2, 3, 4, 5, 6], type=pa.uint8())
    offsets = pa.array([0, 1, 3, 6], type=pa.int64())
    arr = pa.LargeListArray.from_arrays(offsets, values)
    assert isinstance(arr, pa.LargeListArray)
    return arr


def decimal128_array() -> pa.Array:
    arr = pa.Decimal128Array.from_pandas(
        [Decimal("1.23"), Decimal("2.67"), Decimal("4.93")], type=pa.decimal128(10, 3)
    )
    assert isinstance(arr, pa.Decimal128Array)
    return arr


def date32_array() -> pa.Array:
    arr = pa.Date32Array.from_pandas(
        [date(2021, 1, 3), date(2021, 5, 6), date(2021, 8, 9)]
    )
    assert isinstance(arr, pa.Date32Array)
    return arr


def date64_array() -> pa.Array:
    arr = pa.Date64Array.from_pandas(
        [date(2021, 1, 3), date(2021, 5, 6), date(2021, 8, 9)], type=pa.date64()
    )
    assert isinstance(arr, pa.Date64Array)
    return arr


def timestamp_array() -> pa.Array:
    arr = pa.TimestampArray.from_pandas(
        [datetime.now(), datetime.now(), datetime.now()],
        type=pa.timestamp("s", tz="America/New_York"),
    )

    assert isinstance(arr, pa.TimestampArray)
    assert arr.type.unit == "s"
    assert arr.type.tz == "America/New_York"
    return arr


def nullable_int() -> pa.Array:
    # True means null
    mask = [True, False, True]
    arr = pa.array([1, 2, 3], type=pa.uint8(), mask=mask)
    assert isinstance(arr, pa.UInt8Array)
    assert not arr[0].is_valid
    return arr


class MyExtensionType(pa.ExtensionType):
    """
    Refer to https://arrow.apache.org/docs/python/extending_types.html for
    implementation details
    """

    def __init__(self):
        pa.ExtensionType.__init__(self, pa.uint8(), "extension_name")

    def __arrow_ext_serialize__(self):
        # since we don't have a parameterized type, we don't need extra
        # metadata to be deserialized
        return b"extension_metadata"

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        # return an instance of this subclass given the serialized
        # metadata.
        return MyExtensionType()


pa.register_extension_type(MyExtensionType())


def extension_array() -> pa.Array:
    arr = pa.array([1, 2, 3], type=MyExtensionType())
    assert isinstance(arr, pa.ExtensionArray)
    return arr


def table() -> pa.Table:
    return pa.table(
        {
            "fixedsizelist": fixed_size_list_array(),
            "struct": struct_array(),
            "binary": binary_array(),
            "string": string_array(),
            "boolean": boolean_array(),
            "null": null_array(),
            "list": list_array(),
            "extension": extension_array(),
            "decimal128": decimal128_array(),
            "date32": date32_array(),
            "date64": date64_array(),
            "timestamp": timestamp_array(),
            "nullable_int": nullable_int(),
        }
    )


def large_table() -> pa.Table:
    # Important: the order of these columns cannot change
    return pa.table(
        {
            "large_binary": large_binary_array(),
            "large_string": large_string_array(),
            "large_list": large_list_array(),
        }
    )


def main():
    feather.write_feather(table(), "table.arrow", compression="uncompressed")
    feather.write_feather(
        large_table(), "large_table.arrow", compression="uncompressed"
    )


if __name__ == "__main__":
    main()
