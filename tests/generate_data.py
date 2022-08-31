import pyarrow as pa
import pyarrow.feather as feather

compressions = ["SNAPPY", "GZIP", "BROTLI", "LZ4", "ZSTD", "NONE"]


def create_data():
    data = {
        "str": pa.array(["a", "b", "c", "d"], type=pa.string()),
        "uint8": pa.array([1, 2, 3, 4], type=pa.uint8()),
        "int32": pa.array([0, -2147483638, 2147483637, 1], type=pa.int32()),
        "bool": pa.array([True, True, False, False], type=pa.bool_()),
    }
    return pa.table(data)


def write_data(table):
    feather.write_feather(table, "data.arrow", compression="uncompressed")


def main():
    table = create_data()
    write_data(table)


if __name__ == "__main__":
    main()
