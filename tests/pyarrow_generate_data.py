import pyarrow as pa
import pyarrow.feather as feather


def fixed_size_list():
    coords = pa.array([1, 2, 3, 4, 5, 6], type=pa.uint8())
    parr = pa.FixedSizeListArray.from_arrays(coords, 2)
    data = {"fixedsizelist": parr}
    table = pa.table(data)
    feather.write_feather(table, "fixed_size_list.arrow", compression="uncompressed")


def main():
    fixed_size_list()


if __name__ == "__main__":
    main()
