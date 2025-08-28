import { DataType, Field } from "apache-arrow";

// Redefine the arrow Type enum to include LargeList, LargeBinary, and LargeUtf8
export enum Type {
  NONE = 0 /** The default placeholder type */,
  Null = 1 /** A NULL type having no physical storage */,
  Int = 2 /** Signed or unsigned 8, 16, 32, or 64-bit little-endian integer */,
  Float = 3 /** 2, 4, or 8-byte floating point value */,
  Binary = 4 /** Variable-length bytes (no guarantee of UTF8-ness) */,
  Utf8 = 5 /** UTF8 variable-length string as List<Char> */,
  Bool = 6 /** Boolean as 1 bit, LSB bit-packed ordering */,
  Decimal = 7 /** Precision-and-scale-based decimal type. Storage type depends on the parameters. */,
  Date = 8 /** int32_t days or int64_t milliseconds since the UNIX epoch */,
  Time = 9 /** Time as signed 32 or 64-bit integer, representing either seconds, milliseconds, microseconds, or nanoseconds since midnight since midnight */,
  Timestamp = 10 /** Exact timestamp encoded with int64 since UNIX epoch (Default unit millisecond) */,
  Interval = 11 /** YEAR_MONTH or DAY_TIME interval in SQL style */,
  List = 12 /** A list of some logical data type */,
  Struct = 13 /** Struct of logical types */,
  Union = 14 /** Union of logical types */,
  FixedSizeBinary = 15 /** Fixed-size binary. Each value occupies the same number of bytes */,
  FixedSizeList = 16 /** Fixed-size list. Each value occupies the same number of bytes */,
  Map = 17 /** Map of named logical types */,
  Duration = 18 /** Measure of elapsed time in either seconds, milliseconds, microseconds or nanoseconds. */,
  LargeBinary = 19 /** Large variable-length bytes (no guarantee of UTF8-ness) */,
  LargeUtf8 = 20 /** Large variable-length string as List<Char> */,

  // Not yet included in the upstream enum
  LargeList = 30,

  Dictionary = -1 /** Dictionary aka Category type */,
  Int8 = -2,
  Int16 = -3,
  Int32 = -4,
  Int64 = -5,
  Uint8 = -6,
  Uint16 = -7,
  Uint32 = -8,
  Uint64 = -9,
  Float16 = -10,
  Float32 = -11,
  Float64 = -12,
  DateDay = -13,
  DateMillisecond = -14,
  TimestampSecond = -15,
  TimestampMillisecond = -16,
  TimestampMicrosecond = -17,
  TimestampNanosecond = -18,
  TimeSecond = -19,
  TimeMillisecond = -20,
  TimeMicrosecond = -21,
  TimeNanosecond = -22,
  DenseUnion = -23,
  SparseUnion = -24,
  IntervalDayTime = -25,
  IntervalYearMonth = -26,
}

export class LargeList<T extends DataType = any> extends DataType<
  // @ts-expect-error Type 'Type.LargeList' does not satisfy the constraint 'Type'
  Type.LargeList,
  { [0]: T }
> {
  constructor(child: Field<T>) {
    super(Type.LargeList);
    this.children = [child];
  }
  declare public readonly children: Field<T>[];
  public toString() {
    return `LargeList<${this.valueType}>`;
  }
  public get valueType(): T {
    return this.children[0].type as T;
  }
  public get valueField(): Field<T> {
    return this.children[0] as Field<T>;
  }
  public get ArrayType(): T["ArrayType"] {
    return this.valueType.ArrayType;
  }
  protected static [Symbol.toStringTag] = ((proto: LargeList) => {
    (<any>proto).children = null;
    return (proto[Symbol.toStringTag] = "LargeList");
  })(LargeList.prototype);
}

export function isLargeList(x: any): x is LargeList {
  return x?.typeId === Type.LargeList;
}
