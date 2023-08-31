type ArrowString = string | Uint8Array;

export enum ArrowType {
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

/**
 * A type interface for a JavaScript representation of an `ArrowSchema` struct in the C Data
 * interface.
 *
 * The ArrowSchema structure describes the type and metadata of an exported array or record batch.
 */
export interface ArrowSchema {
  /**
   * A string describing the data type. If the data type is nested, child types are not encoded here
   * but in the `children` structures.
   */
  format: string;
  name: ArrowString | null;
  metadata: ArrowString | Map<string, string> | null;
  flags: number | bigint;
  children: ArrowSchema[];
  dictionary: ArrowSchema | null;
}

/**
 * Get the byte length of an array with the given type.
 *
 * @param   {number}     length  [length description]
 * @param   {ArrowType}  type    [type description]
 *
 * @return  {number}             [return description]
 */
export function getByteLength(length: number, type: ArrowType): number {

}


export function getValidityByteLength(length: number): number {

}
