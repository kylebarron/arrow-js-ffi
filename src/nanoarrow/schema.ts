type ArrowString = string | Uint8Array;

/** Types with a static C Data Interface format string */
export enum ArrowStaticType {
  Null = "n",
  Bool = "b",
  Int8 = "c",
  Uint8 = "C",
  Int16 = "s",
  Uint16 = "S",
  Int32 = "i",
  Uint32 = "I",
  Int64 = "l",
  Uint64 = "L",
  Float16 = "e",
  Float32 = "f",
  Float64 = "g",
  Binary = "z",
  LargeBinary = "Z",
  String = "u",
  LargeString = "U",
  DateDay = "tdD",
  DateMillisecond = "tdm",
  TimeSecond = "tts",
  TimeMillisecond = "ttm",
  TimeMicrosecond = "ttu",
  TimeNanosecond = "ttn",
  DurationSecond = "tDs",
  DurationMillisecond = "tDm",
  DurationMicrosecond = "tDu",
  DurationNanosecond = "tDn",
  IntervalYearMonth = "tiM",
  IntervalDayTime = "tiD",
  IntervalMonthDayNanosecond = "tin",
  List = "+l",
  LargeList = "+L",
  Struct = "+s",
  Map = "+m",
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
