export interface ArrowArray {
  /**
   * The logical length of the array (i.e. its number of items).
   */
  length: number;

  /**
   * The number of null items in the array. MAY be -1 if not yet computed.
   */
  nullCount: number;

  /**
   * The logical offset inside the array (i.e. the number of items from the physical start of the
   * buffers). MUST be 0 or positive.
   */
  offset: number;

  /**
   * An array of buffers backing this array. There must be `n_buffers` objects.
   *
   * The producer MUST ensure that each contiguous buffer is large enough to represent length +
   * offset values encoded according to the Columnar format specification.
   *
   * The buffers may be `null` instead of a `Uint8Array` only in two situations:
   *
   * 1. for the null bitmap buffer, if `null_count` is 0;
   * 2. for any buffer, if the size in bytes of the corresponding buffer would be 0.
   *
   * Buffers of children arrays are not included.
   *
   * A Uint8Array is stored instead of ArrayBuffers so that this `ArrowArray` may be a view on
   * external WebAssembly memory.
   */
  buffers: (Uint8Array | null)[];

  /**
   * An array of `ArrowArray` objects representing the children arrays of this array. There must be
   * `n_children` pointers.
   */
  children: ArrowArray[];

  /**
   * An `ArrowArray` with the underlying array of dictionary values.
   *
   * MUST be present if the ArrowArray represents a dictionary-encoded array. MUST be `null`
   * otherwise.
   */
  dictionary: ArrowArray | null;
}
