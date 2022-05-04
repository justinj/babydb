pub mod reader;
pub mod writer;

// This package provides facilities to both read and write Sorted-String Tables
// (SSTs).
//
// An SST is an indexed, immutable, durable data structure.
// SSTs are constructed either by flushing a memtable to disk, or by merging two
// or more existing SSTs.
//
// Logically, an SST contains some set of key-value pairs, ordered on keys, and
// support fast iteration and point reads.
//
// Physically, an SST is stored as a sequence of _blocks_. At the beginning of
// each block, the first key-value pair is written. Subsequent keys represented
// by a pair (usize, [u8]), where the first coordinate denotes the length of the
// shared prefix of this key with the previous key. This particular kind of
// compression is important to make very long keys (which might be needed in a
// hierarchical scheme) are low-cost.
//
// At the end of an SST, the _index block_ is written, which is another sequence
// of key-value pairs, where the keys are the first key in each block, and the
// values are the offset of that block from the start of the file.
//
// Finally, after all the data blocks and the index block, metadata about the SST is written.
// At time of writiing, that metadata is:
// * the minimum key in the block,
// * the maximum key in the block,
// * the length of the index block, and
// * the length of all of the data blocks together.
