# The BabyDB ~~Persistence Protocol~~ Commit Pipeline

## Performing a write

* Acquire the lock,
* append the write to the WAL,
* sync the WAL,
* add the write to the memtable,
* release the lock,
* update the visible seqnum.

# The BabyDB Persistence Protocol V1

BabyDB guarantees linearizability and durability of all writes.  Imprecisely, this means, once a call to a mutation method (`insert`, `delete`) has returned, or the data written by it has been observed, that data will be observable until it has been overwritten, even in the presence of a crash.

The way BabyDB services reads is via a `Layout`.  A `Layout` describes the set of sources consulted when a read is executed, and consists of a set of memtables and SSTs, whose results are logically unioned for any read.

While the `Layout` describes the set of data sources that should be used for in-memory reads, the `DiskLayout` is what is used for recovery. The `DiskLayout` consists of a set of logs and a set of SSTs.

There are the following durability-relevant operations:

* serving a read,
* applying a write,
* flushing a memtable,
* swapping out an SST, and finally
* recovery.

## 1. Serving a Read

a. Acquire the lock.  
b. The internal seqnum is bumped.  
c. Release the lock.
d. The current `Layout` is copied and used to mint a new iterator, assigned the internal seqnum.  

## 2. Applying a Write

a. Acquire the lock.  
b. The internal seqnum is bumped and assigned to the incoming command.  
c. [*] That command is written to the current log.  
d. That command is applied to the active memtable.  
e. Release the lock.  
f. Acknowledge the write.

<!-- Note that if we wrote to the memtable before writing to the current log, we might serve the read before it is durable (even though we won't have acked it). -->

## 3. Flushing a Memtable 

Once a memtable gets too large, we would like to flush it to disk.  This means replacing the in-memory memtable with a fresh, empty one, and moving the current memtable's data to an SST.

a. Acquire the lock.  
b. The current memtable (`m`) is placed into read-only mode and is replaced with a new, writable memtable `m'`. The current memtable is still consulted for any reads, in addition to the new writable one.  
c. The current log `l` is frozen and is replaced and is replaced with a new, empty log `l'` in the current `Layout`.  
d. [\*] The current `DiskLayout` is updated and written to include `l'` as a log.  
e. Release the lock.  
f. `m` is scanned and an SST `s` is constructed which contains the same data as it.  
g. [\*] The SST is written to disk.  
h. The current `Layout` is updated to remove `m` and add `s`.  
i. [\*] The current `DiskLayout` is updated to remove `l` and add `s`.  


## 4. Replacing an SST

When we compact multiple SSTables into a single SSTable (either via an L0 flush or otherwise), we must atomically swap in the new SST which contains the data of the previous ones.

This is pretty straightforward for consistency because the SSTs are not actively changing any more,
so we can just replace a layout like:

```
Layout {
    memtables: [...],
    ssts: [[s1, ... sn, ]...]
}
```
with
```
Layout {
    memtables: [...],
    ssts: [s, ...]
}
```

Where `s` is the union of the `si`s. I don't _think_ we need to be super careful here.

## 5. Recovery

a. Find the most recent `DiskLayout` written at some known location.  
b. This gives us a set of logs and a set of SSTs `S`.  
c. Construct a new empty memtable `m`.  
d. Iterate over each log in order (the `DiskLayout` orders them, but also you can determine the order by the sequence numbers they carry), being careful to ignore any partially written results at the end, and apply each command written to `m`.  
e. Construct a `Layout` consisting of `S` and `m`, and the `DiskLayout` being the same as on disk.  


Operations tagged with a [*] are disk operations.

# Correctness

A write is _observed_ if either some reader has seen it, or it was acknowledged to the writer.

A write is _durable_ if it is written in either a log or SST which is referenced by the written `DiskLayout`, or if it has been overwritten.

Claim 1:

> A log always contains at least as much data as its corresponding memtable.

Pf: True because 2c happens before 2d. This requires synchronization and trust in `fsync`.

Claim 2:
> A write which is durable will never become not durable.

Pf:
There are two places where data is removed from the `DiskLayout`: 4 and 3i.

In 4, we atomically add a new SST which includes the data from the unlinked SST.

In 3i, we atomically add a new SST which includes the data from the removed log. Since this SST was constructed by scanning the relevant memtable, the only danger zone here is where the log contains strictly more data than the memtable when we scanned it, which can only happen between 2c and 2d, however, we acquire the lock between 2c and 2d, and again to place the memtable into read-only mode, so we can never be in the danger-zone while performing this operation.

Claim 3:
> Every observed write is durable.

If the write was observed via acknowledgement, in 2f, then 2c has run and the write is durable.

If the write was observed via a read, then it must have been written to a memtable at some point. This write could have occurred in either 2d or 5d. If it was written in 2d, then it was preceded by 2c and is thus durable. If it was written in 5d, then it was read from the log and thus was written to the log at some point and thus is durable.

Claim 4:
> A reader at seqnum `t` will observe every write bearing `t' < t` that will ever exist.

A reader is parameterized with a snapshot of the memtable and a set of SSTs. It suffices to show that:
1. This snapshot contains all `t' < t` writes that have occurred thus far, and
2. after this snapshot is minted, no more writes bearing `t' < t` can occur.

(1) follows from the fact that every time we bump the seqnum, we acquire the lock, so it's impossible to get a new, higher seqnum in 1b until the previous seqnum assigned in 2b has been written in 2d.
(2) follows from the fact that when we apply a write, in 2d, we bump the seqnum, so any writes occurring later on will bear a later seqnum.

# Misc

| Data Structure | Random Reads | Random Writes | Durable |
| -------------- | ------------ | ------------- | ------- |
| Log            | ❌           | ✅             | ✅      |
| SST            | ✅           | ❌             | ✅      |
| Memtable       | ✅           | ✅             | ❌      |
