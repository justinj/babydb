# Why do we need sequence numbers?

* You need them to filter out new entries in the memtable if you're using a skiplist.
* You need them to specify snapshots.
* If you don't order the L0 SSTs, you need them to determine which keys are newer.
* Probably some complications around range deletions.
* They let you treat hunks of data as order-independent, and give the underlying storage some sort of bag semantics.