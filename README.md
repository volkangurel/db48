db48 - A database written in 48 hours
======================================

(currently up to 12 hours -- 36 more hours to go!)

Goals
------

- Update-in-place storage that assumes large RAM and SSDs
- Simple code that is easy to verify and debug
- Indices
- Relaxed consistency and no transactions
- Field updates are timestamped to allow reconstruction of latest data values when sharding
- Schema-free table and record format
- Reasonably efficient on disk format

Internal Architecture
----------------------

The Table is the building block for storing records and index values. It provides free-space management and
the ability to look records up a fixed offset.

Multi-server architecture
--------------------------

Like memcached, multiple servers can be used with a consistent hashing scheme though the servers themselves
are not aware of this scheme and do not actively replicate, migrate data, etc.

Additional tools can be used to rebalance / migrate records after consistent hashing changes such as addition of
new nodes.

Replication is implemented by the client or via a proxy between client and servers.

Clients are also responsible for learning about node addition / removals, reading / writing from altnerative nodes
if a primary node has failed etc.

Data structures
---------------

Tables
Regions
FieldLists
Fields

A Table has a header and then a sequence of Regions. A Region has a header and then a secuence of FieldLists. A FieldList
has a sequence of Fields.

The Table header primarily exists to provide a series of "region summaries" which are a hint to how full each region
is. This allows quickly picking a region when looking for free space to write new FieldLists.

A Region header primiarly has a fixed size array of free map entries that record the (offset,length) of all free
space in the region.
