# btree

A B+tree implementation with the following properties:
- persistent (stored on disk, using the `buffer_pool` crate)
- concurrent (each node fits on a page and uses its RW-lock for access control)
- maps arbitrary byte sequences to arbitrary byte sequences
- no duplicate support

Some simplifying assumptions for now:
- keys or values larger than a page not supported
- no prefix compression nor suffix truncation supported

For our usage we don't need to store duplicates, because:
- for row storage (by primary key), the keys will be unique
- for other indexes we will store the record id as part of the key (value will be empty)

## Data layout

There are two kinds of nodes: internal and leaf nodes.
Internal nodes store _n_ keys and _n + 1_ child pointers (PageIds).
Since keys are variable-length, they are stored similarly to a heap page. So the layout is:

```
-----------------------------------------
| num_keys (2) | free_space_pointer (2) |
---------------------------------------------------------------------------
| child_page 0 (4) | key 0 offset (2) | key 0 size (2) | child_page 1 (4) |
---------------------------------------------------------------------------
| ... | child_page (num_keys)+1 (4) | free space | key data |
-------------------------------------------------------------
                                                 ^ free space pointer
```

Leaf nodes store keys and values, but no child pointers.
For each entry, the value is stored just after the key (and both key and value size is stored in the header).

```
-----------------------------------------
| num_keys (2) | free_space_pointer (2) |
---------------------------------------------------------------------------
| key 0 offset (2) | key 0 size (2) | value 0 size (2) |
-----------------------------------------------------------------------------------------
| ... | key (num_keys) offset (2) | key (num_keys) size (2) | value (num_keys) size (2) |
-----------------------------------------------------------------------------------------
| free space | key and value data |
-----------------------------------
^ free space pointer
```
