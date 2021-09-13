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

## Operations

### Search

Start from root node.

On each internal node, binary-search in keys. Follow child pointer at the found index (which may be `num_keys` - that's ok because we have one more child pointer than keys).

On leaf node, binary-search by keys. If found exact key, this is it. If not found, we have an insert location.

### Insert

Search for the key. We should find an appropriate leaf node and insert location.

If there's space (before `free_space_ptr`) in the leaf node (for key an additional entry in header):
- copy the key and value into free space
- shift the header entries after the insert location by one
- set the header entry appropriately

Note that only header entries are sorted, not actual keys and values.

If there's no space before `free_space_ptr`, check if the entry would fit after reorganizing - sum up all key and value sizes. If it is sufficient, reorganize the node:
- copy the page into a temporary buffer
- rewrite the page, copying each entry in order (including the new entry at insert location), starting at `PAGE_SIZE-total_entry_size`.

Note: the reorganization means that we need API for writing entries at arbitrary offset.

Third case: if there's no space even after compaction, split the page.
