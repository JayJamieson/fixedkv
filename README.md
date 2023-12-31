# fixed-kv

fixed-kv is a in memory disk persisted key value storage engine of fixed size (4KB), with a dead simple binary format.

A new `fixedkv` instance will use in memory btree for `Get` and `Set` operations. Closing the `fixedkv` will write to disk where.

Data can be restored using the `fixedkv.Reader()`. This will load only header and key/value offsets to allow reading of keys again.

## Format

First 96 bytes of the file is used for storing the header.

```text
<----------Header---------->
| version | #keys | unused |   offsets    | key-values |
| 4B      | 2B    | 90B    |   #keys * 2B | ...        |
```

- File format version
- Number of keys
- Unused bytes for future use
- Offset pointing to key/value pairs in the file
- The stored key/value pairs of variable length key and values

### Key Value format

| key len | value len | key | val |
|   2B    |    2B     | ... | ... |
