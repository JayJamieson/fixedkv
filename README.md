# fixed-kv

fixed-kv is a fixed sized (4KB, 8KB, 16KB) key value storage engine with a dead simple binary format.

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
