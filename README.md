# fixed-kv

fixed-kv is a in memory disk persisted key value storage engine of fixed size (4KB), with a dead simple binary format.

A new `fixedkv` instance will use in memory btree for `Get` and `Set` operations. Closing the `fixedkv` will write to disk where.

Data can be read using the `fixedkv.Open()`. This will load the database in read only mode, to allow reading of keys and values again.

To edit a database, create a new database using `fixedkv.New()` and manually insert from disked backed copy.

## Format v1

First 96 bytes of the file is used for storing the header.

```text
<----------Header---------->
| version | #keys | unused | key-values |
| 4B      | 2B    | 90B    | ...        |
```

- File format version
- Number of keys
- Unused bytes for future use
- The stored key/value pairs of variable length key and values

### Key Value format v1

```text
| key len | value len | key | val |
|   2B    |    2B     | ... | ... |
```

## TODO

- [ ] bounds checking for insert operations to keep within 4KB size
