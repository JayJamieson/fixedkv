package fixedkv

import (
	"bytes"
	"encoding/binary"
	"os"

	"github.com/tidwall/btree"
)

type KVReader struct {
	fp       *os.File
	buff     []byte
	keyCount uint16
	index    *btree.Map[string, uint16]
}

// Open a Fixed KV database for reading
func Open(name string) (*KVReader, error) {
	fp, err := os.OpenFile(name, os.O_RDWR, 0600)

	if err != nil {
		return nil, err
	}

	buff := make([]byte, MaxSize)

	// TODO: probably bounds check amount read
	_, err = fp.Read(buff[:MaxSize])

	if err != nil {
		return nil, err
	}

	if !validateHeader(buff[:HeaderSize]) {
		return nil, errInvalidHeader
	}

	keyCount := binary.LittleEndian.Uint16(buff[KeyCountOffset:])

	reader := &KVReader{
		fp:       fp,
		buff:     buff,
		keyCount: keyCount,
		index:    btree.NewMap[string, uint16](DefaultDegree),
	}

	idx := int(HeaderSize + (keyCount-1)*2)

	for i := HeaderSize; i <= idx; i += 2 {
		offset := binary.LittleEndian.Uint16(reader.buff[i:])
		klen := binary.LittleEndian.Uint16(reader.buff[offset:])

		key := string(reader.buff[offset+4 : offset+4+klen])
		reader.index.Load(key, offset)
	}

	return reader, nil
}

// Get an item
func (r *KVReader) Get(key string) ([]byte, bool) {
	offset, ok := r.index.Get(key)

	if !ok {
		return nil, ok
	}

	keyLen := binary.LittleEndian.Uint16(r.buff[offset:])
	valLen := binary.LittleEndian.Uint16(r.buff[offset+2:])

	start := offset + 4 + keyLen
	end := offset + 4 + keyLen + valLen

	return r.buff[start:end], true
}

// Get all keys in the Fixed KV database
func (r *KVReader) Keys() []string {
	return r.index.Keys()
}

// Get a copy of all values in the Fixed KV database
func (r *KVReader) Values() [][]byte {
	offsets := r.index.Values()
	values := make([][]byte, 0, len(offsets))

	for _, offset := range offsets {
		keyLen := binary.LittleEndian.Uint16(r.buff[offset:])
		valLen := binary.LittleEndian.Uint16(r.buff[offset+2:])

		start := offset + 4 + keyLen
		end := offset + 4 + keyLen + valLen
		data := bytes.Clone(r.buff[start:end])
		values = append(values, data)
	}

	return values
}

func validateHeader(header []byte) bool {
	if len(header) != HeaderSize {
		return false
	}

	if string(header[DBNameOffset:DBNameOffset+len(DBName)]) != DBName {
		return false
	}

	if binary.LittleEndian.Uint32(header) != encodeVersion(Major, Minor, Patch) {
		return false
	}

	return true
}
