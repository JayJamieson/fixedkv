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
func OpenReader(name string) (*KVReader, error) {
	fp, err := os.OpenFile(name, os.O_RDWR, 0600)

	if err != nil {
		return nil, err
	}

	buff := make([]byte, DefaultSize)

	// TODO: maybe need to do something when fsync fails
	err = fp.Sync()
	if err != nil {
		return nil, err
	}

	_, err = fp.Read(buff[:DefaultSize])

	if err != nil {
		return nil, err
	}

	if !validateHeader(buff[:HeaderSize]) {
		return nil, ErrInvalidHeader
	}

	reader := &KVReader{
		fp:       fp,
		buff:     buff,
		keyCount: binary.LittleEndian.Uint16(buff[KeyCountOffset:]),
		index:    btree.NewMap[string, uint16](DefaultDegree),
	}

	loadIndex(reader)

	return reader, nil
}

func loadIndex(reader *KVReader) {
	keyOffset := uint16(HeaderSize)
	for i := uint16(0); i <= reader.keyCount; i += 1 {
		klen := binary.LittleEndian.Uint16(reader.buff[keyOffset:])
		vlen := binary.LittleEndian.Uint16(reader.buff[keyOffset+2:])

		key := string(reader.buff[keyOffset+4 : keyOffset+4+klen])
		reader.index.Load(key, keyOffset)

		keyOffset += 4 + klen + vlen
	}
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
	end := start + valLen

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
	actual := string(header[DBNameOffset : DBNameOffset+len(DBName)])

	return actual == DBName
}
