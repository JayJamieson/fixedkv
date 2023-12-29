package fixedkv

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/tidwall/btree"
)

const KeyCountOffset = 4
const DBNameOffeset = 6
const HeaderSize = 96
const MaxSize = 4096

type FixedKV struct {
	fp       *os.File
	buff     []byte
	keyCount uint16
	index    *btree.Map[string, []byte]
	mu       sync.RWMutex
}

// Creates a new FixedKV database file, will write and flush header data to disk.
func NewFixedKV(name string) (*FixedKV, error) {
	fp, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0600)

	if err != nil {
		return nil, err
	}

	buff := make([]byte, MaxSize)

	// detect if new or existing db file
	read, err := fp.Read(buff[:HeaderSize])

	if err != nil && err != io.EOF {
		fp.Close()
		return nil, err
	}

	kv := &FixedKV{
		fp:    fp,
		buff:  buff,
		index: btree.NewMap[string, []byte](3),
	}

	if read > 0 {
		return kv, nil
	}

	version := encodeVersion(0, 0, 33)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	writeHeader(buff, version, kv.keyCount)

	fp.Write(buff)
	fp.Sync()

	return kv, nil
}

func writeHeader(buff []byte, version uint32, keyCount uint16) {
	binary.LittleEndian.PutUint32(buff, version)
	binary.LittleEndian.PutUint16(buff[KeyCountOffset:], keyCount)
	copy(buff[DBNameOffeset:], []byte("4KB FixedKV database"))
}

func (kv *FixedKV) Get(key string) ([]byte, bool) {
	// get key/value offset from index
	// read value length
	// read value upto length

	return nil, false
}

func (kv *FixedKV) Set(key string, value []byte) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.keyCount++

	// increment key count
	binary.LittleEndian.PutUint16(kv.buff[KeyCountOffset:], kv.keyCount)

	_, result := kv.index.Set(key, value)

	return result
}

func (kv *FixedKV) Close() error {
	return kv.fp.Close()
}

func (kv *FixedKV) Version() string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	version := binary.LittleEndian.Uint32(kv.buff)
	return decodeVersion(version)
}

// Encode version number as a 32bit int
func encodeVersion(major int, minor int, patch int) uint32 {
	version := (major & 0xff) | ((minor & 0xff) << 8) | ((patch & 0xff) << 16)
	return uint32(version)
}

// Decode version number as a semver string
func decodeVersion(version uint32) string {
	major := (version & 0xff)
	minor := ((version >> 8) & 0xff)
	patch := ((version >> 16) & 0xff)

	return fmt.Sprintf("v%d.%d.%d", major, minor, patch)
}
