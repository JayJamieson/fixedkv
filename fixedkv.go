package fixedkv

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	"github.com/tidwall/btree"
)

const KeyOffset = 96
const KeyCountOffset = 32

type FixedKV struct {
	fp    *os.File
	buff  []byte
	index *btree.Map[string, uint64]
	mu    sync.RWMutex
}

// Creates a new FixedKV database file. Will overwrite existing file if already exists
// Use fixedkv.Open() to open existing FixedKV database
func NewFixedKV(name string) (*FixedKV, error) {
	fp, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0600)

	if err != nil {
		return nil, err
	}

	buff := make([]byte, 4096)

	kv := &FixedKV{
		fp:    fp,
		buff:  buff,
		index: btree.NewMap[string, uint64](3),
	}

	version := encodeVersion(0, 0, 1)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	writeHeader(buff, version, 0)

	fp.Write(buff)
	fp.Sync()

	return kv, nil
}

func writeHeader(buff []byte, version uint32, keyCount uint16) {
	binary.LittleEndian.PutUint32(buff[0:KeyCountOffset], version)
	binary.LittleEndian.PutUint16(buff[KeyCountOffset:], keyCount)
}

func (kv *FixedKV) Get(key []byte) ([]byte, bool) {
	return nil, false
}

func (kv *FixedKV) Set(key []byte, value []byte) bool {
	return false
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
