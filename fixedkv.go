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

// Version numbers
const (
	Major = 0
	Minor = 1
	Patch = 0
)

type FixedKV struct {
	fp       *os.File
	buff     []byte
	keyCount uint16
	index    *btree.Map[string, []byte]
	mu       sync.RWMutex
}

// Creates a new FixedKV database file, will write and flush header data to disk.
func New(name string, degree int) (*FixedKV, error) {
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
		index: btree.NewMap[string, []byte](degree),
	}

	if read > 0 {
		return kv, nil
	}

	version := encodeVersion(Major, Minor, Patch)

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
	return kv.index.Get(key)
}

func (kv *FixedKV) Set(key string, value []byte) bool {
	kv.mu.Lock()
	kv.keyCount++

	binary.LittleEndian.PutUint16(kv.buff[KeyCountOffset:], kv.keyCount)
	kv.mu.Unlock()

	_, result := kv.index.Set(key, value)

	return result
}

// Flushes In-memory KV Database to disk and closes file handle
// Must be called or dataloss will occur
func (kv *FixedKV) Close() error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	idx := 0
	kvOffset := HeaderSize + 2*(kv.keyCount-1) + 2

	prev := kvOffset
	next := uint16(0)

	kv.index.Ascend("", func(key string, value []byte) bool {
		idx++
		keyOffset := HeaderSize + 2*(idx-1)
		kLen := len([]byte(key))
		vLen := len(value)

		dataLen := 4 + uint16(kLen+vLen)
		next = prev
		prev = next + dataLen

		binary.LittleEndian.PutUint16(kv.buff[keyOffset:], next)
		binary.LittleEndian.PutUint16(kv.buff[next:], uint16(kLen))
		binary.LittleEndian.PutUint16(kv.buff[next+2:], uint16(vLen))

		copy(kv.buff[next+4:], []byte(key))
		copy(kv.buff[next+4+uint16(kLen):], value)

		return true
	})

	_, err := kv.fp.WriteAt(kv.buff, io.SeekStart)

	if err != nil {
		return err
	}

	err = kv.fp.Sync()

	if err != nil {
		return err
	}

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
