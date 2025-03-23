package fixedkv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/tidwall/btree"
)

var ErrInvalidHeader = errors.New("invalid header format")
var ErrReadonlyDb = errors.New("readonly db")
var ErrDatabaseClosed = errors.New("database closed")

const KeyCountOffset = 4
const DBNameOffset = 6
const HeaderSize = 96
const DefaultSize = 4096
const DefaultDegree = 32
const DBName = "FixedKV database"

const fileFormatVersion = 1

type FixedKV struct {
	fp       *os.File
	index    *btree.Map[string, []byte]
	mu       sync.RWMutex
	readonly bool // false for new fixedkv, true opening existing kv
	closed   bool // true if the database is closed
}

// Creates a new FixedKV database file, will write and flush header data to disk.
func Open(name string) (*FixedKV, error) {
	fp, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0600)

	if err != nil {
		return nil, err
	}

	buff := make([]byte, HeaderSize)

	// detect if new or existing db file
	read, err := fp.Read(buff[:HeaderSize])

	if err != nil && err != io.EOF {
		fp.Close()
		return nil, err
	}

	kv := &FixedKV{
		fp:    fp,
		index: btree.NewMap[string, []byte](DefaultDegree),
	}

	// TODO: maybe need to do something when fsync fails
	err = fp.Sync()
	if err != nil {
		return nil, err
	}

	// if header is found, this is existing fixedkv file and can only be read
	if read > 0 {
		kv.readonly = true
		return kv, nil
	}

	return kv, nil
}

func writeHeader(buff []byte, keyCount uint16) {
	binary.LittleEndian.PutUint32(buff, fileFormatVersion)
	binary.LittleEndian.PutUint16(buff[KeyCountOffset:], keyCount)
	copy(buff[DBNameOffset:], []byte(DBName))
}

func (kv *FixedKV) Get(key string) ([]byte, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if v, ok := kv.index.Get(key); ok {
		return v, true
	}

	// set starting point to of key-value pairs
	kv.fp.Seek(1, HeaderSize)

	var keyLen uint16
	var valLen uint16
	for {
		if err := binary.Read(kv.fp, binary.LittleEndian, &keyLen); err != nil {
			if err == io.EOF {
				break
			}
			return nil, false
		}

		if err := binary.Read(kv.fp, binary.LittleEndian, &valLen); err != nil {
			return nil, false
		}

		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(kv.fp, keyBytes); err != nil {
			return nil, false
		}

		valBytes := make([]byte, valLen)
		if _, err := io.ReadFull(kv.fp, valBytes); err != nil {
			return nil, false
		}

		if string(keyBytes) == key {
			// cache result in index for future lookups
			kv.index.Set(key, valBytes)
			return valBytes, true
		}
	}

	return nil, false
}

// Set inserts a new key-value pair. If file is readonly then set will
// error with ErrReadonlyDb. If setting a non existen key then nil value, false
// for replaced value and nil error.
// If value is replaced then previous value, true and no error is returned.
func (kv *FixedKV) Set(key string, value []byte) ([]byte, bool, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.readonly {
		return nil, false, ErrReadonlyDb
	}

	// if replaced is true then previous value is returned otherwise nil
	previousOrCurrent, replaced := kv.index.Set(key, value)

	return previousOrCurrent, replaced, nil
}

// Flushes In-memory KV Database to disk in ascending key order
// Must be called or dataloss will occur
func (kv *FixedKV) Save() error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.readonly {
		return ErrReadonlyDb
	}

	var buf []byte = make([]byte, DefaultSize)

	// write out number of keys to header
	numKeys := uint16(kv.index.Len())
	writeOffset := HeaderSize

	writeHeader(buf, numKeys)

	kv.index.Ascend("", func(key string, value []byte) bool {

		kLen := len([]byte(key))
		vLen := len(value)

		binary.LittleEndian.PutUint16(buf[writeOffset:], uint16(kLen))
		binary.LittleEndian.PutUint16(buf[writeOffset+2:], uint16(vLen))

		copy(buf[writeOffset+4:], []byte(key))
		copy(buf[uint16(writeOffset)+4+uint16(kLen):], value)

		// move write pointer along by key and value length
		writeOffset += 4 + kLen + vLen

		return true
	})

	_, err := kv.fp.WriteAt(buf[:writeOffset], io.SeekStart)

	if err != nil {
		return err
	}

	err = kv.fp.Sync()

	if err != nil {
		return err
	}

	return nil
}

func (kv *FixedKV) Close() error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.closed {
		return ErrDatabaseClosed
	}

	kv.closed = true
	kv.fp.Sync()

	return kv.fp.Close()
}

func (kv *FixedKV) Version() string {
	return fmt.Sprintf("v%d", fileFormatVersion)
}
