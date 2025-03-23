package fixedkv

import (
	"bytes"
	"os"
	"testing"
)

func TestFixedKV_Set(t *testing.T) {
	dbPath := "test_set.db"
	// Clean up any existing test database
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	kv, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer kv.Close()

	tests := []struct {
		name         string
		key          string
		value        []byte
		wantPrev     []byte
		wantReplaced bool
		wantErr      bool
	}{
		{
			name:         "New key",
			key:          "key1",
			value:        []byte("value1"),
			wantPrev:     nil,
			wantReplaced: false,
			wantErr:      false,
		},
		{
			name:         "Replace existing key",
			key:          "key1",
			value:        []byte("new_value1"),
			wantPrev:     []byte("value1"),
			wantReplaced: true,
			wantErr:      false,
		},
		{
			name:         "Empty value",
			key:          "empty",
			value:        []byte{},
			wantPrev:     nil,
			wantReplaced: false,
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prev, replaced, err := kv.Set(tt.key, tt.value)

			if (err != nil) != tt.wantErr {
				t.Errorf("Set() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantReplaced {
				if !replaced {
					t.Errorf("Set() got replaced = %v, want %v", replaced, tt.wantReplaced)
				}
				if !bytes.Equal(prev, tt.wantPrev) {
					t.Errorf("Set() got prev = %v, want %v", prev, tt.wantPrev)
				}
			} else {
				if replaced {
					t.Errorf("Set() got replaced = %v, want %v", replaced, tt.wantReplaced)
				}
			}

			// Verify the value was set correctly
			got, ok := kv.Get(tt.key)
			if !ok {
				t.Errorf("Key %s not found after Set()", tt.key)
			}
			if !bytes.Equal(got, tt.value) {
				t.Errorf("Get() after Set() got = %v, want %v", got, tt.value)
			}
		})
	}
}

func TestFixedKV_Get(t *testing.T) {
	dbPath := "test_get.db"
	// Clean up any existing test database
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	kv, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer kv.Close()

	// Setup test data
	testData := map[string][]byte{
		"key1":   []byte("value1"),
		"key2":   []byte("value2"),
		"empty":  {},
		"binary": {0x00, 0x01, 0x02, 0x03},
	}

	for k, v := range testData {
		_, _, err := kv.Set(k, v)
		if err != nil {
			t.Fatalf("Failed to set up test data: %v", err)
		}
	}

	tests := []struct {
		name   string
		key    string
		want   []byte
		wantOk bool
	}{
		{
			name:   "Existing key",
			key:    "key1",
			want:   []byte("value1"),
			wantOk: true,
		},
		{
			name:   "Another existing key",
			key:    "key2",
			want:   []byte("value2"),
			wantOk: true,
		},
		{
			name:   "Empty value",
			key:    "empty",
			want:   []byte{},
			wantOk: true,
		},
		{
			name:   "Binary data",
			key:    "binary",
			want:   []byte{0x00, 0x01, 0x02, 0x03},
			wantOk: true,
		},
		{
			name:   "Non-existent key",
			key:    "nonexistent",
			want:   nil,
			wantOk: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := kv.Get(tt.key)

			if ok != tt.wantOk {
				t.Errorf("Get() ok = %v, wantOk %v", ok, tt.wantOk)
				return
			}

			if tt.wantOk && !bytes.Equal(got, tt.want) {
				t.Errorf("Get() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFixedKV_Save(t *testing.T) {
	dbPath := "test_save.db"
	// Clean up any existing test database
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	// Create and populate the database
	kv, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	testData := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
		"key3": []byte("value3"),
	}

	for k, v := range testData {
		_, _, err := kv.Set(k, v)
		if err != nil {
			t.Fatalf("Failed to set up test data: %v", err)
		}
	}

	// Save to disk
	if err := kv.Save(); err != nil {
		t.Fatalf("Failed to save database: %v", err)
	}

	if err := kv.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Reopen the database and verify the data
	reopened, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer reopened.Close()

	// The database should be in readonly mode since it exists
	if !reopened.readonly {
		t.Errorf("Reopened database is not in readonly mode")
	}

	// Check that all the keys were saved
	for k, expectedValue := range testData {
		gotValue, ok := reopened.Get(k)
		if !ok {
			t.Errorf("Key %s not found after reopen", k)
			continue
		}

		if !bytes.Equal(gotValue, expectedValue) {
			t.Errorf("For key %s, got value = %v, want %v", k, gotValue, expectedValue)
		}
	}
}

func TestFixedKV_ReadonlyMode(t *testing.T) {
	dbPath := "test_readonly.db"
	// Clean up any existing test database
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	// Create and populate the database
	kv, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Set some initial data
	_, _, err = kv.Set("key1", []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to set initial data: %v", err)
	}

	// Save to disk
	if err := kv.Save(); err != nil {
		t.Fatalf("Failed to save database: %v", err)
	}

	if err := kv.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Reopen the database - it should be in readonly mode
	reopened, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer reopened.Close()

	// Verify it's in readonly mode
	if !reopened.readonly {
		t.Errorf("Reopened database is not in readonly mode")
	}

	// Try to set a value, should fail with ErrReadonlyDb
	_, _, err = reopened.Set("key2", []byte("value2"))
	if err != ErrReadonlyDb {
		t.Errorf("Set() error = %v, want %v", err, ErrReadonlyDb)
	}

	// We should still be able to read existing data
	value, ok := reopened.Get("key1")
	if !ok || !bytes.Equal(value, []byte("value1")) {
		t.Errorf("Get() got = %v, want %v", value, []byte("value1"))
	}
}

func TestFixedKV_CloseAndReopen(t *testing.T) {
	dbPath := "test_close_reopen.db"
	// Clean up any existing test database
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	// Create and populate the database
	kv, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Set some data
	_, _, err = kv.Set("key1", []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to set data: %v", err)
	}

	// Save and close
	if err := kv.Save(); err != nil {
		t.Fatalf("Failed to save database: %v", err)
	}

	if err := kv.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// TODO: The current implementation doesn't check for closed status in Get/Set
	// This test may need to be updated if that functionality is added

	// Reopen the database
	reopened, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer reopened.Close()

	// Verify we can read the data
	value, ok := reopened.Get("key1")
	if !ok || !bytes.Equal(value, []byte("value1")) {
		t.Errorf("Get() after reopen got = %v, want %v", value, []byte("value1"))
	}
}

func TestFixedKV_Version(t *testing.T) {
	dbPath := "test_version.db"
	// Clean up any existing test database
	os.Remove(dbPath)
	defer os.Remove(dbPath)

	kv, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer kv.Close()

	// Check the version string
	expectedVersion := "v1" // Based on fileFormatVersion = 1
	version := kv.Version()

	if version != expectedVersion {
		t.Errorf("Version() = %v, want %v", version, expectedVersion)
	}
}
