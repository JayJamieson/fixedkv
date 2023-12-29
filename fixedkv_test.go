package fixedkv

import (
	"testing"
)

func Test_decodeVersion(t *testing.T) {
	type args struct {
		version uint32
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Patch only version",
			args: args{
				version: 65536,
			},
			want: "v0.0.1",
		},
		{
			name: "Minor only version",
			args: args{
				version: 256,
			},
			want: "v0.1.0",
		},
		{
			name: "Major only version",
			args: args{
				version: 1,
			},
			want: "v1.0.0",
		},
		{
			name: "Major, Minor, Patch",
			args: args{
				version: 65793,
			},
			want: "v1.1.1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := decodeVersion(tt.args.version); got != tt.want {
				t.Errorf("decodeVersion() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_encodeVersion(t *testing.T) {
	type args struct {
		major int
		minor int
		patch int
	}
	tests := []struct {
		name string
		args args
		want uint32
	}{
		{
			name: "Patch only version",
			args: args{
				major: 0,
				minor: 0,
				patch: 1,
			},
			want: 65536,
		},
		{
			name: "Minor only version",
			args: args{
				major: 0,
				minor: 1,
				patch: 0,
			},
			want: 256,
		},
		{
			name: "Major only version",
			args: args{
				major: 1,
				minor: 0,
				patch: 0,
			},
			want: 1,
		},
		{
			name: "Major, Minor, Patch",
			args: args{
				major: 1,
				minor: 1,
				patch: 1,
			},
			want: 65793,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := encodeVersion(tt.args.major, tt.args.minor, tt.args.patch); got != tt.want {
				t.Errorf("encodeVersion() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFixedKV_Version(t *testing.T) {

	kv := &FixedKV{
		buff: make([]byte, 4096),
	}

	version := encodeVersion(0, 0, 1)

	writeHeader(kv.buff, version, 0)

	want := "v0.0.1"

	if got := kv.Version(); got != want {
		t.Errorf("FixedKV.Version() = %v, want %v", got, want)
	}
}
