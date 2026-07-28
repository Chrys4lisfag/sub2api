package geminicli

import (
	"strings"
	"testing"
)

func TestDriveStorageInfo(t *testing.T) {
	// 测试 DriveStorageInfo 结构体
	info := &DriveStorageInfo{
		Limit: 100 * 1024 * 1024 * 1024, // 100GB
		Usage: 50 * 1024 * 1024 * 1024,  // 50GB
	}

	if info.Limit != 100*1024*1024*1024 {
		t.Errorf("Expected limit 100GB, got %d", info.Limit)
	}
	if info.Usage != 50*1024*1024*1024 {
		t.Errorf("Expected usage 50GB, got %d", info.Usage)
	}
}

func TestDecodeDriveStorageQuota(t *testing.T) {
	tests := []struct {
		name          string
		body          string
		wantLimit     int64
		wantUsage     int64
		wantUnlimited bool
		wantErr       bool
	}{
		{name: "quoted integers", body: `{"storageQuota":{"limit":"100","usage":"25"}}`, wantLimit: 100, wantUsage: 25},
		{name: "numeric integers", body: `{"storageQuota":{"limit":100,"usage":25}}`, wantLimit: 100, wantUsage: 25},
		{name: "unlimited omits limit", body: `{"storageQuota":{"usage":"25"}}`, wantUsage: 25, wantUnlimited: true},
		{name: "missing quota", body: `{}`, wantErr: true},
		{name: "missing field", body: `{"storageQuota":{"limit":"100"}}`, wantErr: true},
		{name: "malformed string", body: `{"storageQuota":{"limit":"many","usage":"25"}}`, wantErr: true},
		{name: "decimal number", body: `{"storageQuota":{"limit":100.5,"usage":25}}`, wantErr: true},
		{name: "negative value", body: `{"storageQuota":{"limit":"-1","usage":"25"}}`, wantErr: true},
		{name: "overflow", body: `{"storageQuota":{"limit":"9223372036854775808","usage":"25"}}`, wantErr: true},
		{name: "null value", body: `{"storageQuota":{"limit":null,"usage":25}}`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decodeDriveStorageQuota(strings.NewReader(tt.body))
			if tt.wantErr {
				if err == nil {
					t.Fatalf("decodeDriveStorageQuota() error = nil, want error; got %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("decodeDriveStorageQuota() error = %v", err)
			}
			if got.Limit != tt.wantLimit || got.Usage != tt.wantUsage || got.Unlimited != tt.wantUnlimited {
				t.Fatalf("decodeDriveStorageQuota() = %+v, want limit=%d usage=%d unlimited=%v", got, tt.wantLimit, tt.wantUsage, tt.wantUnlimited)
			}
		})
	}
}
