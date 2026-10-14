package main

import (
	"testing"

	"github.com/rclone/rclone/fs"
)

func TestAzureBlobBackendRegistered(t *testing.T) {
	backend, err := fs.Find("azureblob")
	if err != nil {
		t.Fatalf("Azure Blob backend is not registered: %v", err)
	}
	if backend.Name != "azureblob" {
		t.Errorf("backend name = %q, want %q", backend.Name, "azureblob")
	}
}
