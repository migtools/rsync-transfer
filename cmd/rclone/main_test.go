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

func TestGoogleCloudStorageBackendRegistered(t *testing.T) {
	backend, err := fs.Find("googlecloudstorage")
	if err != nil {
		t.Fatalf("Google Cloud Storage backend is not registered: %v", err)
	}
	if backend.Name != "google cloud storage" {
		t.Errorf("backend name = %q, want %q", backend.Name, "google cloud storage")
	}
	if backend.Prefix != "gcs" {
		t.Errorf("backend prefix = %q, want %q", backend.Prefix, "gcs")
	}
}
