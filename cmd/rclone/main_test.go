package main

import (
	"testing"

	"github.com/rclone/rclone/cmd"
	"github.com/rclone/rclone/fs"
)

// commandRegistered reports whether a subcommand with the given name is
// registered on rclone's root command (subcommands self-register via init()
// when their package is blank-imported in main.go).
func commandRegistered(name string) bool {
	for _, c := range cmd.Root.Commands() {
		if c.Name() == name {
			return true
		}
	}
	return false
}

func TestMkdirCommandRegistered(t *testing.T) {
	if !commandRegistered("mkdir") {
		t.Fatal("mkdir command is not registered; ensure cmd/rclone/main.go blank-imports github.com/rclone/rclone/cmd/mkdir")
	}
}

func TestSyncCommandRegistered(t *testing.T) {
	if !commandRegistered("sync") {
		t.Fatal("sync command is not registered; ensure cmd/rclone/main.go blank-imports github.com/rclone/rclone/cmd/sync")
	}
}

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
