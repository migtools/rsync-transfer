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

// TestCommandsRegistered verifies that each subcommand the custom binary relies
// on is compiled in. Every command self-registers via init() when its package is
// blank-imported in main.go, so a missing blank import silently drops the command
// from the binary; this test guards against that. "mkdir" is the command added by
// this change (empty-PVC indirect transfers need it to create the remote root
// marker); "sync" is kept as a cheap regression guard for the pre-existing upload
// path. Add new commands to the list as they are wired up.
func TestCommandsRegistered(t *testing.T) {
	for _, name := range []string{"mkdir", "sync"} {
		t.Run(name, func(t *testing.T) {
			if !commandRegistered(name) {
				t.Fatalf("%s command is not registered; ensure cmd/rclone/main.go blank-imports github.com/rclone/rclone/cmd/%s", name, name)
			}
		})
	}
}

// TestAzureBlobBackendRegistered verifies the Azure Blob backend is compiled in.
func TestAzureBlobBackendRegistered(t *testing.T) {
	backend, err := fs.Find("azureblob")
	if err != nil {
		t.Fatalf("Azure Blob backend is not registered: %v", err)
	}
	if backend.Name != "azureblob" {
		t.Errorf("backend name = %q, want %q", backend.Name, "azureblob")
	}
}

// TestGoogleCloudStorageBackendRegistered verifies the Google Cloud Storage
// backend is compiled in and exposes the expected name and prefix.
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
