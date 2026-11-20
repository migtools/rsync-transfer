# rsync-transfer

`rsync-transfer` provides the transfer-side utilities used by
[`github.com/migtools/crane`](https://github.com/migtools/crane), in particular
by its `transfer-pvc` command for moving PersistentVolumeClaim data between
Kubernetes clusters.

The container image includes:

- `rsync`, standard data sync transfer utility;
- `proxy`, which connects transfer endpoints; and
- a minimal `rclone` build with local, S3, and crypt backends.

## Build

Build the container image from the repository root:

```sh
podman build -t rsync-transfer .
```

## Test

Run the Go test suite with:

```sh
go test ./...
```

The binaries are intended to be invoked by `crane transfer-pvc`; this
repository does not provide the PVC migration command-line interface itself.
