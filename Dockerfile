######################################################################
# Establish a common builder image for all golang-based images
FROM docker.io/golang:1.21 as golang-builder
USER root
WORKDIR /workspace
# We don't vendor modules. Enforce that behavior
ENV GOFLAGS=-mod=readonly
ENV GO111MODULE=on
ARG TARGETOS
ARG TARGETARCH
ENV GOOS=${TARGETOS:-linux}
ENV GOARCH=${TARGETARCH}

######################################################################
# Build block binary
FROM golang-builder AS blockrsync-builder

# Copy the Go Modules manifests & download dependencies
COPY go.mod go.mod
COPY go.sum go.sum

# Copy the go source
COPY cmd/ cmd/
COPY ./pkg/. pkg/
RUN go mod download

# Build
RUN go build -o blockrsync ./cmd/blockrsync/main.go
RUN go build -o proxy ./cmd/proxy/main.go

######################################################################
# Final container
FROM registry.access.redhat.com/ubi8/ubi-minimal:latest
RUN microdnf update -y
RUN microdnf -y install openssh-server stunnel rsync nmap && microdnf clean all
COPY sshd_config /etc/ssh/sshd_config
COPY stunnel.conf /etc/stunnel/stunnel.conf
USER 65534:65534

WORKDIR /

##### blockrsync
COPY --from=blockrsync-builder /workspace/blockrsync /blockrsync
COPY --from=blockrsync-builder /workspace/proxy /proxy

