FROM brew.registry.redhat.io/rh-osbs/openshift-golang-builder:rhel_8_golang_1.24 AS builder

WORKDIR /workspace/
COPY go.mod go.mod
COPY go.sum go.sum
COPY cmd/ cmd/
COPY pkg/ pkg/

ENV GOEXPERIMENT strictfipsruntime
ENV BUILDTAGS containers_image_ostree_stub exclude_graphdriver_devicemapper exclude_graphdriver_btrfs containers_image_openpgp exclude_graphdriver_overlay strictfipsruntime
RUN GO111MODULE=auto CGO_ENABLED=1 GOOS=linux go build -mod=readonly -v -installsuffix "static" -tags "$BUILDTAGS" -o _output/blockrsync ./cmd/blockrsync/main.go
RUN GO111MODULE=auto CGO_ENABLED=1 GOOS=linux go build -mod=readonly -v -installsuffix "static" -tags "$BUILDTAGS" -o _output/proxy ./cmd/proxy/main.go

FROM registry.redhat.io/ubi8/ubi:latest
RUN dnf -y install openssh-server stunnel rsync nmap && dnf clean all

COPY --from=builder /workspace/_output/blockrsync /blockrsync
COPY --from=builder /workspace/_output/proxy /proxy
COPY sshd_config /etc/ssh/sshd_config
COPY stunnel.conf /etc/stunnel/stunnel.conf
COPY LICENSE /licenses/

USER 65534:65534
WORKDIR /
