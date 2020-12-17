FROM registry.access.redhat.com/ubi8/ubi-minimal:latest
RUN dnf -y install openssh-server stunnel rsync && dnf clean all
COPY sshd_config /etc/ssh/sshd_config
COPY stunnel.conf /etc/stunnel/stunnel.conf
