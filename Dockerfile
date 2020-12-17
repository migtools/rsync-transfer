FROM registry.access.redhat.com/ubi8/ubi-minimal:latest
RUN echo -ne "[centos-8-baseos]\nname = CentOS 8 (RPMs) - BaseOS\nbaseurl = http://mirror.centos.org/centos-8/8/BaseOS/x86_64/os/\nenabled = 1\ngpgcheck = 0" > /etc/yum.repos.d/centos.repo
RUN microdnf -y install openssh-server stunnel rsync && microdnf clean all
COPY sshd_config /etc/ssh/sshd_config
COPY stunnel.conf /etc/stunnel/stunnel.conf
