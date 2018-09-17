FROM artifactory.service.bo1.csnzoo.com/external-staging/ekidd/rust-musl-builder:1.28.0 as builder
RUN sudo apt update && sudo apt install -y bison flex automake
RUN sudo cp /usr/bin/musl-gcc /usr/bin/musl-g++
WORKDIR /home/rust/src
COPY Cargo.* /home/rust/src/
COPY src /home/rust/src/src
COPY window /home/rust/src/window
ENV CC=musl-gcc
ENV CFLAGS=-I/usr/local/musl/include
ENV LIB_LDFLAGS=-L/usr/lib/x86_64-linux-gnu
ENV PREFIX=/usr/local/musl
RUN cargo build --release

FROM artifactory.service.bo1.csnzoo.com/wayfair/centos74-base:0.1.2

WORKDIR /root/
RUN yum install -y rpm-build
RUN mkdir -p /root/rpmbuild/SOURCES/tremor-{vsn}
RUN mkdir -p /root/rpmbuild/SPECS/
COPY --from=builder /home/rust/src/target/x86_64-unknown-linux-musl/release/tremor-runtime /root/rpmbuild/SOURCES/tremor-{vsn}/tremor-runtime
COPY rpm/tremor-runtime-svc.sh /root/rpmbuild/SOURCES/tremor-{vsn}
COPY rpm/tremor.conf.sample /root/rpmbuild/SOURCES/tremor-{vsn}
COPY rpm/tremor.service /root/rpmbuild/SOURCES/tremor-{vsn}
COPY LICENSE /root/rpmbuild/SOURCES/tremor-{vsn}
RUN cd /root/rpmbuild/SOURCES && tar -czf tremor-{vsn}.tar.gz tremor-{vsn}
COPY rpm/tremor.spec /root/rpmbuild/SPECS/tremor-{vsn}.spec

RUN rpmbuild -v -ba /root/rpmbuild/SPECS/tremor-{vsn}.spec

# This image runs SimpleHTTPServer when the container starts.
#
# 9.  Change this to a command which starts your application.
#
CMD /usr/sbin/init
