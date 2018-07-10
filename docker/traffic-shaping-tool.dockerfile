#
# docker/mshello.dockerfile
#
# This Dockerfile servces as an example for how to use Wayfair Docker CI and
# how to structure Docker specific configuration.
#
# The produced container does the following:
#
# - Inherits from a Wayfair specific base image.
# - Runs a simple Python HTTP webserver.
#
# When applying this to your own application, follow the steps below to adapt
# it to your application.
#
# **NOTE**: There are numbered instructions below on what you should change,
# which is appropriate for working from a base CentOS image. Modifications
# necessary to this file may change depending on which base image you're using.
#
# @author Matthew Coleman <mcoleman@wayfair.com>
# @copyright 2018 Wayfair, LLC. -- All rights reserved.

FROM artifactory.service.bo1.csnzoo.com/external-staging/ekidd/rust-musl-builder:1.26.0 as builder
WORKDIR /home/rust/src
COPY Cargo.* /home/rust/src/
COPY src /home/rust/src/src
RUN find
RUN LIB_LDFLAGS=-L/usr/lib/x86_64-linux-gnu CFLAGS=-I/usr/local/musl/include CC=musl-gcc PREFIX=/usr/local/musl cargo build --release


# The `centos74-base` image is a basic CentOS Docker image that can act as a
# basis for anything.
#
# 1. Change this to a base image appropriate for your application.
#
# FROM artifactory.service.bo1.csnzoo.com/wayfair/centos74-base:0.1.2
FROM artifactory.service.bo1.csnzoo.com/external/alpine:3.6

# The `wf_version` is a semantic version that should increment on each
# application or Docker configuration change. CI will check the version that
# you're building doesn't already exist.
#
# 2. Start at a version semantic version you prefer to use.
#
ARG tag=0.1.3
ENV wf_version=$tag

# This ENV declaration uses a base image build hook defined in `centos74-base`.
#
# 3.  Change this to a description of your application.
#
ENV wf_label="Data Engineering kafka to kafka copyer"

# Label metadata can be used by automation to gather information about Docker
# images deploying to production, or be directly observable using something
# like `docker inspect`.
#
# For example, the `com.wayfair.version` determines the semantic verion used to
# tag an image in CI before deployment.
LABEL \
    # This should be the same as your container image name.
    #
    # 4.  Change this to same name as your image.
    #
    com.wayfair.app="wayfair/data-engineering/traffic-shaping-tool" \
    com.wayfair.description=${wf_label} \
    # This should define the team that owns the application running in the
    # container.
    #
    # 5.  Change this label to the team that maintains your application.
    #
    com.wayfair.maintainer="Heinz Gies <hgies@wayfair.com>" \
    com.wayfair.vendor="Wayfair LLC." \
    com.wayfair.version=${wf_version}

# In this case, the Python HTTP server listens on port 5678, and these set up
# the application. It tells the Docker engine about the listening port, creates
# space for the application code to live, and copies it into the image.
#
# 6.  Change the port number to what your service listens on, or remove
#     if this image doesn't actually run an application that listens on a port.
#
# 7.  Change the COPY command to copy from whatever directory holds your
#     application code in this repository.
#
WORKDIR /root/
RUN apk --no-cache add ca-certificates
COPY --from=builder /home/rust/src/target/x86_64-unknown-linux-musl/release/traffic-shaping-tool .
COPY traffic-shaping-tool.sh .

# This image runs SimpleHTTPServer when the container starts.
#
# 9.  Change this to a command which starts your application.
#
CMD ["./traffic-shaping-tool.sh"]
