# build via:
#   docker build --network host -t tremorproject/tremor-musl-builder-cross:latest -f ./tremor-musl-builder-cross.Dockerfile .

# version at the end here is cross version
#
# this starter image should be built from https://github.com/anupdhml/cross/blob/upgrade_musl_cross_make/, via
#   ./build-docker-image.sh x86_64-unknown-linux-musl
#
# not basing this from cross's default image since that has older gcc (6.4) which does not
# work for our dependencies like snmalloc
FROM anupdhml/cross:x86_64-unknown-linux-musl-0.2.0

# works almost but does not have static version of libatomic and libstdc++
# (requirements for snmalloc)
#FROM ekidd/rust-musl-builder:stable
#
# since the image ekidd/rust-musl-builder runs code as this user
#USER rust

RUN apt-get update \
    && apt-get install -y \
      # for onig_sys (via the regex crate)
      libclang-dev \
      # build essentials
      # TODO we should not need all of this but trying for now
      cmake musl-dev musl-tools linux-libc-dev \
      libatomic1-dbg cmake-doc ninja-build \
    # Clean up
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

# attempt to overide rustc that cross shares
#RUN apt-get install -y curl
#RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o rustup-init.sh \
#    #&& sh rustup-init.sh -y --no-modify-path \
#    && sh rustup-init.sh -y \
#    && rm rustup-init.sh
#ENV PATH="/root/.cargo/bin:${PATH}"
#RUN rustup target add x86_64-unknown-linux-musl

#RUN ln -s "/usr/bin/g++" "/usr/bin/musl-g++"
#RUN sudo cp /usr/lib/gcc/x86_64-linux-gnu/7/libstdc++.a /usr/local/musl/lib/
