FROM gitpod/workspace-full

USER gitpod

RUN bash -lc "rustup default stable"

USER root
