FROM ubuntu:precise

RUN apt-get update && apt-get install -y \
  build-essential \
  curl \
  libssl-dev \
  lsb-release \
  pkg-config

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH="/root/.cargo/bin:$PATH"

RUN cargo install sccache
ENV RUSTC_WRAPPER=sccache