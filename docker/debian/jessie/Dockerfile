FROM rust:1.26-jessie

RUN apt-get update && apt-get install -y \
  build-essential \
  curl \
  libssl-dev \
  lsb-release \
  pkg-config

RUN cargo install sccache
ENV RUSTC_WRAPPER=sccache