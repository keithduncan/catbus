# catbus

Fast tarball transport. A content-aware transport for tar archives modelled on git `upload-pack` / `receive-pack`.

![](nekobasu.gif)

Save bytes and time when transferring a sequence of tar archives containing
largely the same content over time by sourcing already received parts from
a library-of-parts cache.

catbus allows you to slim down the bytes necessary to transport a given tar archive by
constructing an archive on the remote end from previously received tar archives.

## How it works

![](protocol.png]

_Generated using https://sequencediagram.org/_

1. An index of each tarball is precomputed and stored beside the tar archives.
  1. `catbus index` is used to generate the index files.
1. A receiver invokes `catbus transport receive-index` connected to a sender `catbus transport upload-index`
  1. A reliable transport is assumed, such as SSH tunnels.
1. The sender writes the index to the receiver
1. The receiver tries to source the parts from the previously received local library of parts
1. The receiver requests missing parts from the sender
1. The sender generates an in-memory archive of the missing parts and sends it
1. The receiver inserts the missing parts and serialises the full archive

## How to build

catbus is built using cargo and rust.

```
cd ~/catbus
cargo build
```
