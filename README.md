# catbus

Fast tarball transport.

A content-aware transport for tar archives modelled on git `upload-pack` / `receive-pack`.

![](nekobasu.gif)

Save bytes and time when transferring a sequence of tar archives containing
largely the same content over time by sourcing already received parts from
a library-of-parts cache.

catbus allows you to slim down the bytes necessary to transport a given tar archive by
constructing an archive on the remote end from previously received tar archives.
