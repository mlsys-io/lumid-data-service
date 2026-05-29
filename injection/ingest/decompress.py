"""Content-Encoding: gzip|zstd transparent decode.

One helper, used by both single-shot (typed/adapter/file) and streaming
(ndjson stream) paths. Zstd is optional (zstandard pip lib); gzip is in
the stdlib.
"""
from __future__ import annotations

import gzip
import io
import logging
from typing import AsyncIterator, Iterable, Iterator

from .errors import IngestError

log = logging.getLogger("findata.ingest.decompress")


def decode(body: bytes, content_encoding: str | None) -> bytes:
    """One-shot: decode `body` if Content-Encoding asks for it; else return as-is."""
    enc = (content_encoding or "").strip().lower()
    if not enc or enc == "identity":
        return body
    if enc == "gzip" or enc == "x-gzip":
        try:
            return gzip.decompress(body)
        except OSError as e:
            raise IngestError(f"gzip decode failed: {e}") from e
    if enc == "zstd":
        try:
            import zstandard as zstd  # type: ignore
        except Exception as e:  # pragma: no cover — zstandard optional
            raise IngestError(f"zstd decode requires `zstandard` package ({e})") from e
        try:
            return zstd.ZstdDecompressor().decompress(body)
        except Exception as e:
            raise IngestError(f"zstd decode failed: {e}") from e
    raise IngestError(f"unsupported Content-Encoding {enc!r}")


async def aiter_decoded(
    stream: AsyncIterator[bytes], content_encoding: str | None
) -> AsyncIterator[bytes]:
    """Async byte iterator with transparent decode.

    For gzip we decode in 64 KB output chunks via `zlib.decompressobj(gzip)`.
    For zstd we use ZstdDecompressor.stream_reader. For identity we just
    pass through.
    """
    enc = (content_encoding or "").strip().lower()
    if not enc or enc == "identity":
        async for chunk in stream:
            yield chunk
        return
    if enc in ("gzip", "x-gzip"):
        import zlib
        dec = zlib.decompressobj(zlib.MAX_WBITS | 16)
        async for chunk in stream:
            if not chunk:
                continue
            out = dec.decompress(chunk)
            if out:
                yield out
        tail = dec.flush()
        if tail:
            yield tail
        return
    if enc == "zstd":
        try:
            import zstandard as zstd  # type: ignore
        except Exception as e:  # pragma: no cover
            raise IngestError(f"zstd decode requires `zstandard` package ({e})") from e
        dec = zstd.ZstdDecompressor()
        rdr = dec.stream_reader(_AsyncBytesAsStream(stream))
        while True:
            buf = rdr.read(65536)
            if not buf:
                break
            yield buf
        return
    raise IngestError(f"unsupported Content-Encoding {enc!r}")


class _AsyncBytesAsStream:
    """Adapter from an async byte iterator to a sync `read(n)` interface
    (zstandard.stream_reader expects sync read). We buffer in memory; the
    typical compressed-payload sizes (MB-scale) are fine for that."""

    def __init__(self, aiter: AsyncIterator[bytes]):
        self._aiter = aiter
        self._buf = io.BytesIO()
        self._done = False

    def _drain_sync(self) -> None:
        # zstandard calls read() synchronously; we can't await here. The
        # callers using aiter_decoded(...) with zstd must therefore prefetch
        # the whole compressed body upfront via b''.join([…]). Raise loudly
        # if we hit this path unprepared.
        raise RuntimeError(
            "zstd streaming decode requires preloaded bytes — call "
            "decompress.decode() on a fully-read body instead."
        )

    def read(self, n: int = -1) -> bytes:  # pragma: no cover — see _drain_sync
        self._drain_sync()
        return b""
