#!/usr/bin/env python3
"""Minimal S3-compatible endpoint backed by the local filesystem.

Just enough of the API for the Kimistore agent to run unmodified: PutObject
(including the If-Match / If-None-Match conditional writes the writer lease is
built on), GetObject (with Range), ListObjectsV2, DeleteObject, HeadBucket.
"""
import hashlib
import os
import re
import sys
import threading
import shutil
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, unquote
from datetime import datetime, timezone


def iso8601(ts):
    """S3 returns ISO-8601, not RFC 1123; the SDK rejects anything else."""
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

ROOT = sys.argv[2] if len(sys.argv) > 2 else "/tmp/kimi-s3"
BUCKET = "kimistore"


def key_path(bucket, key):
    safe = key.replace("..", "_")
    return os.path.join(ROOT, bucket, safe)


# The server is threaded and the agent writes from several goroutines at once
# (segments, checkpoints, manifests, ownership claims, routing tables), so every
# filesystem touch has to be serialised. Without it two problems appear that
# look like agent bugs rather than harness bugs: a concurrent mkdir of the same
# directory races, and -- far worse -- a reader can observe a file that a writer
# has truncated but not yet filled, which reads as a corrupt segment.
FS_LOCK = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        if os.environ.get("SHIM_DEBUG"):
            print("SHIM:", a, file=sys.stderr, flush=True)

    def _split(self):
        p = urlparse(self.path)
        parts = p.path.lstrip("/").split("/", 1)
        bucket = unquote(parts[0]) if parts else ""
        key = unquote(parts[1]) if len(parts) > 1 else ""
        return bucket, key, parse_qs(p.query)

    def _error(self, code, s3code, msg):
        """Send an S3 error and report that the response has been handled.

        The return value matters: callers that must not fall through to writing
        the body rely on it. Returning None here would let a rejected
        conditional write proceed and overwrite the object it was supposed to
        be refused for.
        """
        body = (
            f'<?xml version="1.0" encoding="UTF-8"?><Error><Code>{s3code}</Code>'
            f"<Message>{msg}</Message></Error>"
        ).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/xml")
        self.send_header("Content-Length", str(len(body)))
        # Close the connection on an error. The SDK pools connections, and a
        # kept-alive socket after a 4xx is how a client ends up reading a
        # stale response body for its next request.
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.close_connection = True
        return True

    def _ok(self, body=b"", ctype="application/xml", extra=None):
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if body:
            self.wfile.write(body)

    def _etag_for(self, path):
        """S3's ETag is the MD5 of the object, quoted. Conditional writes are
        expressed with it, so it has to be real rather than a constant."""
        with open(path, "rb") as f:
            return '"%s"' % hashlib.md5(f.read()).hexdigest()

    def _peek(self, path):
        try:
            with open(path, "rb") as f:
                return f.read()[:16]
        except OSError:
            return None

    def _check_preconditions(self, path):
        """Enforce If-Match / If-None-Match. True when the write is refused.

        Without this the writer lease cannot fence: two agents would both
        believe their conditional write succeeded.
        """
        if_none_match = self.headers.get("If-None-Match")
        if_match = self.headers.get("If-Match")
        if if_none_match is None and if_match is None:
            return None

        exists = os.path.isfile(path)
        if if_none_match == "*" and exists:
            return self._error(412, "PreconditionFailed", "At least one of the preconditions failed.")
        if if_match is not None:
            if not exists:
                return self._error(412, "PreconditionFailed", "At least one of the preconditions failed.")
            want = if_match.strip('"')
            have = self._etag_for(path).strip('"')
            if want != "ETAG" and want != have:
                return self._error(412, "PreconditionFailed", "At least one of the preconditions failed.")
        return None

    def do_PUT(self):
        bucket, key, _ = self._split()
        path = key_path(bucket, key)
        n = int(self.headers.get("Content-Length", 0))

        with FS_LOCK:
            have = self._etag_for(path) if os.path.isfile(path) else None
            failed = self._check_preconditions(path)
            if os.environ.get("SHIM_DEBUG"):
                print("SHIM PUT %s inm=%r im=%r have=%r rejected=%s content=%r" % (
                    key, self.headers.get("If-None-Match"), self.headers.get("If-Match"),
                    have, failed, self._peek(path)), file=sys.stderr, flush=True)
            if not failed:
                # Write to a temporary file and rename it into place, so a
                # concurrent reader sees either the old object or the new one and
                # never a half-written one.
                os.makedirs(os.path.dirname(path), exist_ok=True)
                tmp = "%s.tmp.%d" % (path, threading.get_ident())
                try:
                    with open(tmp, "wb") as f:
                        remaining = n
                        while remaining > 0:
                            chunk = self.rfile.read(min(65536, remaining))
                            if not chunk:
                                break
                            f.write(chunk)
                            remaining -= len(chunk)
                    os.replace(tmp, path)
                except BaseException:
                    if os.path.exists(tmp):
                        os.remove(tmp)
                    raise
            etag = self._etag_for(path)

        if failed:
            # Drain the body so the client's write completes cleanly before
            # the connection goes away.
            self.rfile.read(n)
            return
        self._ok(b"", "application/xml", {"ETag": etag})

    def do_DELETE(self):
        bucket, key, _ = self._split()
        p = key_path(bucket, key)
        with FS_LOCK:
            if os.path.isfile(p):
                os.remove(p)
        self._ok(b"")

    def do_HEAD(self):
        bucket, key, q = self._split()
        p = key_path(bucket, key)
        if not key:
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        with FS_LOCK:
            if not os.path.isfile(p):
                size = None
            else:
                size = os.path.getsize(p)
        if size is None:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Length", str(size))
        self.end_headers()

    def do_GET(self):
        bucket, key, q = self._split()

        if not key:
            return self._list(bucket, q)

        p = key_path(bucket, key)
        rng = self.headers.get("Range")
        # Hold the lock across the whole read so a concurrent writer cannot
        # truncate the file between the size check and the read.
        with FS_LOCK:
            if not os.path.isfile(p):
                return self._error(404, "NoSuchKey", "The specified key does not exist.")

            size = os.path.getsize(p)
            start, end = 0, size - 1
            partial = False
            if rng:
                m = re.match(r"bytes=(\d*)-(\d*)", rng)
                if m:
                    partial = True
                    if m.group(1):
                        start = int(m.group(1))
                        if m.group(2):
                            end = int(m.group(2))
                    else:
                        start = max(0, size - int(m.group(2)))
                        end = size - 1
            if start >= size:
                unavailable = True
            else:
                unavailable = False
                end = min(end, size - 1)
                length = end - start + 1
                with open(p, "rb") as f:
                    f.seek(start)
                    data = f.read(length)
                etag = self._etag_for(p)

        if unavailable:
            self.send_response(416)
            self.send_header("Content-Range", f"bytes */{size}")
            self.send_header("Content-Length", "0")
            self.send_header("Connection", "close")
            self.end_headers()
            self.close_connection = True
            return

        self.send_response(206 if partial else 200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("ETag", etag)
        if partial:
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()
        self.wfile.write(data)

    def _list(self, bucket, q):
        prefix = (q.get("prefix") or [""])[0]
        base = os.path.join(ROOT, bucket)
        items = []
        with FS_LOCK:
            for dirpath, _dirs, files in os.walk(base):
                for fn in files:
                    # A partially written temporary file is not an object.
                    if ".tmp." in fn:
                        continue
                    full = os.path.join(dirpath, fn)
                    rel = os.path.relpath(full, base).replace(os.sep, "/")
                    if rel.startswith(prefix):
                        st = os.stat(full)
                        items.append((rel, st.st_size, st.st_mtime))
            items.sort()
        max_keys = int((q.get("max-keys") or ["1000"])[0])
        truncated = len(items) > max_keys
        items = items[:max_keys]

        parts = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
            f"<Name>{bucket}</Name><Prefix>{prefix}</Prefix>",
            f"<KeyCount>{len(items)}</KeyCount><MaxKeys>{max_keys}</MaxKeys>",
            f"<IsTruncated>{'true' if truncated else 'false'}</IsTruncated>",
        ]
        for key, size, mtime in items:
            ts = iso8601(mtime)
            etag = self._etag_for(os.path.join(base, key))
            parts.append(
                f"<Contents><Key>{key}</Key><LastModified>{ts}</LastModified>"
                f"<ETag>&quot;{etag.strip(chr(34))}&quot;</ETag><Size>{size}</Size>"
                f"<StorageClass>STANDARD</StorageClass></Contents>"
            )
        parts.append("</ListBucketResult>")
        self._ok("".join(parts).encode())


if __name__ == "__main__":
    # Create the bucket if absent; never wipe it, so a restart of the broker can
    # recover state written by the previous process.
    os.makedirs(os.path.join(ROOT, BUCKET), exist_ok=True)
    port = int(sys.argv[1])
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
