"""Pure request policy for ActionGuard: which requests pass, which need an approval token,
how a request is described and fingerprinted, and whether a token matches it. No I/O."""

import base64
import hashlib
from fnmatch import fnmatchcase
from typing import Literal
from urllib.parse import urlsplit

from swarm.ports import ApprovalToken, RequestDescriptor
from swarm.types import GuardMode, WorkOrderId

SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
# workers on these schemes belong to the browser itself (component extensions, WebUI) and are
# not reachable from web content; every other scheme (http, https, blob, data, filesystem) is
# guarded
RELEASED_WORKER_SCHEMES = frozenset({"chrome-extension", "chrome", "chrome-untrusted", "devtools"})

Verdict = Literal["pass", "token", "block"]


def host_allowed(host: str, allowlist: list[str]) -> bool:
    h = host.lower().rstrip(".")
    for entry in allowlist:
        e = entry.lower().strip().rstrip(".")
        if e and (h == e or h.endswith("." + e)):
            return True
    return False


def classify_request(mode: GuardMode, method: str, host: str, allowlist: list[str]) -> Verdict:
    if method.upper() in SAFE_METHODS or mode == "off":
        return "pass"
    if mode == "dry_run":
        return "block"
    return "pass" if host_allowed(host, allowlist) else "token"


def fingerprint(method: str, host: str, path: str, body: bytes) -> str:
    return hashlib.sha256(f"{method}\n{host}\n{path}\n".encode() + body).hexdigest()


def request_body(request: dict) -> bytes:
    entries = request.get("postDataEntries")
    if entries:
        return b"".join(base64.b64decode(e.get("bytes", "")) for e in entries)
    return (request.get("postData") or "").encode()


def describe_request(request: dict) -> RequestDescriptor:
    method = request["method"].upper()
    parts = urlsplit(request["url"])
    host = (parts.hostname or "").lower()
    path = (parts.path or "/") + (f"?{parts.query}" if parts.query else "")
    body = request_body(request)
    return RequestDescriptor(
        method=method,
        host=host,
        path=path,
        body_sha256=hashlib.sha256(body).hexdigest(),
        fingerprint=fingerprint(method, host, path, body),
    )


def guardable_worker(info: dict) -> bool:
    """A service/shared worker reachable from web content (judged by its target info)."""
    return urlsplit(info.get("url", "")).scheme not in RELEASED_WORKER_SCHEMES


def token_matches(token: ApprovalToken, d: RequestDescriptor, order_id: WorkOrderId) -> bool:
    return (
        not token.used
        and token.order_id == order_id
        and token.method.upper() == d["method"]
        and token.host.lower() == d["host"]
        and fnmatchcase(d["path"], token.path_pattern)
    )
