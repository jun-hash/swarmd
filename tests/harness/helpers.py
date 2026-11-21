"""Helpers for the browser MCP integration tests: local site, fake internal API, MCP client."""

import contextlib
import json
import socket
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import uvicorn
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from swarm.types import CdpSession

PAGES = {
    "/": (
        b"<!doctype html><title>Home</title><h1>Home page</h1>"
        b'<a href="/two">Go to two</a>'
        b'<form method="POST" action="/post"><input name="v" aria-label="Name">'
        b'<button type="submit">Send</button></form>'
        b"<p id=t>" + b"filler text " * 1000 + b"</p>"
    ),
    "/dead": (
        b"<!doctype html><title>Dead</title>"
        b'<form method="POST" action="http://127.0.0.1:1/post"><button>Fire</button></form>'
    ),
    "/two": b"<!doctype html><title>Two</title><h1>Second page</h1>",
    "/search": (
        b"<!doctype html><title>Search</title><form method=GET action=/results>"
        b'<input name=q aria-label="Query"></form>'
    ),
    "/results": b"<!doctype html><title>Results</title><h1>Results page</h1>",
}


class Site:
    def __init__(self, pages: dict[str, bytes] | None = None) -> None:
        pages = PAGES if pages is None else pages
        self.posts: list[bytes] = []
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a) -> None:
                pass

            def do_GET(self) -> None:
                path = self.path.split("?", 1)[0]
                body = pages.get(path, b"not found")
                self.send_response(200 if path in pages else 404)
                self.send_header("Content-Type", "text/html")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self) -> None:
                n = int(self.headers.get("Content-Length") or 0)
                owner.posts.append(self.rfile.read(n))
                body = b"<!doctype html><title>Posted</title>posted"
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://localhost:{self.httpd.server_port}"
        self._t = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def start(self) -> "Site":
        self._t.start()
        return self

    def stop(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()


class FakeInternal:
    """Stands in for the internal API: records requests, answers with canned bodies."""

    def __init__(self) -> None:
        self.requests: list[dict] = []
        self.ask_reply: dict = {"ask_id": "ask_1", "outcome": None, "number": 7}

        async def record(request: Request) -> JSONResponse:
            body = await request.json()
            self.requests.append(
                {
                    "path": request.url.path,
                    "auth": request.headers.get("authorization"),
                    "body": body,
                }
            )
            if request.url.path.endswith("/asks"):
                return JSONResponse(self.ask_reply)
            return JSONResponse({"evidence_id": "ev_1"})

        app = Starlette(routes=[Route("/internal/runs/{run}/{what}", record, methods=["POST"])])
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        self.url = f"http://127.0.0.1:{port}"
        self.server = uvicorn.Server(uvicorn.Config(app, port=port, log_level="error"))
        self._t = threading.Thread(target=self.server.run, daemon=True)

    def start(self) -> "FakeInternal":
        self._t.start()
        for _ in range(100):
            if self.server.started:
                break
            time.sleep(0.05)
        return self

    def stop(self) -> None:
        self.server.should_exit = True
        self._t.join(5)


class Mcp:
    def __init__(self, session: ClientSession) -> None:
        self.session = session

    async def call(self, name: str, **args) -> str:
        res = await self.session.call_tool(name, args)
        text = "\n".join(c.text for c in res.content if getattr(c, "type", "") == "text")
        assert not res.is_error, text
        return text


@contextlib.asynccontextmanager
async def mcp_client(
    session: CdpSession,
    *,
    internal: FakeInternal,
    tmp_path: Path,
    policy: str = "compressed",
):
    token = tmp_path / "token"
    token.write_text("tok-123\n")
    blobs = tmp_path / "blobs"
    blobs.mkdir(exist_ok=True)
    params = StdioServerParameters(
        command=sys.executable,
        args=[
            "-m", "swarm.harness.own.browser_mcp",
            "--cdp-url", session["cdp_url"],
            "--target-id", session["target_id"],
            "--run-id", "run_1",
            "--internal", internal.url,
            "--token-file", str(token),
            "--blob-dir", str(blobs),
            "--policy", policy,
        ],
    )  # fmt: skip
    async with stdio_client(params) as (r, w), ClientSession(r, w) as cs:
        await cs.initialize()
        yield Mcp(cs)


def dump(x) -> str:
    return json.dumps(x, default=str)
