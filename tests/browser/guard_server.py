"""Tiny local HTTP server for guard tests: serves a form page and counts hits on /post."""

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

FORM_PAGE = b"<!doctype html><html><head><title>form</title></head><body>form page</body></html>"


class GuardServer:
    def __init__(self) -> None:
        self.requests: list[tuple[str, str, bytes]] = []  # (method, path, body) for /post*
        self.redirect_hits = 0
        self._lock = threading.Lock()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args) -> None:  # keep test output quiet
                pass

            def _reply(self, code: int, body: bytes, ctype: str = "text/html") -> None:
                self.send_response(code)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(body)

            def _handle(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length else b""
                path = self.path.split("?", 1)[0]
                if path.startswith("/post"):
                    with owner._lock:
                        owner.requests.append((self.command, self.path, body))
                    self._reply(200, b"posted ok", "text/plain")
                elif path == "/popup":
                    self._reply(200, b"<!doctype html><title>popup</title>popup")
                elif path == "/redirect":
                    with owner._lock:
                        owner.redirect_hits += 1
                    self.send_response(307)
                    self.send_header("Location", "/post-redirected")
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                elif path == "/sw.js":
                    self._reply(200, owner.service_worker_js().encode(), "text/javascript")
                elif path in ("/worker.js", "/shared.js"):
                    self._reply(200, owner.worker_js(path).encode(), "text/javascript")
                elif path == "/framehost":
                    frame = f"{owner.local_url}/autopost"
                    page = f'<!doctype html><title>framehost</title><iframe src="{frame}"></iframe>'
                    self._reply(200, page.encode())
                elif path == "/autopost":  # runs on localhost, posts cross-site to 127.0.0.1
                    js = (
                        f"fetch('{owner.url}/post', {{method: 'POST', body: 'from-frame', "
                        "mode: 'no-cors'}).then(() => document.title = 'sent', "
                        "() => document.title = 'failed')"
                    )
                    self._reply(
                        200, f"<!doctype html><title>frame</title><script>{js}</script>".encode()
                    )
                else:
                    self._reply(200, FORM_PAGE)

            do_GET = do_POST = do_PUT = do_DELETE = do_HEAD = do_PATCH = _handle

        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self._httpd.server_address[1]
        self.url = f"http://127.0.0.1:{self.port}"
        self.local_url = f"http://localhost:{self.port}"  # same server, different host name
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)

    def worker_js(self, path: str) -> str:
        post = (
            f"fetch('{self.url}/post', {{method: 'POST', body: 'from-worker'}})"
            ".then(r => 'status ' + r.status, e => 'error ' + e)"
        )
        if path == "/shared.js":
            reply = f"{post}.then(m => p.postMessage(m));"
            return f"onconnect = (e) => {{ const p = e.ports[0]; {reply} }};"
        return f"{post}.then(m => postMessage(m));"

    def service_worker_js(self) -> str:
        return (
            "self.addEventListener('install', (e) => self.skipWaiting());"
            "self.addEventListener('activate', (e) => e.waitUntil(self.clients.claim()));"
            "self.addEventListener('message', (e) => {"
            f" fetch('{self.url}/post', {{method: 'POST', body: 'from-sw'}})"
            ".then(r => 'status ' + r.status, err => 'error ' + err)"
            ".then(m => e.source.postMessage(m)); });"
        )

    @property
    def hits(self) -> int:
        with self._lock:
            return len(self.requests)

    def start(self) -> "GuardServer":
        self._thread.start()
        return self

    def stop(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
