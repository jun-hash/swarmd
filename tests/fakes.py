"""Fakes shared across test packages: a scripted brain, a notifier spy and a Telegram API."""

import json

import httpx

from swarm.ports import BrainError


class FakeBrain:
    """Returns `result` or raises `error`, recording each call's arguments.

    With neither set, `decide` raises BrainError, as an unavailable engine would."""

    def __init__(self, result=None, error: Exception | None = None):
        self.result = result
        self.error = error
        self.calls: list[dict] = []

    async def decide(self, *, purpose, prompt, schema, timeout_s=60):
        self.calls.append(
            {"purpose": purpose, "prompt": prompt, "schema": schema, "timeout_s": timeout_s}
        )
        if self.error is not None:
            raise self.error
        if self.result is None:
            raise BrainError("no result")
        return self.result


class NotifierSpy:
    """Records every sent message.

    `fail_next` makes the next n sends raise RuntimeError, and channels in `reject` raise
    ValueError. With `one_per_ref_id`, a repeated ref_id is dropped, like the outbox."""

    def __init__(self, *, one_per_ref_id: bool = False) -> None:
        self.sent: list[dict] = []
        self.fail_next = 0
        self.reject: set[str] = set()
        self.one_per_ref_id = one_per_ref_id

    async def send(self, msg):
        if self.fail_next:
            self.fail_next -= 1
            raise RuntimeError("db locked")
        if self.reject and msg["channel"] in self.reject:
            raise ValueError("not a telegram channel")
        if self.one_per_ref_id and msg["ref_id"]:
            if any(m["ref_id"] == msg["ref_id"] for m in self.sent):
                return
        self.sent.append(dict(msg))

    def has_ask(self, ask_id):
        return any(m["kind"] == "ask" and m["ref_id"] == ask_id for m in self.sent)


class FakeTelegram:
    """httpx transport standing in for api.telegram.org (no network).

    `fail` is a queue of scripted replies: an int status (with an optional retry_after
    after a colon, e.g. "429:90"), or an Exception to raise. Empty queue -> success."""

    def __init__(self) -> None:
        self.sent: list[dict] = []
        self.attempts: list[dict] = []
        self.paths: list[str] = []
        self.fail: list = []
        self.transport = httpx.MockTransport(self.handle)

    def handle(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        self.paths.append(request.url.path)
        self.attempts.append(body)
        if self.fail:
            f = self.fail.pop(0)
            if isinstance(f, Exception):
                raise f
            status, _, retry = str(f).partition(":")
            reply: dict = {"ok": False, "error_code": int(status), "description": "scripted"}
            if retry:
                reply["parameters"] = {"retry_after": int(retry)}
            return httpx.Response(int(status), json=reply)
        self.sent.append(body)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": len(self.sent)}})

    def texts(self) -> list[str]:
        return [m["text"] for m in self.sent]
