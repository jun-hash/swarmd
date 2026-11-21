"""Secret redaction: one set of secrets (bot and session tokens), scrubbed from the records of
every logger that prints requests and from every error text we log or store."""

import logging

# a logger's filters only see records logged on that very logger (not on its children), so
# each logger that prints requests is named: websockets.client logs handshake headers at DEBUG
REQUEST_LOGGERS = ("httpx", "httpcore.connection", "httpcore.http11", "httpcore.http2",
                   "httpcore.proxy", "websockets.client")  # fmt: skip
MASK = "<token>"

_secrets: set[str] = set()


def add_secret(secret: str) -> None:
    if secret:  # an empty string would match between every character
        _secrets.add(secret)


def redact(text: str) -> str:
    for s in _secrets:
        text = text.replace(s, MASK)
    return text


class _Filter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if _secrets:
            msg = record.getMessage()
            clean = redact(msg)
            if clean != msg:
                record.msg, record.args = clean, None
        return True


_FILTER = _Filter()


def install(*logger_names: str) -> None:
    for name in logger_names:
        logging.getLogger(name).addFilter(_FILTER)


install(*REQUEST_LOGGERS)
