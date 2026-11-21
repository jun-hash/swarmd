"""Pure ActionGuard decision rules (no Chrome)."""

import base64
import hashlib

import pytest

from swarm.browser.requests import (
    classify_request,
    describe_request,
    fingerprint,
    host_allowed,
    token_matches,
)
from swarm.ports import ApprovalToken


@pytest.mark.parametrize("method", ["GET", "HEAD", "OPTIONS", "get"])
@pytest.mark.parametrize("mode", ["guarded", "dry_run", "off"])
def test_safe_methods_always_pass(method, mode):
    assert classify_request(mode, method, "evil.example", []) == "pass"


def test_guarded_non_get_needs_token_unless_allowlisted():
    assert classify_request("guarded", "POST", "shop.example", []) == "token"
    assert classify_request("guarded", "DELETE", "api.shop.example", ["shop.example"]) == "pass"
    assert classify_request("guarded", "PUT", "shop.example", ["shop.example"]) == "pass"
    assert classify_request("guarded", "POST", "notshop.example", ["shop.example"]) == "token"


def test_dry_run_blocks_every_non_get_even_allowlisted():
    assert classify_request("dry_run", "POST", "shop.example", ["shop.example"]) == "block"
    assert classify_request("dry_run", "PATCH", "x.example", []) == "block"


def test_off_passes_everything():
    assert classify_request("off", "POST", "x.example", []) == "pass"


def test_host_allowed_matches_exact_and_subdomains_case_insensitive():
    assert host_allowed("Mail.Google.com", ["google.com"])
    assert host_allowed("google.com", ["GOOGLE.COM"])
    assert not host_allowed("evilgoogle.com", ["google.com"])
    assert not host_allowed("google.com", [])


def test_describe_request_hashes_body_and_fingerprints():
    body = b"a=1&b=2"
    req = {
        "method": "post",
        "url": "https://Shop.Example:8443/cart/checkout?x=1#frag",
        "postDataEntries": [
            {"bytes": base64.b64encode(body[:3]).decode()},
            {"bytes": base64.b64encode(body[3:]).decode()},
        ],
    }
    d = describe_request(req)
    assert d["method"] == "POST"
    assert d["host"] == "shop.example"
    assert d["path"] == "/cart/checkout?x=1"  # query is part of the path; fragment is not
    assert d["body_sha256"] == hashlib.sha256(body).hexdigest()
    assert d["fingerprint"] == fingerprint("POST", "shop.example", "/cart/checkout?x=1", body)
    bare = describe_request({"method": "POST", "url": "https://shop.example/cart/checkout"})
    assert bare["path"] == "/cart/checkout"
    # postData string fallback gives the same fingerprint
    d2 = describe_request({"method": "POST", "url": req["url"], "postData": "a=1&b=2"})
    assert d2["fingerprint"] == d["fingerprint"]
    assert describe_request({"method": "POST", "url": req["url"]})["body_sha256"] == (
        hashlib.sha256(b"").hexdigest()
    )


def test_fingerprint_changes_with_any_component():
    base = fingerprint("POST", "h", "/p", b"x")
    assert base == hashlib.sha256(b"POST\nh\n/p\nx").hexdigest()
    assert (
        len(
            {
                base,
                fingerprint("PUT", "h", "/p", b"x"),
                fingerprint("POST", "g", "/p", b"x"),
                fingerprint("POST", "h", "/q", b"x"),
                fingerprint("POST", "h", "/p", b"y"),
            }
        )
        == 5
    )


def test_token_matches_method_host_path_pattern_and_order():
    d = describe_request({"method": "POST", "url": "https://h.example/api/items/42"})
    t = ApprovalToken(
        ask_id="ask_1",
        method="POST",
        host="h.example",
        path_pattern="/api/items/*",
        order_id="wo_1",
    )
    assert token_matches(t, d, "wo_1")
    assert not token_matches(t, d, "wo_2")  # a token only serves its own order

    def tok(method, host, pattern):
        return ApprovalToken("ask_1", method, host, pattern, order_id="wo_1")

    assert not token_matches(tok("PUT", "h.example", "/api/items/*"), d, "wo_1")
    assert not token_matches(tok("POST", "g.example", "/api/*"), d, "wo_1")
    assert not token_matches(tok("POST", "h.example", "/api/other"), d, "wo_1")
    with pytest.raises(TypeError):
        ApprovalToken("ask_1", "POST", "h.example", "/api/*")  # order_id is required
    t.used = True
    assert not token_matches(t, d, "wo_1")


def test_token_from_descriptor_is_exact_path():
    d = describe_request({"method": "POST", "url": "https://h.example/a[1]/b*"})
    t = ApprovalToken.from_descriptor("ask_9", d, order_id="wo_1")
    assert (t.method, t.host, t.used, t.order_id) == ("POST", "h.example", False, "wo_1")
    assert token_matches(t, d, "wo_1")
    other = describe_request({"method": "POST", "url": "https://h.example/a1/bzz"})
    assert not token_matches(t, other, "wo_1")


def test_token_for_path_does_not_pass_same_path_with_query():
    plain = describe_request({"method": "POST", "url": "https://h.example/x"})
    t = ApprovalToken.from_descriptor("ask_1", plain, order_id="wo_1")
    sneaky = describe_request({"method": "POST", "url": "https://h.example/x?action=delete"})
    assert token_matches(t, plain, "wo_1")
    assert not token_matches(t, sneaky, "wo_1")
    assert sneaky["fingerprint"] != plain["fingerprint"]
    q = ApprovalToken.from_descriptor("ask_2", sneaky, order_id="wo_1")
    assert token_matches(q, sneaky, "wo_1") and not token_matches(q, plain, "wo_1")
