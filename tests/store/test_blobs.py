from datetime import timedelta


def test_prune_removes_old_blobs_only(blobs, fake_clock):
    old = blobs.put(b"a", "png")
    fake_clock.advance(timedelta(days=31))
    new = blobs.put(b"b", "png")
    assert blobs.prune(fake_clock.now(), keep_days=30) == 1
    assert not blobs.path(old).exists() and blobs.path(new).exists()


def test_put_ref_format_and_free_bytes(blobs):
    ref = blobs.put(b"zz", "png")
    assert ref.startswith("blob_") and ref.endswith(".png")
    assert blobs.path(ref).read_bytes() == b"zz"
    assert blobs.free_bytes() > 0


def test_put_rejects_unsafe_ext(blobs):
    import pytest

    for bad in ("../x", "", "a/b", "toolongext1"):
        with pytest.raises(ValueError):
            blobs.put(b"x", bad)
