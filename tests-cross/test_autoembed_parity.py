"""
Autoembedding configuration parity.

The bug these tests exist for: the PyO3 constructor simply never grew an
``auto_embed`` parameter, so every binding *except* Python could configure
on-device embedding.  Nothing failed — ``Collection(..., auto_embed=...)``
raised ``TypeError`` as if the caller had made a typo, and the pure-Python
backend had no counterpart to compare against.

So the contract asserted here is the one that was missing: **every backend
accepts the parameter**, and a backend that cannot honour it says so in a way
the caller can catch, rather than looking like a bad keyword argument.

Nothing here loads a model — that would mean a ~130 MB download.  These cover
the config path up to the point where the model is resolved.
"""

import inspect

import pytest

from moofile.errors import MooFileError

# An unresolvable registry id: far enough through the config path to prove
# every key was accepted, without downloading anything.
MODEL = "definitely-not-a-real-model"


def _config(**overrides):
    cfg = {"model": MODEL, "target": "embedding", "dims": 1024, "precision": "int8"}
    cfg.update(overrides)
    return {"content": cfg}


def test_constructor_accepts_auto_embed(make_collection):
    """A portable auto_embed block must never look like a typo.

    Both backends take the keyword.  The Rust backend gets as far as opening
    the model file (and fails there, because the path is bogus); the pure
    Python backend refuses up front.  Either is fine — TypeError is not.
    """
    with pytest.raises((MooFileError, NotImplementedError)) as exc:
        make_collection(vector_indexes={"embedding": 1024}, auto_embed=_config())

    assert not isinstance(exc.value, TypeError), (
        "auto_embed was rejected as an unknown keyword argument — the backend "
        "is missing the parameter entirely, not declining to honour it"
    )


def test_auto_embed_signature_parity():
    """Both Collection implementations expose the same constructor keywords."""
    import moofile
    from moofile.collection import Collection as PyCollection

    if not getattr(moofile, "_NATIVE_LOADED", False):
        pytest.skip("native extension not built")
    from moofile._rust_adapter import Collection as RustCollection

    py_params = set(inspect.signature(PyCollection.__init__).parameters)
    rust_params = set(inspect.signature(RustCollection.__init__).parameters)
    assert py_params == rust_params, (
        f"constructor keywords diverge: python-only={py_params - rust_params}, "
        f"rust-only={rust_params - py_params}"
    )


def test_semantic_without_config_is_catchable(make_collection):
    """semantic() on a collection with no autoembed config fails cleanly."""
    db = make_collection(vector_indexes={"embedding": 4})
    try:
        db.insert({"content": "hello", "embedding": [1.0, 0.0, 0.0, 0.0]})
        with pytest.raises((MooFileError, NotImplementedError)):
            db.find({}).semantic("content", "hello", 5).to_list()
    finally:
        db.close()


# --- Rust-only: config validation happens inside the native constructor ---


@pytest.fixture
def rust_collection(make_collection, backend):
    if backend != "rust":
        pytest.skip("config validation is native-side only")
    return make_collection


@pytest.mark.parametrize(
    "config, expected_error, match",
    [
        (_config(precision="fp16"), ValueError, "unknown precision"),
        ({"content": {"model": MODEL, "precison": "int8"}}, ValueError, "unknown key"),
        ({"content": {"target": "embedding"}}, ValueError, "'model' is required"),
        ({"content": MODEL}, TypeError, "must be a dict"),
    ],
)
def test_config_errors_are_specific(rust_collection, config, expected_error, match):
    """A malformed auto_embed block names what is wrong with it.

    Unknown keys are rejected rather than ignored: silently dropping
    ``precison`` would leave the vectors at f32 and quadruple stored size.
    """
    with pytest.raises(expected_error, match=match):
        rust_collection(vector_indexes={"embedding": 1024}, auto_embed=config)


def test_unknown_model_is_moofile_error(rust_collection):
    """An unresolvable model surfaces as MooFileError, not a bare RuntimeError."""
    with pytest.raises(MooFileError, match="unknown embedding model"):
        rust_collection(vector_indexes={"embedding": 1024}, auto_embed=_config())


def test_gguf_uri_reports_the_migration(rust_collection):
    """Pre-1.1 configs are the ones most likely to hit this path.

    A bare "unknown model" would be actively misleading for a `hf:` URI that
    used to work, so the error has to name the replacement.
    """
    config = _config(model="hf:jsonMartin/voyage-4-nano-gguf:voyage-4-nano-q8_0.gguf")
    with pytest.raises(MooFileError, match="fastembed") as exc:
        rust_collection(vector_indexes={"embedding": 1024}, auto_embed=config)
    assert "bge-small" in str(exc.value), (
        "the migration error must suggest a replacement model, got: %s" % exc.value
    )


def test_local_model_path_is_rejected_clearly(rust_collection):
    """Local ONNX models are not wired up yet; say so rather than 'unknown'."""
    config = _config(model="./models/my-model.onnx")
    with pytest.raises(MooFileError, match="local model paths"):
        rust_collection(vector_indexes={"embedding": 1024}, auto_embed=config)


def test_all_documented_keys_are_accepted(rust_collection):
    """Every key the C ABI parses is also accepted here.

    The two parsers are separate code; this is what keeps an ``auto_embed``
    block portable verbatim between Python and the other bindings.
    """
    config = _config(
        normalize=False,
        query_prefix="query: ",
        doc_prefix="doc: ",
        precision="binary",
        dims=256,
    )
    # Fails at model resolution, i.e. after every key has been accepted.
    with pytest.raises(MooFileError, match="unknown embedding model"):
        rust_collection(vector_indexes={"embedding": 256}, auto_embed=config)
