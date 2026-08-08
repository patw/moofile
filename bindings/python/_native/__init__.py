# When maturin installs the native .so with abi3, Python finds
# moofile/_native.abi3.so and imports it directly — this __init__.py
# is never reached.  If you are seeing this file execute, Python fell
# through to the directory package because the native extension could
# not be loaded (missing, wrong architecture, or incompatible ABI).
# Do not attempt a relative import here: it would look for
# moofile._native._native, which does not exist in the abi3 layout,
# and the resulting ImportError would obscure the real problem.
#
# Just let the import fail cleanly — moofile/__init__.py will catch it
# and fall back to the pure-Python implementation.
