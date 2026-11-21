"""Routers of the read-only REST API, one module per resource. `create_api`
(`cheeto/daemon/api.py`) mounts each `router` under the configured prefix."""

# FastAPI resolves route signatures through each module's namespace; the
# marker covers every module in this package. See the lazy-import rules in
# CLAUDE.md.
__eager_imports__ = True
