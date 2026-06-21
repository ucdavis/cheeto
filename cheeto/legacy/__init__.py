#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# (c) The Regents of the University of California, Davis
# License: Modified BSD
#
# v1 (mongoengine) support, kept only for the v1 -> v2 data migration. It is
# gated behind the optional `legacy` extra so the default install is
# mongoengine-free. Importing the v1 models or running `cheeto ng migrate`
# without the extra raises a clear, actionable error.

_LEGACY_HINT = (
    "v1 (mongoengine) support is not installed. Install the legacy extra:\n"
    "    pip install 'cheeto[legacy]'\n"
    "  or, for development:\n"
    "    poetry install --extras legacy"
)


def require_legacy() -> None:
    """Raise a clear error unless the optional `legacy` extra (mongoengine) is
    installed. Call this at v1/migration entry points before importing the
    legacy models."""
    try:
        import mongoengine  # noqa: F401
    except ImportError as e:
        raise RuntimeError(_LEGACY_HINT) from e
