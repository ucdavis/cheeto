# Configuration file for the Sphinx documentation builder.
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import importlib.metadata

# -- Project information -----------------------------------------------------

project = 'cheeto'
author = 'UC Davis HPC Core Facility'
copyright = '2023-2026, The Regents of the University of California, Davis'

# Pulled from the installed package metadata (kept in sync by
# poetry-bumpversion). Requires `cheeto` to be installed in the build env
# (`poetry install --with docs`).
release = importlib.metadata.version('cheeto')
version = '.'.join(release.split('.')[:2])

# -- General configuration ---------------------------------------------------

extensions = [
    'myst_parser',        # author docs in Markdown (MyST)
    'autoapi.extension',  # auto-generate the API reference from source
]

# MyST Markdown niceties (fenced directives, definition lists, …).
myst_enable_extensions = [
    'colon_fence',
    'deflist',
]

templates_path = ['_templates']

# The generated API clients (hippoapi/iamapi) are excluded from the API docs
# (see autoapi_ignore); silence autoapi's noise about being unable to resolve
# imports into those intentionally-undocumented modules.
suppress_warnings = ['autoapi.python_import_resolution']

# The two ad-hoc porting-status notes in docs/ are not part of the built site.
exclude_patterns = [
    '_build',
    'Thumbs.db',
    '.DS_Store',
    'porting-status-*.md',
]

# -- HTML output -------------------------------------------------------------

html_theme = 'furo'
html_static_path = ['_static']

# -- sphinx-autoapi ----------------------------------------------------------
# Static analysis (astroid) — the `cheeto` package is NOT imported at build
# time, so beanie/mongo/async/generated-client imports never run.

autoapi_type = 'python'
autoapi_dirs = ['../cheeto']
autoapi_root = 'reference/api'
# We place the API tree under Reference ourselves (see reference/index.md)
# rather than letting autoapi inject its own top-level toctree entry.
autoapi_add_toctree_entry = False
# Keep the generated pages on disk (they live under autoapi_root, which is
# gitignored). This keeps `reference/api/index` present when the Reference
# toctree resolves, including on incremental and `-W` builds.
autoapi_keep_files = True
autoapi_ignore = [
    '*/tests/*',
    '*/templates/*',  # Jinja/cloud-init templates (not importable API)
    '*/hippoapi/*',   # generated HiPPO API client
    '*/iamapi/*',     # generated UC Davis IAM API client
]
autoapi_options = [
    'members',
    'undoc-members',
    'show-inheritance',
    'show-module-summary',
]
