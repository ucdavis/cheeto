"""Shared storage-default building blocks.

Leaf module: it imports only `constants`, so `site.py`, `host.py`, and
`storage.py` can all import it without a cycle (`storage.py` imports `Site`,
so `site.py` must never import `storage.py`).

Two kinds of default live here:

- **NFS export config** (`NFSExportConfig`): the `/etc/exports` options and
  client ranges. Resolved per field, most specific wins, along
  `Storage` -> `StorageVolume` -> `StorageHost` -> `Site.storage`
  (see `queries/storage.py::effective_nfs_export`).
- **ZFS path templates**: per-category (`home`/`group`/`share`) `str.format`
  templates that derive a standalone volume's `host_path`, e.g.
  `/{host}/share/{name}`. Allowed fields are `ZFS_TEMPLATE_FIELDS`. Templates
  apply only to standalone volumes; children under a parent volume always
  take `parent.host_path/<name>`.

`StorageDefaults` is the embedded/mixin model both `SiteStorageSettings` and
`StorageHost` inherit, so the two tiers share one field shape and validator.
"""

from __future__ import annotations

import string
from pathlib import PurePosixPath

from pydantic import BaseModel, Field, field_validator

from ..constants import STORAGE_CATEGORIES


class NFSExportConfig(BaseModel):
    export_options: str = ''
    export_ranges: list[str] = Field(default_factory=list)


ZFS_TEMPLATE_FIELDS = frozenset({'host', 'name', 'site'})

_FORMATTER = string.Formatter()


def validate_zfs_path_template(template: str) -> str:
    """Validate one ZFS path template. Absolute path; named fields only, from
    `ZFS_TEMPLATE_FIELDS`; no positional fields, format specs, conversions,
    or attribute/index access. Raises ValueError naming the template."""
    if not template.startswith('/'):
        raise ValueError(
            f'ZFS path template {template!r} must be an absolute path'
        )
    try:
        parsed = list(_FORMATTER.parse(template))
    except ValueError as e:
        raise ValueError(f'ZFS path template {template!r}: {e}') from e
    for _literal, field, spec, conversion in parsed:
        if field is None:
            continue
        if field == '' or field.isdigit():
            raise ValueError(
                f'ZFS path template {template!r}: positional fields are not '
                f'allowed; use {{host}}, {{name}}, or {{site}}'
            )
        if spec or conversion is not None:
            raise ValueError(
                f'ZFS path template {template!r}: format specs and '
                f'conversions are not allowed'
            )
        if field not in ZFS_TEMPLATE_FIELDS:
            allowed = ', '.join(f'{{{f}}}' for f in sorted(ZFS_TEMPLATE_FIELDS))
            raise ValueError(
                f'ZFS path template {template!r}: unknown field {{{field}}}; '
                f'allowed fields are {allowed}'
            )
    return template


def render_host_path(template: str, *, host: str, name: str, site: str) -> str:
    """Render a validated template into a canonical absolute host path."""
    validate_zfs_path_template(template)
    rendered = template.format_map({'host': host, 'name': name, 'site': site})
    return str(PurePosixPath(rendered))


def validate_zfs_path_templates(templates: dict[str, str]) -> dict[str, str]:
    """Validate a category -> template map: keys must be storage categories,
    values valid templates."""
    for category, template in templates.items():
        if category not in STORAGE_CATEGORIES:
            raise ValueError(
                f'Invalid ZFS path template category {category!r}; '
                f'expected one of {", ".join(STORAGE_CATEGORIES)}'
            )
        validate_zfs_path_template(template)
    return templates


class StorageDefaults(BaseModel):
    """Storage defaults shared by the site tier (`SiteStorageSettings`) and
    the host tier (`StorageHost`). Both fields are optional: `None` / `{}`
    means "defer to the next tier"."""

    nfs_export: NFSExportConfig | None = None
    zfs_path_templates: dict[str, str] = Field(default_factory=dict)

    @field_validator('zfs_path_templates')
    @classmethod
    def _validate_zfs_path_templates(cls, v: dict[str, str]) -> dict[str, str]:
        return validate_zfs_path_templates(v)
