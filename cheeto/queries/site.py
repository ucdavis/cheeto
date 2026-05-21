"""Site lookup helpers."""

from __future__ import annotations

from ..models.site import Site


async def find_site_by_name(name: str) -> Site | None:
    return await Site.find_one(Site.name == name)
