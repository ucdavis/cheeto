from __future__ import annotations

from beanie import Link

from .base import BaseDocument, Expirable
from .site import Site


class SiteAssociation(BaseDocument, Expirable):
    """Abstract base for per-site association edges.

    A `SiteAssociation` records that some subject (a user, typically) stands
    in a relationship to some object *at a particular site*. v1 stored these
    relationships per-site (e.g. `SiteGroup._members`); the first v2 cut
    collapsed them to global links, and this base reintroduces the per-site
    scoping that requirements now demand again.

    This class is **abstract**: it carries the shared `site` link, the
    `Expirable` timestamps, and shared query helpers, but defines no
    `Settings.name` and is never registered with beanie. Each concrete
    subclass (e.g. `GroupMembership`) is its own collection with its own
    concrete-typed links and correctly-scoped indexes. We deliberately avoid
    a single polymorphic collection: beanie 2.x cannot express
    `Link[Union[...]]`, so a shared collection would force untyped links and
    a conflated index. Per-collection inheritance gives us shared behavior
    without those costs; the tradeoff is no single cross-type
    `SiteAssociation.find()`, which queries don't need in practice.
    """

    site: Link[Site]

    @classmethod
    def at_site(cls, site: Site):
        """A find query for every association of this concrete type at `site`."""
        return cls.find(cls.site.id == site.id)
