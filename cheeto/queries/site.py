"""Site lookup helpers and per-site dependent enumeration."""

from __future__ import annotations

from beanie import PydanticObjectId
from beanie.operators import Or

from ..models.base import link_target_id
from ..models.site import Site
from ..models.slurm import (
    SlurmAccount,
    SlurmAssociation,
    SlurmPartition,
    SlurmQOS,
)


async def find_site_by_name(name: str) -> Site | None:
    return await Site.find_one(Site.name == name)


async def find_site(value: str) -> Site | None:
    """Resolve a site by its canonical `name`, its `fqdn`, or any of its
    `aliases`. `Site.aliases == value` matches documents whose aliases array
    contains `value`. Used by the daemon API, where the `{site}` path segment
    may be any of these."""
    return await Site.find_one(
        Or(Site.name == value, Site.fqdn == value, Site.aliases == value)
    )


def _build_site_linked_models() -> list[tuple[str, type]]:
    """(label, model) for every collection holding a `site: Link[Site]` — the
    single source of truth for both `count_site_dependents` and the cascade
    delete in `RemoveSite`, so the two never drift. Beanie has no reverse
    cascade, so removing a site must delete each of these explicitly.

    Order is delete-safe: referencing collections come before the ones they
    reference (storage → static_mounts → storage_volumes). StorageVolume's
    `parent` self-link is fine — parents and children die in the same bulk
    delete."""
    from ..models.storage import AutomountMap, StaticMount, Storage, StorageVolume
    from ..models.user_site_info import UserSiteInfo
    from ..models.group_membership import GroupMembership
    from ..models.hippo import HippoEvent
    return [
        ('user_site_info', UserSiteInfo),
        ('group_membership', GroupMembership),
        ('slurm_associations', SlurmAssociation),
        ('slurm_qos', SlurmQOS),
        ('slurm_partitions', SlurmPartition),
        ('slurm_accounts', SlurmAccount),
        ('storage', Storage),
        ('static_mounts', StaticMount),
        ('storage_volumes', StorageVolume),
        ('automount_maps', AutomountMap),
        ('hippo_events', HippoEvent),
    ]


SITE_LINKED_MODELS: list[tuple[str, type]] = _build_site_linked_models()


async def site_alloc_ids(site: Site) -> list[PydanticObjectId]:
    """Ids of every `SlurmAllocation` owned by the site's QOSes. Allocations
    have no `site` field, so they must be collected via their owning QOS
    before the QOS records are deleted."""
    qoses = await SlurmQOS.find(SlurmQOS.site.id == site.id).to_list()
    ids: list[PydanticObjectId] = []
    for qos in qoses:
        for bucket in (qos.group_limits, qos.user_limits, qos.job_limits):
            ids.extend(
                tid for link in bucket
                if (tid := link_target_id(link)) is not None
            )
    return ids


async def count_site_dependents(site: Site) -> dict[str, int]:
    """Per-collection count of documents that would be cascade-deleted with
    `site` (plus owned slurm allocations). The Site itself is not counted."""
    counts: dict[str, int] = {}
    for label, model in SITE_LINKED_MODELS:
        counts[label] = await model.find(model.site.id == site.id).count()
    counts['slurm_allocations'] = len(set(await site_alloc_ids(site)))
    return counts
