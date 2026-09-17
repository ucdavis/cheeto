"""StorageHost operations: create/edit/delete host records and the one-time
backfill that links existing volumes to hosts and restores site-level export
defaults.

A `StorageHost` is the per-`(site, hostname)` record that carries host-tier
storage defaults (NFS export config, ZFS path templates). Volumes reference
it through `StorageVolume.storage_host` while `StorageVolume.host` stays the
concrete hostname string. See `models/host.py` for the polymorphism rules
and `queries/storage.py::effective_nfs_export` for precedence.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from beanie.operators import In, Set, Unset
from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession

from ..models.base import link_target_id
from ..models.host import StorageHost, validate_hostname
from ..models.site import Site
from ..models.storage import NFSExportConfig, StorageVolume
from ..models.user import User
from ..queries.storage import (
    effective_nfs_export,
    find_storage_host,
    list_site_storage_hosts,
)
from .base import UNSET, Operation
from .storage import _find_site, _normalize_ranges, apply_storage_defaults


async def _find_storage_host_or_raise(site: Site, hostname: str) -> StorageHost:
    host = await find_storage_host(site, hostname)
    if host is None:
        raise ValueError(
            f'StorageHost {hostname!r} does not exist on {site.name}'
        )
    return host


class CreateStorageHost(Operation):
    """Create a StorageHost record at a site, optionally with its host-tier
    defaults. Identity is `(site, hostname)`; the same NAS on two sites is
    two records (per-site defaults differ)."""

    op_name = 'create_storage_host'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        site_name: str,
        hostname: str,
        export_options: str | None = None,
        export_ranges: list[str] | None = None,
        zfs_path_templates: dict[str, str] | None = None,
    ) -> None:
        super().__init__(client, author)
        self.site_name = site_name
        self.hostname = validate_hostname(hostname)
        self.export_options = export_options
        self.export_ranges = export_ranges
        self.zfs_path_templates = dict(zfs_path_templates or {})

    async def execute(self, session: AsyncClientSession) -> StorageHost:
        site = await _find_site(self.site_name)
        if await find_storage_host(site, self.hostname) is not None:
            raise ValueError(
                f'StorageHost {self.hostname} already exists on {self.site_name}'
            )
        host = StorageHost(hostname=self.hostname, site=site)
        apply_storage_defaults(
            host,
            export_options=(
                self.export_options if self.export_options is not None
                else UNSET
            ),
            export_ranges=(
                self.export_ranges if self.export_ranges is not None
                else UNSET
            ),
            set_templates=self.zfs_path_templates or None,
        )
        await host.insert(session=session)
        self._host = host
        return host

    def describe(self) -> dict[str, Any]:
        return {
            'site': self.site_name,
            'hostname': self.hostname,
            'export_options': self.export_options,
            'export_ranges': (
                _normalize_ranges(self.export_ranges)
                if self.export_ranges is not None else None
            ),
            'zfs_path_templates': self.zfs_path_templates or None,
        }


class EditStorageHost(Operation):
    """Edit a StorageHost's defaults. `UNSET` kwargs are left alone;
    `clear_nfs_export` nulls the export config; `zfs_path_templates` /
    `clear_zfs_path_templates` add and remove ZFS path templates by
    category (same kwarg names as `SetSiteStorageDefaults`, so the CLI
    shares one flag group). The hostname itself is immutable here:
    `StorageVolume.host` strings denormalize it, so a rename needs its own
    cascading operation."""

    op_name = 'edit_storage_host'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        site_name: str,
        hostname: str,
        export_options: Any = UNSET,
        export_ranges: Any = UNSET,
        clear_nfs_export: bool = False,
        zfs_path_templates: dict[str, str] | None = None,
        clear_zfs_path_templates: list[str] | None = None,
    ) -> None:
        super().__init__(client, author)
        if (
            export_options is UNSET and export_ranges is UNSET
            and not clear_nfs_export and not zfs_path_templates
            and not clear_zfs_path_templates
        ):
            raise ValueError('Nothing to edit; pass at least one option')
        self.site_name = site_name
        self.hostname = hostname
        self.export_options = export_options
        self.export_ranges = export_ranges
        self.clear_nfs_export = clear_nfs_export
        self.zfs_path_templates = dict(zfs_path_templates or {})
        self.clear_zfs_path_templates = list(clear_zfs_path_templates or [])
        self._changes: dict[str, Any] = {}

    async def execute(self, session: AsyncClientSession) -> StorageHost:
        site = await _find_site(self.site_name)
        host = await _find_storage_host_or_raise(site, self.hostname)
        self._changes = apply_storage_defaults(
            host,
            export_options=self.export_options,
            export_ranges=self.export_ranges,
            clear_nfs_export=self.clear_nfs_export,
            set_templates=self.zfs_path_templates or None,
            unset_templates=self.clear_zfs_path_templates or None,
        )
        await host.save(session=session)
        self._host = host
        return host

    def describe(self) -> dict[str, Any]:
        return {
            'site': self.site_name,
            'hostname': self.hostname,
            **self._changes,
        }


class DeleteStorageHost(Operation):
    """Delete a StorageHost record. Refused while any volume at the site
    still names the host (by `host` string, which also covers unlinked
    pre-backfill rows)."""

    op_name = 'delete_storage_host'

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        site_name: str,
        hostname: str,
    ) -> None:
        super().__init__(client, author)
        self.site_name = site_name
        self.hostname = hostname

    async def execute(self, session: AsyncClientSession) -> None:
        site = await _find_site(self.site_name)
        host = await _find_storage_host_or_raise(site, self.hostname)
        n = await StorageVolume.find(
            StorageVolume.site.id == site.id,
            StorageVolume.host == self.hostname,
        ).count()
        if n:
            raise ValueError(
                f'StorageHost {self.hostname} on {self.site_name} is '
                f'referenced by {n} volume(s); move or delete them first'
            )
        await host.delete(session=session)

    def describe(self) -> dict[str, Any]:
        return {'site': self.site_name, 'hostname': self.hostname}


def _export_key(cfg: NFSExportConfig) -> tuple[str, frozenset[str]]:
    # Order-independent identity: v1's range union produced differently
    # ordered copies of the same config.
    return cfg.export_options, frozenset(cfg.export_ranges)


class BackfillStorageHosts(Operation):
    """One-time (idempotent) backfill after the StorageHost model landed:

      1. get-or-create a StorageHost for every distinct `(site, host)` on
         `storage_volumes`;
      2. link every volume's `storage_host` to its host;
      3. `seed_site_defaults`: when every exported volume at a site shares
         one export config and the site has none, copy it to
         `Site.storage.nfs_export` (mixed sites are logged and skipped);
      4. `dedupe_exports` (after seeding): null each volume's own
         `nfs_export` when it equals the host/site default, so future default
         changes reach it. Refused for a site with no default at any tier.

    Steps 2 and 4 use query-level `update_many` per `(site, host)` group,
    which skips the document `after_event` hooks on purpose: neither `host`
    nor `host_path` changes, so the Storages backed by these volumes must
    NOT be marked LDAP-dirty (exports feed puppet, not LDAP). Bare session
    (`transactional=False`): a whole-collection pass would exceed the
    server's transaction lifetime, and every step is independently
    idempotent. `dry_run` counts without writing; the CLI passes
    `skip_history=dry_run` so a dry run leaves no History row either.
    """

    op_name = 'backfill_storage_hosts'
    transactional = False

    def __init__(
        self,
        client: AsyncMongoClient,
        author: User | None,
        *,
        site_name: str | None = None,
        seed_site_defaults: bool = False,
        dedupe_exports: bool = False,
        dry_run: bool = False,
    ) -> None:
        super().__init__(client, author)
        self.site_name = site_name
        self.seed_site_defaults = seed_site_defaults
        self.dedupe_exports = dedupe_exports
        self.dry_run = dry_run
        self.sites_processed = 0
        self.hosts_created = 0
        self.volumes_linked = 0
        self.sites_seeded = 0
        self.sites_skipped_mixed = 0
        self.exports_deduped = 0

    async def _link_volumes(
        self, session, site: Site, volumes: list[StorageVolume],
        hosts: dict[str, StorageHost],
    ) -> None:
        by_host: dict[str, list] = {}
        for v in volumes:
            host = hosts.get(v.host)
            target_id = host.id if host is not None else None
            if host is not None and link_target_id(v.storage_host) == target_id:
                continue
            by_host.setdefault(v.host, []).append(v.id)
        now = datetime.now(timezone.utc)
        for hostname, ids in sorted(by_host.items()):
            self.volumes_linked += len(ids)
            host = hosts.get(hostname)
            if self.dry_run or host is None:
                # host is None only in dry_run (creation was skipped)
                continue
            await StorageVolume.find(
                In(StorageVolume.id, ids),
            ).update_many(
                Set({
                    StorageVolume.storage_host: host.to_ref(),
                    StorageVolume.updated_at: now,
                }),
                session=session,
            )
            self.logger.info(
                'Linked %d volume(s) on %s to StorageHost %s',
                len(ids), site.name, hostname,
            )

    async def _seed_site(
        self, session, site: Site, volumes: list[StorageVolume],
    ) -> None:
        if site.storage.nfs_export is not None:
            self.logger.info(
                '%s already has a site export default; not seeding', site.name,
            )
            return
        configs: dict[tuple, tuple[NFSExportConfig, int]] = {}
        for v in volumes:
            if v.nfs_export is None:
                continue
            key = _export_key(v.nfs_export)
            cfg, n = configs.get(key, (v.nfs_export, 0))
            configs[key] = (cfg, n + 1)
        if not configs:
            self.logger.info(
                '%s: no volume carries an export config; nothing to seed',
                site.name,
            )
            return
        if len(configs) > 1:
            self.sites_skipped_mixed += 1
            self.logger.warning(
                '%s: %d distinct export configs across volumes; not seeding '
                '(set the site default by hand):', site.name, len(configs),
            )
            for cfg, n in sorted(configs.values(), key=lambda c: -c[1]):
                self.logger.warning(
                    '  %d volume(s): options=%r ranges=%r',
                    n, cfg.export_options, sorted(cfg.export_ranges),
                )
            return
        (cfg, n), = configs.values()
        site.storage.nfs_export = NFSExportConfig(
            export_options=cfg.export_options,
            export_ranges=_normalize_ranges(cfg.export_ranges),
        )
        self.sites_seeded += 1
        self.logger.info(
            '%s: seeding site export default from %d volume(s): options=%r '
            'ranges=%r', site.name, n, cfg.export_options,
            site.storage.nfs_export.export_ranges,
        )
        if not self.dry_run:
            await site.save(session=session)

    async def _dedupe_site(
        self, session, site: Site, volumes: list[StorageVolume],
        hosts: dict[str, StorageHost],
    ) -> None:
        if site.storage.nfs_export is None and not any(
            h.nfs_export is not None for h in hosts.values()
        ):
            raise ValueError(
                f'{site.name} has no site- or host-level export default; '
                f'run with --seed-site-defaults or set one before '
                f'--dedupe-exports'
            )
        redundant: list = []
        for v in volumes:
            if v.nfs_export is None:
                continue
            default, _levels = effective_nfs_export(
                volume=None, host=hosts.get(v.host), site=site,
            )
            if default is None:
                continue
            if _export_key(default) == _export_key(v.nfs_export):
                redundant.append(v.id)
        self.exports_deduped += len(redundant)
        if redundant:
            self.logger.info(
                '%s: %d volume export config(s) equal their default; clearing',
                site.name, len(redundant),
            )
        if redundant and not self.dry_run:
            await StorageVolume.find(
                In(StorageVolume.id, redundant),
            ).update_many(
                Unset({StorageVolume.nfs_export: ''}),
                session=session,
            )

    async def execute(self, session: AsyncClientSession) -> dict[str, Any]:
        if self.site_name is not None:
            sites = [await _find_site(self.site_name)]
        else:
            sites = await Site.find_all().sort('+name').to_list()

        for site in sites:
            volumes = await StorageVolume.find(
                StorageVolume.site.id == site.id,
            ).to_list()
            hosts = {h.hostname: h for h in await list_site_storage_hosts(site)}

            for hostname in sorted({v.host for v in volumes}):
                if hostname in hosts:
                    continue
                self.hosts_created += 1
                self.logger.info(
                    'Creating StorageHost %s on %s', hostname, site.name,
                )
                if self.dry_run:
                    continue
                host = StorageHost(hostname=hostname, site=site)
                await host.insert(session=session)
                hosts[hostname] = host

            await self._link_volumes(session, site, volumes, hosts)
            if self.seed_site_defaults:
                await self._seed_site(session, site, volumes)
            if self.dedupe_exports:
                await self._dedupe_site(session, site, volumes, hosts)
            self.sites_processed += 1

        self.logger.info(
            'BackfillStorageHosts done: sites=%d hosts_created=%d '
            'volumes_linked=%d sites_seeded=%d exports_deduped=%d dry_run=%s',
            self.sites_processed, self.hosts_created, self.volumes_linked,
            self.sites_seeded, self.exports_deduped, self.dry_run,
        )
        return self.describe()

    def describe(self) -> dict[str, Any]:
        return {
            'site': self.site_name,
            'dry_run': self.dry_run,
            'sites_processed': self.sites_processed,
            'hosts_created': self.hosts_created,
            'volumes_linked': self.volumes_linked,
            'sites_seeded': self.sites_seeded,
            'sites_skipped_mixed': self.sites_skipped_mixed,
            'exports_deduped': self.exports_deduped,
        }
