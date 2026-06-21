# Porting Status Report: Old `db`/`hippo` CLI → New `ng`/beanie stack

_Generated 2026-05-14 · supersedes [porting-status-2026-04-22.md](./porting-status-2026-04-22.md)_

## Executive summary

Three major gaps from the previous report have closed: **IAM sync, LDAP sync, and v1 LDAP cutover are done.** The async stack now owns user/group provisioning end-to-end through HiPPO ingest → beanie persistence → IAM bookkeeping → LDAP projection, with `ng user list`/`show` rounding out the operator surface. The v1 ldap3 stack (`cheeto/ldap.py`, `cheeto/database/ldap.py`, `db site to-ldap`, `db site check-ldap`, `ldap-new-cluster.sh`, the `ldap3` dependency) has been deleted; v1 LDAP code is no longer in the tree.

**Schema redesign**: access types and user statuses are now first-class beanie documents (`AccessGroup`, `StatusGroup`) sharing the polymorphic `groups` collection with regular `Group`. `User.access` / `User.status` and `UserSiteInfo.access` / `UserSiteInfo.status` are typed `Link[…]`. The shorthand→LDAP-name mapping that used to live on `LDAPConfig.user_access_groups` / `user_status_groups` now lives on the records themselves.

**Access override semantics**: `UserSiteInfo.access` no longer unions with `User.access` — a non-empty per-site list **overrides** the global list. Empty falls through. Same intent for `UserSiteInfo.status` (single-link override). Migration folds v1 global ∪ per-site accesses into the new global, leaves `UserSiteInfo.access` empty.

**Still unported (down from previous report):** Puppet export and import, `cheeto slurm sync` against beanie, bulk class provisioning, storage operations beyond `create home`, broader show/list filters. See the gap list at the bottom.

`cheeto ng` now exposes ~90 subcommands across 11 top-level domains: `site`, `user`, `group`, `slurm`, `storage`, `history`, `migrate`, `hippo`, **`iam`**, **`ldap`**.

---

## What landed since the previous report

### `ng iam` — IAM sync stack ✅ (was the previous report's #3 gap)

| Old | New | Status |
|---|---|---|
| `db iam sync` (bulk) | `ng iam sync-all` | ✅ |
| `db iam new-user(s)` | folded into `ng iam sync` / `ng iam sync-all` | ✅ |
| (none) | `ng iam show <user>` | ✅ (new) — local IAM bookkeeping for one user, no API call |
| (none) | `ng iam reap` | ✅ (new) — flips `offboarding` users whose `expires_at` has passed to `inactive` |

Architecture: `cheeto/iam_async.py` (httpx wrapper around the UCD IAM API with cached resolution + transient-error classification), `cheeto/operations/iam.py` (state machine: `hit / hit_restored / miss_first / miss_within_grace / miss_offboarding / miss_already_expiring / miss_never_seen / no_iam_id`), `cheeto/cmds/ng/iam.py` (CLI). All writes go through `History`. Naive-UTC normalization throughout. Default grace = 3 days; offboarding expiry computed as `first_missing_at + (grace_days + expiry_offset_days)`. `User.iam: UCDIAMInfo` is the storage target; `ng user show` now renders IAM state + projected expiry/grace.

### `ng ldap` — async LDAP sync ✅ (was the previous report's #2 gap) + v1 cutover

| Old | New | Status |
|---|---|---|
| `db site to-ldap` | `ng ldap sync-site` | ✅ — and supports `--scope users,groups,automounts,prune`, `--concurrency`, `--dry-run`, `--max-deletions` |
| `db site check-ldap` | `ng ldap show site` | ✅ — read-only DN existence probe |
| (none) | `ng ldap bootstrap` | ✅ (new) — replaces `ldap-new-cluster.sh`; idempotent OU tree + automount maps + special access/status group entries |
| (none) | `ng ldap sync-user` / `sync-group` | ✅ (new) — one-record sync |
| (none) | `ng ldap prune-site` | ✅ (new) — orphan deletion with `max_deletions` cap and dry-run preview |
| (none) | `ng ldap clear-tree` | ✅ (new) — wipe under searchbase (excluding `ou=Services`); dry-run preview, configurable cap |

Architecture: `cheeto/ldap_async.py` is a bonsai-based async manager (`AIOConnectionPool`, retry-on-stale-connection per the bonsai gotcha, semantic exception hierarchy `LDAPNotFound` / `LDAPAlreadyExists` / `LDAPInvalidUser` / `LDAPCommitFailed` / `LDAPTransientError` / `LDAPPruneAborted`). LDAP attribute names (`uid`, `mail`, `memberUid`, `cn`, `displayName`, `sn`, `homeDirectory`, `loginShell`, `sshPublicKey`, `userPassword`, `shadowExpire`) are written inline in the mapping helpers — no config-driven indirection. `shadowAccount` is part of the user objectClass set; `User.expires_at` projects as `shadowExpire` (days-since-epoch per RFC 2307) and round-trips on read. Internal helpers (`_add_entry`, `_modify_attrs`, `_patch_member_set`, `_ensure_entry`, `_delete_dn_idempotent`) absorb the bonsai search/add/modify/delete boilerplate.

Operations layer: `BootstrapLDAPSite`, `SyncUserToLDAP`, `SyncGroupToLDAP`, `SyncSiteAutomounts`, `PruneSiteLDAP`, `ClearLDAPTree`, `SyncSiteLDAP` (the driver). Driver loop iterates users via a `LDAPSyncResult(name, outcome, extra)` tally; transient errors are counted as `transient_error` and skipped so one bad user doesn't cascade.

**v1 deleted in the cutover commit (`1f87022`)**: `cheeto/ldap.py`, `cheeto/database/ldap.py`, `cheeto/tests/test_ldap.py`, `ldap-new-cluster.sh`, `db site to-ldap`, `db site check-ldap`, `ldap3` dependency, the `user_attrs` / `group_attrs` / `user_classes` / `group_classes` / `user_status_groups` / `user_access_groups` fields on `LDAPConfig`. `python-ldap` and `gssapi` remain in `pyproject.toml` (transitive use by bonsai's GSSAPI auth).

### Schema redesign: `AccessGroup` / `StatusGroup` as polymorphic Documents

`Group` now has `is_root=True`; `AccessGroup(Group)` adds `access_name: str` (shorthand) and `StatusGroup(Group)` adds `status_name: str`. The inherited `Group.name` IS the LDAP groupname. Special group GIDs start at `MIN_SPECIAL_GID` (`3_333_333_000`), separate from the system UID range.

`User.access: list[Link[AccessGroup]]`, `User.status: Link[StatusGroup]`. Same shape on `UserSiteInfo`. Validation of "is this a known access type?" moves from `ACCESS_TYPES`/`USER_STATUSES` constants to Link existence — adding a new access type now means inserting an `AccessGroup` record, not editing code or config.

Bootstrap: `cheeto ng group seed-access-status` seeds the default `AccessGroup`/`StatusGroup` records into beanie. `cheeto ng ldap bootstrap` reads them and creates the LDAP-side entries. Decoupled status from login-ssh: `StatusGroup(status_name='active', name='active-users')` is its own LDAP group, distinct from `AccessGroup(access_name='login-ssh', name='login-ssh-users')`. PAM/SSH must intersect both to gate login.

Migration path: `ng migrate access-status-groups` reads v1 `GlobalGroup` records whose names match the v1 config's access/status names, creates the corresponding `AccessGroup`/`StatusGroup` records in beanie preserving `gid` (legacy `_class_id=None` rows are upgraded in place, gid renumbered into the special band — safe because special groups have no file ownership). `ng migrate users` then resolves v1 access/status shorthands to Links.

### Access semantics: override, not union

Old (v1) and the prior v2 implementation: `effective_access = user.access ∪ usi.access`. This made "subtract one access at one site" impossible to express, since per-site sets could only add. New contract: **non-empty `usi.access` completely overrides `user.access` at that site; empty falls through to `user.access`.** Same intent for status (single link).

Implementation: `effective_access_links(user, usi)` in `cheeto/queries/user.py`. `SyncUserToLDAP`, `_ids_with_access` (the `--access` filter on `ng user list`), and `ng user show` all go through it. Migration: `MigrateUser` folds v1 global plus every per-site `_access` into the new global `User.access` and leaves `UserSiteInfo.access` empty (override is opt-in post-migration via `AddUserAccess --site SITE`).

### Migrations

| Scope | Status |
|---|---|
| `ng migrate access-status-groups` | ✅ (new) — must run before users; idempotent + legacy `_class_id=None` upgrade |
| `ng migrate users` access-fold-to-global | ✅ — clean-slate per-site = empty |
| `ng migrate --drop` semantics | ✅ — actually drops the polymorphic collection + re-init_beanie (was: `find_all().delete()`); typed-confirmation prompt |

### `ng user` surface

| New | Status |
|---|---|
| `ng user show --uid` / `--email` | ✅ (new) — single-user lookup by any of name/uid/email |
| `ng user show --site <site>` | ✅ — renders effective access (with override-active vs fall-through indication), status, slurm, IAM bookkeeping |
| `ng user list --status / --access / --type / --site / --group / --operator AND\|OR` | ✅ (new) — multi-filter listing; `_ids_with_status` and `_ids_with_access` accept optional sitename to union in per-site overrides |
| `ng user add ssh-key --key / --key-file [--replace]` | ✅ (new) |
| `ng user remove ssh-key` (interactive when >1) | ✅ (new) |
| `SetUserStatus` clears `expires_at` on offboarding→active | ✅ (new) — both global and site-scoped paths; prevents next IAM sync from re-offboarding a just-reactivated user |

### Bookkeeping / quality

- `ng slurm allocation` show / edit accept `expires_at` / `provisioned_at`; `ng slurm partition show`; `ng slurm allocation show` rendering of unlimited TRES no longer prints `Nonec/Noneg`.
- N+1 fix in `ng history`.
- `Expirable` mixin on `User`, `UserSiteInfo`, `SshKey`, `SlurmAllocation`.
- `SshKey` is now its own `Document` (was an embedded field).
- `--no-resolve-author` debug flag for sessions where the author User model has drifted underfoot during development.

---

## Status by domain

Legend: ✅ ported · 🟡 partial/differs · ❌ not yet ported · ➖ intentionally dropped (deprecated concept in new model)

### Site

| Old | New | Status | Notes |
|---|---|---|---|
| `db site new` | `ng site new` | ✅ | |
| `db site show` | `ng user/group show --site` only exposes site-scoped views | ❌ | No `ng site show` — can't list default home / global groups / FQDN of a site |
| `db site list` | — | ❌ | No way to list sites |
| `db site add-global-slurm` | — | ➖ | New model has no `Site.global_slurmers` — groups are global now |
| `db site check-ldap` | `ng ldap show site` | ✅ | |
| `db site to-ldap` | `ng ldap sync-site` | ✅ | Richer: `--scope`, `--concurrency`, `--dry-run`, `--max-deletions` |
| `db site to-puppet` | — | ❌ | Puppet export not ported |
| `db site sync-old-puppet` / `sync-new-puppet` | — | ❌ | Puppet repo sync not ported |
| `db site load` (puppet import) | — | ❌ | Puppet import not ported |
| `db site root-key` | — | ❌ | Admin SSH key export not ported |
| `db site to-sympa` | — | ❌ | Sympa email-list export not ported |

### User

| Old | New | Status | Notes |
|---|---|---|---|
| `db user show` | `ng user show` | ✅ | Single-user lookup by `--user` / `--uid` / `--email`; site-scope view incl. effective access, slurm, IAM state, projected expiry. Text search via UserSearch ngram index is still missing — see `ng user list` for filtered queries. |
| (none) | `ng user list` | ✅ (new) | Multi-filter (status/access/type/site/group) with `--operator AND\|OR`, `--long`, `--yaml`, `--limit`. |
| `db user new system / shared` | `ng user new system / shared` | ✅ | No `--owner` requirement on shared |
| (none) | `ng user new class / generic` | ✅ (new) | Class is single-user; bulk variant still pending |
| `db user set status / shell / type` | `ng user status / shell / type` | ✅ | `status` now clears `expires_at` when transitioning offboarding→active |
| `db user set password` | `ng user password` | 🟡 | Always generates and prints. No way to set a specific pre-hashed password. |
| `db user add/remove access` | `ng user add/remove access` | ✅ | Honors override-not-union semantics when `--site` is passed |
| `db user add/remove site` | `ng user add/remove site` | 🟡 | No `--create-storage` option |
| (none) | `ng user add/remove ssh-key` | ✅ (new) | `add` supports `--replace`; `remove` is interactive when >1 |
| `db user generate-passwords` | — | ❌ | Bulk CSV generator not ported |
| `db user groups` | `ng user show --site` shows memberships | 🟡 | No cross-site membership dump |
| `db user index` | — | ➖ | UserSearch ngram index not brought forward |
| (none) | `ng user comment` | ✅ (new) | |

### Group

| Old | New | Status | Notes |
|---|---|---|---|
| `db group show` | `ng group show` | 🟡 | Single-group lookup works incl. site-scoped Slurm view. Old also had `--short` toggle and deeper partition/QOS rendering. |
| `db group list` | — | ❌ | No bulk list by type |
| `db group new system / lab / sponsor` | `ng group new system / lab` + `ng group from-sponsor` | ✅ | Sponsor renamed; new creates `{sponsor}grp` |
| `db group new class` | — | ❌ | **Still the critical gap**: bulk-creates class group + N students + passwords + sponsor wiring |
| `db group add/remove member/sponsor/sudoer/slurmer` | `ng group add/remove …` | ✅ | |
| `db group add/remove site` | — | ➖ | Groups are global in new model |
| (none) | `ng group seed-access-status` | ✅ (new) | Seeds the standard `AccessGroup`/`StatusGroup` records before `ng ldap bootstrap` |

### Slurm

| Old | New | Status | Notes |
|---|---|---|---|
| `db slurm new partition / qos / assoc` | `ng slurm partition new / qos new / association new` | ✅ | QOS new takes TRES via `--group/user/job-limits` → SlurmAllocations |
| `db slurm new alloc` | — | 🟡 | Old was one-shot QOS+association; new requires separate steps (`qos new` + `association new` + `allocation add`) |
| `db slurm edit qos` | `ng slurm qos edit` | 🟡 | Allocations are editable (`ng slurm allocation edit`, incl. `expires_at` / `provisioned_at`); editing QOS `priority` / `flags` / replacing limits lists still needs work |
| `db slurm remove qos / partition / assoc` | `ng slurm qos remove / partition remove / association remove` | ✅ | |
| `db slurm show qos / assoc` | `ng slurm qos show`, `ng slurm partition show`, `ng slurm allocation show`, `ng slurm association show` | ✅ | Unlimited TRES renders correctly (was "Nonec/Noneg") |
| (none) | `ng slurm allocation add / edit / show / provision` | ✅ (new) | Allocation lifecycle |

### Storage

| Old | New | Status | Notes |
|---|---|---|---|
| `db storage new home` | `ng storage new home` | 🟡 | Creation only; old also has pre-existing source re-use + site-wide defaults |
| `db storage new storage` | — | ❌ | Generic storage creation (non-home) not ported |
| `db storage new collection` | — | ➖ | New model eliminated SourceCollections |
| `db storage new automountmap` | — | ❌ | AutomountMap exists in the new model but no CLI to create one |
| `db storage edit source` | — | ❌ | Not ported |
| `db storage remove home` | — | ❌ | Not ported |
| `db storage show` | — | ❌ | Big gap — old had rich filter-by-user/group/name/collection/host/automount |
| `db storage to-puppet` | — | ❌ | Puppet export not ported |

### IAM (UC Davis IAM sync) ✅ (ported)

| Old | New | Status | Notes |
|---|---|---|---|
| `db iam sync` | `ng iam sync-all` | ✅ | Concurrency-bounded; per-user transient errors counted into tally, not fatal |
| `db iam new-user(s)` | folded into `ng iam sync` | ✅ | The state machine handles `miss_never_seen` and `miss_first` correctly |
| (none) | `ng iam sync <user>` | ✅ (new) | One-user sync |
| (none) | `ng iam show <user>` | ✅ (new) | Local-only IAM bookkeeping render; projects grace/expiry dates |
| (none) | `ng iam reap` | ✅ (new) | Flips `offboarding` users whose `expires_at` has passed to `inactive`. No IAM I/O — runs on its own schedule. |

`SyncUserIAM` is the per-user state machine. Outcomes: `hit / hit_restored / miss_first / miss_within_grace / miss_offboarding / miss_already_expiring / miss_never_seen / no_iam_id`. Resurrection from offboarding only fires when status is still `offboarding` AND `expires_at` is in the future — operator-set `inactive`/`disabled` is not auto-resurrected.

### LDAP ✅ (ported, v1 cutover complete)

| Old | New | Status | Notes |
|---|---|---|---|
| `db site to-ldap` | `ng ldap sync-site` | ✅ | Per-record ops, transient-error tally, prune phase with safety cap |
| `db site check-ldap` | `ng ldap show site` | ✅ | DN existence probe; covers tree + special groups |
| `ldap-new-cluster.sh` | `ng ldap bootstrap` | ✅ | OU tree + automount maps + special access/status groups; idempotent |
| (none) | `ng ldap sync-user / sync-group` | ✅ (new) | One-record sync |
| (none) | `ng ldap prune-site` | ✅ (new) | Orphan deletion with `--max-deletions`, `--dry-run`, `--scope`; access/status special-group LDAP entries are never pruned |
| (none) | `ng ldap clear-tree` | ✅ (new) | Wipe-and-restart for dev clusters |
| (none) | `ng ldap show user / group / site` | ✅ (new) | Read-only LDAP inspection |

Connection pool (`AIOConnectionPool`) configurable via `LDAPConfig.pool_max_connections` / `pool_idle_connections` / `request_timeout_seconds`. SIMPLE auth is wired; GSSAPI fields land in config but are not yet exercised.

### HiPPO ✅ (unchanged from previous report)

All five handlers ported. `_ensure_home_storage` in `CreateAccountHandler` is **still a no-op** pending site-default host info in the beanie `Site` model.

### History / audit

| Old | New | Status |
|---|---|---|
| Timestamped comment-string audit on user | `History` collection for all write ops + `ng history --user --op --limit` | ✅ (new capability exceeds old) |

N+1 fix landed since the previous report.

### Migrations (v1 → v2)

| Scope | Command | Status |
|---|---|---|
| Sites | `ng migrate sites` | ✅ |
| AccessGroup/StatusGroup (must run before users) | `ng migrate access-status-groups` | ✅ (new) |
| Users + UserSiteInfo | `ng migrate user[s]` | ✅ — folds v1 global ∪ per-site access into v2 global |
| Groups (members + sponsors + sudoers + slurmers) | `ng migrate groups` | ✅ — legacy `_class_id=None` upgraded in place |
| Slurm partitions / QOSes / accounts / associations | `ng migrate slurm partitions / qoses / accounts / associations` | ✅ |
| `ng migrate --drop` | drops actual collections (was: `find_all().delete()`); typed confirmation | ✅ |
| HippoEvents | — | ❌ (low value — events are a processing log, not reference data) |
| Storage | — | ❌ (storage model changed too much for a 1:1 migrate) |

### Adjacent top-level modules

- `cheeto puppet` (YAML validate / merge / postload) — pure-schema, no DB dep; **no port needed**.
- `cheeto slurm` (sync to `slurmctld`) — currently reads mongoengine; not ported against beanie. **Still pending.**
- `cheeto nocloud` (Jinja templates) — no DB dep; no port needed.
- `cheeto monitor` (IPMI) — no DB dep; no port needed.
- `cheeto ipython` — works against both ODMs; v1 LDAP-manager constructor removed since v1 is gone.

---

## Biggest gaps remaining

1. **Puppet export** (`*_to_puppet` + `db site to-puppet` + `db site sync-*-puppet`). Production artifact generation. Without this the new DB can't drive Puppet.
2. **`cheeto slurm sync` against beanie.** Reads old models, pushes to slurmctld. The runtime that actually makes slurm allocations take effect.
3. **Bulk class provisioning (`db group new class`).** The main mechanism for class onboarding — creates the group, N users, generates passwords, CSV export.
4. **Storage operations beyond `create home`.** Need create/edit/remove for group and share storages before the new storage model is useful in production.
5. **Show / list coverage holdovers.** `ng site show / site list`, `db user generate-passwords` bulk CSV, `db user groups` cross-site dump, `db group list`, `db storage show`.
6. **Home-storage creation from `CreateAccountHandler`.** Still a no-op pending site-default host info in the beanie `Site` model.
7. **`User.password` plaintext set.** `ng user password` always generates. No way to set a specific pre-hashed value.

Removed from the "biggest gaps" list since the previous report: IAM sync, LDAP sync, v1 LDAP cutover.

---

## Suggested porting order

1. **Puppet export** (`group_to_puppet`, `user_to_puppet`, `site_to_puppet`, `_storage_to_puppet`) — highest external impact, pure read path, unblocks running both stacks in parallel.
2. **`cheeto slurm sync` against beanie** — once exports are ported, make the slurm runtime reach the new DB.
3. **Bulk class provisioning** (ng replacement for `db group new class`).
4. **Site-default host info + wire home-storage creation into `CreateAccountHandler`**.
5. **Storage beyond home** (group / share create, edit, remove, show).
6. **`ng site show` + `ng site list`** — basic operator surface that's still missing.
7. **Puppet import** (can likely be deferred indefinitely — migration is one-time).
8. **List/query coverage** in `ng` show commands (text search, additional filters). Iterative.

Closed since the previous report (no longer in this list): IAM sync, LDAP sync, Slurm remove ops + edit qos.
