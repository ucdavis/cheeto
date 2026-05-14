# Porting Status Report: Old `db`/`hippo` CLI → New `ng`/beanie stack

_Generated 2026-04-22 · last updated 2026-04-22 (HiPPO port landed)_

## Executive summary

The async/beanie port has landed a **strong core** for users, groups, sites, slurm, audit history, **and HiPPO event processing** — including a full history log, the bulk of the write operations as an OO layer, migration pathways from v1, and a richer queryable `HippoEvent` model. **Puppet export, LDAP sync, IAM sync, bulk class provisioning, `cheeto slurm sync`, and most storage commands still need to be ported.** The old `cheeto db` CLI has ~54 subcommands; the new `cheeto ng` now has ~71 across all domains.

Storage is the biggest architectural gap — only `create home storage` has an operation, and none of the read / export / collection / automountmap / edit commands are ported. The new storage model is deliberately different from the old one so some old commands won't port 1:1.

---

## Status by domain

Legend: ✅ ported · 🟡 partial/differs · ❌ not yet ported · ➖ intentionally dropped (deprecated concept in new model)

### Site

| Old | New | Status | Notes |
|---|---|---|---|
| `db site new` | `ng site new` | ✅ | |
| `db site show` | `ng user/group show --site …` only exposes site-scoped views | ❌ | No `ng site show` — can't list default home / global groups / FQDN of a site |
| `db site list` | — | ❌ | No way to list sites |
| `db site add-global-slurm` | — | ➖ | New model has no `Site.global_slurmers` — groups are global now |
| `db site check-ldap` | — | ❌ | LDAP validation not ported |
| `db site to-ldap` | — | ❌ | LDAP push not ported |
| `db site to-puppet` | — | ❌ | Puppet export not ported |
| `db site sync-old-puppet` / `sync-new-puppet` | — | ❌ | Puppet repo sync not ported |
| `db site load` (puppet import) | — | ❌ | Puppet import not ported |
| `db site root-key` | — | ❌ | Admin SSH key export not ported |
| `db site to-sympa` | — | ❌ | Sympa email-list export not ported |

### User

| Old | New | Status | Notes |
|---|---|---|---|
| `db user show` | `ng user show` | 🟡 | New only does single-user lookup by `--user`. Old supported `--uid --type --access --status --find` filters, text-search via UserSearch index, `--list` summary mode, and auto-sites. Also adds `--site` for per-site scoping plus Slurm access — a new capability. |
| `db user new system` | `ng user new system` | ✅ | |
| `db user new shared` | `ng user new shared` | ✅ | No `--owner` requirement — new op doesn't link to an owner |
| (none) | `ng user new class` | ✅ (new) | Single class user; bulk variant (see group) not ported |
| (none) | `ng user new generic` | ✅ (new) | Allows explicit UID/type |
| `db user set status` | `ng user status` | ✅ | |
| `db user set shell` | `ng user shell` | ✅ | |
| `db user set type` | `ng user type` | ✅ | |
| `db user set password` | `ng user password` | 🟡 | New **always generates** and prints. Old accepted plaintext arg. This is intentional (safer) but means there's no way to set a specific pre-hashed password via ng. |
| `db user add access` | `ng user add access` | ✅ | |
| `db user remove access` | `ng user remove access` | ✅ | |
| `db user add site` | `ng user add site` | 🟡 | No `--create-storage` option (old created a home storage on add) |
| `db user remove site` | `ng user remove site` | ✅ | |
| `db user generate-passwords` | — | ❌ | Bulk CSV generator not ported |
| `db user groups` | `ng user show --site …` shows memberships | 🟡 | No cross-site membership dump / YAML export |
| `db user index` | — | ➖ | UserSearch ngram index not brought forward (no text search in ng yet) |
| (none) | `ng user comment` | ✅ (new) | Adds a comment directly (old only set via `set_user_status` reason) |

### Group

| Old | New | Status | Notes |
|---|---|---|---|
| `db group show` | `ng group show` | 🟡 | Single-group lookup works, including site-scoped Slurm view. Old also showed partitions/QOSes more deeply with `--short` toggle. |
| `db group list` | — | ❌ | No bulk list by type |
| `db group new system` | `ng group new system` | ✅ | |
| `db group new class` | — | ❌ | **Critical gap**: old bulk-creates class group + N students + passwords + sponsor wiring — the main mechanism for class provisioning |
| `db group new lab` | `ng group new lab` | ✅ | |
| `db group new sponsor` | `ng group from-sponsor` | ✅ | Renamed; new creates `{sponsor}grp` (different naming) |
| `db group add/remove member` | `ng group add/remove member` | ✅ | |
| `db group add/remove sponsor` | `ng group add/remove sponsor` | ✅ | |
| `db group add/remove sudoer` | `ng group add/remove sudoer` | ✅ | |
| `db group add/remove slurmer` | `ng group add/remove slurmer` | ✅ | |
| `db group add/remove site` | — | ➖ | Groups are global in new model |

### Slurm

| Old | New | Status | Notes |
|---|---|---|---|
| `db slurm new partition` | `ng slurm partition new` | ✅ | |
| `db slurm new qos` | `ng slurm qos new` | ✅ | New is richer — takes TRES via `--group/user/job-limits` that spawn SlurmAllocations with `--comment` |
| `db slurm new assoc` | `ng slurm association new` | ✅ | |
| `db slurm new alloc` | — | 🟡 | Old was a convenience: creates QOS + association in one shot. New has separate `qos new` + `association new` + `allocation add`. Workflow possible but no single command. |
| `db slurm edit qos` | — | 🟡 | Only allocations are editable (`ng slurm allocation edit`). Editing QOS `priority` / `flags` / replacing limits lists requires new operations. |
| `db slurm remove qos` | — | ❌ | Not ported |
| `db slurm remove partition` | — | ❌ | Not ported |
| `db slurm remove assoc` | — | ❌ | Not ported |
| `db slurm show qos` | `ng group show --site` and `ng user show --site` surface QOS info | 🟡 | No standalone `ng slurm qos show` or `ng slurm association show` |
| `db slurm show assoc` | ↑ same | 🟡 | |
| (none) | `ng slurm allocation add/edit` | ✅ (new) | Allocation lifecycle commands, new capability |

### Storage

| Old | New | Status | Notes |
|---|---|---|---|
| `db storage new home` | `ng storage new home` | 🟡 | New op covers creation; old also has optional pre-existing source re-use and site-wide defaults |
| `db storage new storage` | — | ❌ | Generic storage creation (non-home) not ported |
| `db storage new collection` | — | ➖ | New model eliminated SourceCollections |
| `db storage new automountmap` | — | ❌ | AutomountMap exists in the new model but no CLI to create one |
| `db storage edit source` | — | ❌ | Not ported |
| `db storage remove home` | — | ❌ | Not ported |
| `db storage show` | — | ❌ | Big gap — old had rich filter-by-user/group/name/collection/host/automount |
| `db storage to-puppet` | — | ❌ | Puppet export not ported |

### IAM (UC Davis IAM sync)

| Old | New | Status | Notes |
|---|---|---|---|
| `db iam sync` | — | ❌ | Bulk IAM resync not ported |
| `db iam new-user` | — | ❌ | Not ported |
| `db iam new-users` | — | ❌ | Not ported |

The new `User` model has an `iam: UCDIAMInfo` embedded (the target data shape), but nothing populates or refreshes it.

### HiPPO ✅ (ported)

| Old | New | Status | Notes |
|---|---|---|---|
| `hippo process` | `ng hippo process` | ✅ | Async handler dispatch; each handler invokes ng operations so every hippo-driven mutation lands in the History audit log |
| `hippo events` | `ng hippo events` | ✅ | Upstream event queue listing |
| `hippo sync-puppet` | `ng hippo sync-puppet` | ✅ | |
| `CreateAccount` handler | `CreateAccountHandler` (async) | 🟡 | User creation, site membership, access grants, group adds, and email all ported. **`_ensure_home_storage` is a no-op** pending site-default host info in the beanie `Site` model. |
| `AddAccountToGroup` handler | `AddAccountToGroupHandler` | ✅ | |
| `UpdateSshKey` handler | `UpdateSshKeyHandler` | ✅ | |
| `RemoveAccountFromGroup` handler | `RemoveAccountFromGroupHandler` | ✅ | |
| `CreateGroup` handler | `CreateGroupHandler` | ✅ | Sponsor group naming follows new `CreateGroupFromSponsor` op (`{sponsor}grp`) |
| Old `HippoEvent(mongoengine)` — opaque `data` dict | New `HippoEvent(beanie)` | ✅ (new capability) | Rich fields: resolved `site`, `target_user`, `target_groups`, plus `raw` payload, `first_seen_at` / `completed_at` timestamps, `last_error`. Indexed on status, action, site, target_user. |
| (none) | `ng hippo list` + `ng hippo show` | ✅ (new) | Query the local HippoEvent collection by status/action/user/site; `show` dumps one event as YAML |

### History / audit

| Old | New | Status |
|---|---|---|
| Timestamped comment-string audit on user | `History` collection for all write ops + `ng history --user --op --limit` | ✅ (new capability exceeds old) |

### Migrations (v1 → v2)

| Scope | New command | Status |
|---|---|---|
| Sites | `ng migrate sites` | ✅ |
| Users + UserSiteInfo | `ng migrate user[s]` | ✅ |
| Groups (members + sponsors + sudoers + slurmers) | `ng migrate groups` | ✅ |
| Slurm partitions | `ng migrate slurm partitions` | ✅ |
| Slurm QOSes (allocations stamped "migrated from v1") | `ng migrate slurm qoses` | ✅ |
| Slurm accounts | `ng migrate slurm accounts` | ✅ |
| Slurm associations | `ng migrate slurm associations` | ✅ |
| HippoEvents | — | ❌ (low value — events are a processing log, not reference data) |
| Storage | — | ❌ (storage model changed too much for a 1:1 migrate) |

### Adjacent top-level modules (not part of the `db` / `hippo` surface)

- `cheeto puppet` (YAML validate / merge / postload) — pure-schema, has no DB dependency; **no port needed**.
- `cheeto slurm` (sync from DB/YAML to `slurmctld`) — currently reads mongoengine models. Not ported against beanie.
- `cheeto nocloud` (Jinja templates) — no DB dependency; no port needed.
- `cheeto monitor` (IPMI) — no DB dependency; no port needed.
- `cheeto ipython` — was broken by the async postprocessor, now fixed with `nest_asyncio`; works with both ODMs.

---

## CRUD function coverage (old → new)

| Domain | Old crud.py count | Ported as operations | Ported as queries | Unported |
|---|---|---|---|---|
| User (writes) | 17 | 11 | — | `create_user_from_hippo` (folded into CreateAccountHandler), `query_admin_keys`, `user_to_puppet`, `tag_comment`, `get_next_*_id` (internal to ops) |
| User (reads) | 8 | — | 0 direct (show uses raw find_one) | `query_user_exists`, `query_user_type/access/status`, `query_user_groups/slurm/partitions`, all storages |
| Group (writes) | 11 | 11 | — | `add/remove_site_group`, `add_site_global_slurmer/group` (deprecated concept) |
| Group (reads) | 6 | — | 0 | `query_group_exists/slurm_associations/qoses/partitions/storages` |
| Site | 14 | 1 (`CreateSite`) | — | Everything else |
| Slurm (writes) | 3 | 5 (plus alloc add/edit) | — | No remove or edit ops; `load_slurm_from_puppet` unported |
| Slurm (reads) | 4 | — | `user_slurm_at_site`, `group_slurm_at_site`, `total_tres` | `query_slurm_associations`, `slurm_qos_state`, `slurm_association_state` |
| Storage (writes) | 1 | 1 (`CreateHomeStorage`) | — | Everything else (create storage, edit source, remove home, etc.) |
| Storage (reads) | 4 | — | 0 | All of them |
| Puppet export | 5 | — | — | `user_to_puppet`, `group_to_puppet`, `share_to_puppet`, `site_to_puppet`, `_storage_to_puppet` |
| Puppet import | 3 | — | — | `load_share_from_puppet`, `load_group_storages_from_puppet`, `load_slurm_from_puppet` |
| HiPPO handlers | 5 | 5 | — | — |

**Rough coverage**: ~40/79 (~51%) of old crud functions have a new-stack equivalent, concentrated in write operations and HiPPO handlers.

---

## Biggest gaps worth calling out

1. **Puppet export (`*_to_puppet` + `db site to-puppet` + `db site sync-*-puppet`).** Production artifact generation. Without this the new DB can't drive Puppet.
2. **LDAP sync (`db site to-ldap`, `db site check-ldap`).** Without this, users provisioned in beanie aren't visible to anything that authenticates via LDAP.
3. **IAM sync (`db iam sync`, `new-user(s)`).** The `User.iam` field is a dead letterbox until these are ported.
4. **Bulk class provisioning (`db group new class`).** The main mechanism for class onboarding — creates the group, N users, generates passwords, CSV export.
5. **`cheeto slurm sync`.** Reads old models, pushes to slurmctld. This is the runtime that actually makes slurm allocations take effect.
6. **Storage operations beyond `create home`.** Need create/edit/remove for group and share storages before the new storage model is useful in production.
7. **Show / list coverage.** `ng` has `user show` and `group show`. No list, no filter-by-type/access/status, no text search, no `site show`, no `slurm qos show`, no `storage show`.
8. **Home-storage creation from `CreateAccountHandler`.** Currently a no-op pending site-default host info in the beanie `Site` model.

---

## Suggested porting order

1. **Slurm remove ops + edit qos** (complete the CRUD on what's already modeled). Low effort.
2. **Puppet export** (`group_to_puppet`, `user_to_puppet`, `site_to_puppet`, `_storage_to_puppet`) — highest external impact, pure read path, unblocks running both stacks in parallel.
3. **`cheeto slurm sync` against beanie** — once exports are ported, make the slurm runtime reach the new DB.
4. **LDAP sync** — unblocks user-facing auth.
5. **IAM sync**.
6. **Bulk class provisioning** (ng replacement for `db group new class`).
7. **Site-default host info + wire home-storage creation into `CreateAccountHandler`**.
8. **Storage beyond home** (group / share create, edit, remove, show).
9. **Puppet import** (can likely be deferred indefinitely — migration is one-time).
10. **List/query coverage** in `ng` show commands (filter-by-type, text search, etc.). Iterative.
