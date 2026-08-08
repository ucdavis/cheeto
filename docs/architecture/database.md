# Database schema

MongoDB collections are defined by [beanie](https://beanie-odm.dev/) `Document` classes in
`cheeto/models/`. Field metadata uses pydantic, and indexes are declared in each document's
`Settings` subclass. Every document inherits `created_at`/`updated_at`; most identity and
allocation documents also carry `expires_at`/`provisioned_at` from the `Expirable` mixin,
which is how "granted but not yet built" and "expires at the end of the quarter" are
represented without extra collections.

## Global identity, per-site configuration

A POSIX account is cluster-wide, but access, membership, and status are per-cluster. The
schema splits accordingly:

- **`User` and `Group` hold global identity** — name, uid/gid, email, shell, type. Neither
  carries site-scoped data, and `Group` holds no member list at all.
- **`UserSiteInfo` is the `(user, site)` edge**: a user's presence at a site, plus optional
  overrides. A null `status` means inherit the global one; a **non-empty** `access` list
  *replaces* the global access list at that site, while an empty one inherits.
- **`GroupMembership` is the `(user, group, site)` edge**, carrying a `roles` array —
  `member`, `sponsor`, `sudoer`, `slurmer` — which replaced v1's four parallel per-site
  membership buckets.

Because the override semantics are not obvious, resolve them with
`queries.user.effective_access_links()` and `effective_status_link()` rather than reading the
fields directly.

`Site` holds cluster identity (`name`, `fqdn`, `aliases`) plus three defaults blocks: Slurm
accounts and groups every user at the site implicitly gets (`sticky`), and the storage
defaults `CreateHomeStorage` uses when provisioning a new home — a parent volume, a quota, and
exactly one of an automount map or a static mount (Farm uses automount, Hive uses static).

Access and status are themselves groups: `AccessGroup` and `StatusGroup` are subclasses of
`Group` sharing the `groups` collection through beanie's polymorphic discriminator, which is
why querying groups generically needs `with_children=True`. `AccessGroup.access_name` is the
short name used on users (`sudo`), while `name` is the LDAP `cn` (`sudo-users`). Their
membership is computed from `User.access` / `UserSiteInfo.access`, not from
`GroupMembership`.

```{mermaid}
erDiagram
    Site ||--o{ UserSiteInfo : "site"
    User ||--o{ UserSiteInfo : "user"
    Site ||--o{ GroupMembership : "site"
    User ||--o{ GroupMembership : "user"
    Group ||--o{ GroupMembership : "group"
    User ||--o{ SshKey : "user"
    StatusGroup |o--o{ User : "status"
    AccessGroup }o--o{ User : "access"
    StatusGroup |o--o{ UserSiteInfo : "status override"
    AccessGroup }o--o{ UserSiteInfo : "access override"
    Group ||..|| AccessGroup : "subclass, same collection"
    Group ||..|| StatusGroup : "subclass, same collection"

    Site {
        string name UK
        string fqdn
        string_array aliases
    }
    User {
        string name
        int uid
        int gid
        string type "user|admin|system|class|shared"
        datetime expires_at
    }
    Group {
        string name UK
        int gid UK
        string type "user|access|status|system|group|admin|class"
    }
    UserSiteInfo {
        link user UK "unique with site"
        link site UK
        link status "null = inherit global"
        link_array access "non-empty = replace global"
    }
    GroupMembership {
        link user UK "unique with group, site"
        link group UK
        link site UK
        string_array roles "member|sponsor|sudoer|slurmer"
    }
    SshKey {
        link user
        string key
    }
```

## Slurm

`SlurmAccount` is the per-site materialization of a `Group` as a Slurm account, unique on
`(group, site)`. `SlurmAssociation` is a pure edge mirroring Slurm's own association tuple —
site, account, partition, QOS, unique across all four. `SlurmQOS` references
`SlurmAllocation` records in three buckets that map onto sacctmgr's limit types:
`group_limits` → `GrpTRES`, `user_limits` → `MaxTRESPerUser`, `job_limits` →
`MaxTRESPerJob`.

```{mermaid}
erDiagram
    Site ||--o{ SlurmAccount : "site"
    Group ||--o{ SlurmAccount : "group"
    User }o--o{ SlurmAccount : "coordinators"
    Site ||--o{ SlurmPartition : "site"
    Site ||--o{ SlurmQOS : "site"
    Site ||--o{ SlurmAssociation : "site"
    SlurmAccount ||--o{ SlurmAssociation : "account"
    SlurmPartition ||--o{ SlurmAssociation : "partition"
    SlurmQOS ||--o{ SlurmAssociation : "qos"
    SlurmQOS }o--o{ SlurmAllocation : "group, user, job limits"
    Site }o..o{ SlurmAccount : "sticky, default_account (DocRef)"

    SlurmAccount {
        link group UK "unique with site"
        link site UK
        int max_user_jobs "-1 = unlimited"
        int max_group_jobs
        int max_submit_jobs
        string max_job_length
    }
    SlurmQOS {
        string name UK "unique with site; derived from account+partition"
        int priority
        string_array flags "default DenyOnLimit"
    }
    SlurmAllocation {
        tres tres "cpus, mem, gpus; None = unlimited"
        string comment
    }
    SlurmPartition {
        string name UK "unique with site"
    }
```

## Storage

Three concerns are kept separate: where the bytes live, how a client mounts them, and what is
exported to whom.

- **`StorageVolume`** is a backing dataset — ZFS or Quobyte, exactly one config block — with
  a self-referential `parent` forming the volume tree (per-user home datasets beneath a site
  home volume). `host` and `host_path` are always concrete, denormalized at creation rather
  than resolved through `parent` at read time.
- **`AutomountMap`** and **`StaticMount`** are the two mount mechanisms: an autofs map
  published to LDAP, or an fstab-style static mount.
- **`Storage`** is the thing users are granted: a named home, group, or share, owned by a
  user and a group, living on a volume, and mounted through *either* an automount map *or* a
  static mount — never both, and legitimately neither for a Quobyte native client.

```{mermaid}
erDiagram
    Site ||--o{ StorageVolume : "site"
    Site ||--o{ AutomountMap : "site"
    Site ||--o{ StaticMount : "site"
    Site ||--o{ Storage : "site"
    StorageVolume |o--o{ StorageVolume : "parent"
    StorageVolume |o--o{ StaticMount : "volume, XOR spec"
    StorageVolume ||--o{ Storage : "volume"
    User ||--o{ Storage : "owner"
    Group ||--o{ Storage : "group"
    AutomountMap |o--o{ Storage : "automount_map, XOR static_mount"
    StaticMount |o--o{ Storage : "static_mount, XOR automount_map"

    StorageVolume {
        string name UK "unique with site"
        string backend "zfs|quobyte"
        string host UK "unique with site, host_path"
        string host_path UK
        allocation_array allocations "quota + comment"
        nfs_export nfs_export
    }
    Storage {
        string name UK "unique with site, category"
        string category UK "home|group|share"
        string subpath "non-empty = exported subdirectory"
        string mount_name "automount only"
        mount_overrides mount_overrides
        bool globus
    }
    AutomountMap {
        string name UK "unique with site"
        string prefix "mount root, e.g. /group"
        string_array options
    }
    StaticMount {
        string name UK "unique with site"
        string mount_path UK "unique with site"
        string fstype "nfs|nfs4|cvmfs"
        string spec "raw device spec, XOR volume"
    }
```

Derived properties — `host`, `host_path`, `quota`, `mount_options`, `mount_path` — require
their links to be fetched and raise rather than misbehave on an unfetched link proxy. A
`Storage` may reference a volume at a *different* site; that is intentional, and is how
cross-site mounts are expressed.

## Operational collections

| Collection | Holds |
|---|---|
| `history` | one document per `Operation` run: op name, the operation's own change description, author, timestamp. Written inside the operation's transaction, so a rollback leaves no record and a record implies the write committed. |
| `hippo_events` | every ingested HiPPO event: action, status, retry count, last error, denormalized target names, the raw payload, and lifecycle timestamps. Unique on `(hippo_id, hippo_endpoint)`. |
| `celery_taskmeta` | celery task results and tracebacks (written by celery, not by cheeto). |

Two embedded blocks appear on documents rather than in their own collections: `LDAPInfo`
(`ignore`, `fingerprint`, `modified_at`, and the per-site `synced` watermark map) on `User`,
`Group`, and `Storage`; and `UCDIAMInfo` (person record, PPS associations) on `User`.

## Two beanie constraints that shape the models

Both have tripwire tests in `cheeto/tests/test_beanie.py`, because both fail *silently*.

**`Link` and `BackLink` may only be declared on `Document` classes.** Beanie never walks
embedded `BaseModel`s, so a `Link` nested inside one is stored as an inline document snapshot
instead of a reference — a stale copy that no longer tracks its target. Embedded models
therefore reference documents with `DocRef` (`models/base.py`): a bare `ObjectId` with a
coercing validator. This is why `Site`'s settings blocks appear as dotted, non-identifying
edges in the diagrams above — they are soft references, not links, and cannot be resolved
with `fetch_links`.

**`@before_event`/`@after_event` methods must not be underscore-prefixed.** Beanie's
`init_actions` skips private attributes, so a hook named `_normalize` is never registered and
never runs.
