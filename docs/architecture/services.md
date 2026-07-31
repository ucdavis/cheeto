# External services

Each section below covers one integration: the module that owns it, the transport, which way
data flows, and what drives it. "Driver" is either a CLI subcommand, a scheduled celery task
(see [Scheduling](scheduling.md)), or an HTTP request from outside.

## MongoDB

| | |
|---|---|
| Module | `cheeto/db.py`, `cheeto/models/` |
| Transport | pymongo async (`AsyncMongoClient`) + beanie ODM |
| Direction | read/write — the source of truth |
| Driver | everything |

`connect_beanie()` is the only connection factory; it builds the client from the `mongo`
config section and initializes beanie over `models.ALL_MODELS`. A **replica set is
required**: operations are transactional by default, and MongoDB only offers multi-document
transactions on a replica set (a single-node set is sufficient).

MongoDB is also celery's result backend — task results and failures land in the
`celery_taskmeta` collection of the same database, which is what makes a failed sync
queryable after the fact.

## LDAP

| | |
|---|---|
| Module | `cheeto/ldap_async.py` (client), `cheeto/operations/ldap.py` (sync ops), `cheeto/models/ldap_sync.py` (watermarks) |
| Transport | [bonsai](https://bonsai.readthedocs.io/) async client over a connection pool |
| Direction | one-way, MongoDB → LDAP (read only to diff) |
| Driver | `cheeto ng ldap …`, `cheeto.ldap_sync` task (per site) |

One directory serves all sites; sites are organizational units beneath a single
`searchbase`. Users live in a **global** subtree (`uid=<name>,ou=users,<searchbase>`) because
a POSIX account is cluster-wide, while groups and automount maps are **per-site**
(`ou=groups,ou=<site>,…`, `ou=automount,ou=<site>,…`). `DNBuilder` owns that layout; nothing
else constructs DNs.

Users are written as `inetOrgPerson` + `posixAccount` + `ldapPublicKey` + `shadowAccount`
(the account expiry becomes `shadowExpire`, days since epoch); groups as `groupOfMembers` +
`posixGroup`; and autofs maps (`auto.master`, `auto.home`, `auto.group`, `auto.share`) from
the storage records. Access and status groups are materialized as ordinary POSIX groups so
that clients can gate on them.

Sync is per-site and incremental via the `LDAPInfo` watermark described in the
[overview](index.md). `SyncSiteLDAP` drives users → groups → automounts → prune. The prune
step deletes directory entries with no corresponding database record, which is the dangerous
half: it honors a `max_deletions` cap (50 by default) and aborts the whole run rather than
exceeding it. Setting `ldap.ignore` on a record opts it out of the projection entirely —
that is how hand-maintained service entries survive a prune.

## Slurm

| | |
|---|---|
| Module | `cheeto/slurm_sync.py`, `cheeto/operations/slurm.py`, `cheeto/queries/slurm.py` |
| Transport | `sacctmgr`, invoked via [`sh`](https://sh.readthedocs.io/) (never a shell string) |
| Direction | one-way, MongoDB → slurmdbd (read only to diff) |
| Driver | `cheeto ng slurm sync`, `cheeto.slurm_sync` task (per site, on the controller host) |

cheeto drives Slurm's accounting database only — it does not schedule, and it does not touch
`slurm.conf`. The model it mirrors is Slurm's own: **cluster → account → user**, where an
*association* is the tuple `(cluster, account, user, partition)` and is the thing limits
attach to. A *QOS* is a named bundle of limits, priority, and flags referenced by
associations; limits are expressed as TRES (`cpu=…,mem=…,gres/gpu=…`, where unlimited is
`-1`). QOS names are derived from `(account, partition)` rather than chosen by hand.

`AsyncSAcctMgr` bakes `sacctmgr -iQ` (immediate, quiet) and returns unexecuted commands, so
a sync can be printed before it runs. State is read with `--parsable2 --noheader` and parsed
as pipe-delimited CSV.

Reconciliation emits ten ordered command batches — add/modify QOS, then accounts, then user
associations, then **repoint user default accounts**, then delete associations, QOS, and
accounts. The ordering is not cosmetic: Slurm refuses to delete a user's current default
association, so the repoint has to sit between the adds and the deletes. Commands run
concurrently within a batch, sequentially across batches, and a single command failure is
reported without aborting the batch.

```{mermaid}
flowchart LR
    desired["desired state<br/>(queries/slurm.py)"] --> diff{{reconcile}}
    current["current state<br/>(sacctmgr show -P)"] --> diff
    diff --> batches["ordered command batches"]
    batches --> guard{"deletions ><br/>max_deletions?"}
    guard -- yes --> abort["abort, mutate nothing"]
    guard -- no --> apply["apply (--apply only)"]
```

The same shape governs the LDAP sync; only the transport differs.

## HiPPO

| | |
|---|---|
| Module | `cheeto/hippo.py` (client), `cheeto/operations/hippo.py` (event handling), `cheeto/models/hippo.py` |
| Transport | REST/JSON over httpx, via the generated `cheeto/hippoapi/` client (`X-API-Key`) |
| Direction | both — events in, status and email out |
| Driver | `cheeto ng hippo process`, `cheeto.hippo_process` task |

HiPPO is the user-facing provisioning portal. cheeto **polls** it — `GET
/api/EventQueue/PendingEvents` — rather than receiving webhooks; the only HTTP server cheeto
runs is the puppet pull API below. Five event actions are handled: `CreateAccount`,
`AddAccountToGroup`, `RemoveAccountFromGroup`, `UpdateSshKey`, `CreateGroup`. HiPPO's cluster
names are translated to cheeto site names through `hippo.site_aliases`.

Handlers do not write documents directly; they compose ordinary Operations (`CreateUser`,
`AddSiteUser`, `AddUserAccess`, `AddGroupMember`, `CreateHomeStorage`, …), so a HiPPO-driven
change is indistinguishable in History from an admin-driven one.

Each event is persisted as a `HippoEvent` keyed on `(hippo_id, hippo_endpoint)` — the
idempotency key, endpoint-qualified so test and production HiPPO instances can't collide.
Terminal status is posted back to `/api/EventQueue/UpdateStatus`, and `posted_back_at` is
stamped only on HTTP 200. Processing gates on `status` while postback gates on
`posted_back_at`, so a failed postback is retried on the next run *without* reprocessing the
event. Retries are bounded by `hippo.max_tries` before the event is marked `Failed`.

HiPPO is also the mail transport (see *Email and Sympa* below) and the recipient of
`cheeto ng hippo sync-puppet`, which asks HiPPO to re-read the puppet YAML.

## UC Davis IAM

| | |
|---|---|
| Module | `cheeto/iam_async.py`, `cheeto/operations/iam.py` |
| Transport | REST/JSON over httpx, via the generated `cheeto/iamapi/` client |
| Direction | read-only from IAM, write into MongoDB |
| Driver | `cheeto ng iam …`, `cheeto.iam_sync` and `cheeto.reap` tasks |

Campus IAM is the authority on whether a person is still affiliated with UC Davis. cheeto
fetches the kerberos-account search, contact info, PPS associations, and org divisions,
bundles them into the embedded `UCDIAMInfo` on `User`, and uses the result to drive
offboarding. Authentication is a `key=` **query parameter**, not a header.

The distinction that matters is definitive miss versus transient failure: an HTTP 200 whose
`responseData.results` is empty *is* the signal that someone has left, whereas a 5xx or a
timeout raises before any database write so the transaction rolls back and the user is
retried later. From there `SyncUserIAM` is a state machine over `grace_days` and
`expiry_offset_days` — first miss, within grace, offboarding, already-expiring, restored —
and `ReapOffboardedUsers` flips expired offboarding users to inactive on its own schedule.
Only real user accounts are synced; `system`, `class`, and `shared` accounts are never
touched.

## Puppet

Two independent channels, in opposite directions.

**Push: legacy YAML into git.** `SyncOldPuppet` renders a site to the v1 puppet layout —
`domains/<site.fqdn>/merged/all.yaml` plus `keys/<username>.pub` — and round-trips it through
git: checkout base, pull, branch, commit, push, merge, push, delete the branch. Git is driven
through `sh`; concurrent runs are serialized with a lock file inside the repo. The clone must
already exist (the daemon never clones), and an empty commit is reported as "no change"
rather than an error.

**Pull: JSON out of the API.** `cheeto daemon api` serves two read-only endpoints out of
MongoDB:

| Endpoint | Returns |
|---|---|
| `GET /puppet/root-keys/{site}` | root `authorized_keys` for that site's admins |
| `GET /puppet/storage/{site}` | the legacy puppet ZFS/NFS storage structure |

`{site}` accepts a site name, an alias, or an fqdn. Both endpoints run their operation with
`skip_history=True` — they are hot reads, not mutations. Note that `root-keys` falls back to
the global admin key set for an unrecognized site instead of returning 404, while `storage`
404s.

`cheeto/puppet.py` itself is the v1 marshmallow schema for puppet.hpc YAML, with deep-merge
support; it does no network I/O and is exposed as `cheeto puppet validate`.

## Email and Sympa

| | |
|---|---|
| Module | `cheeto/mail.py` (messages), `cheeto/operations/email.py` (audit), `cheeto/hippo.py` (transport) |
| Transport | HiPPO `POST /api/Notify/Styled` — there is no SMTP path |
| Direction | outbound |
| Driver | HiPPO handlers inline; `iam_sync` and `reap` when `notify` is enabled |

Message bodies are Jinja templates rendered to Markdown, then split into paragraphs because
HiPPO's notification model takes a list of them. Every send goes through `SendUserEmail`,
which records the subject, recipients, and the full rendered body in History — so what a
user was told is recoverable. Sending is best-effort and non-transactional: a failure is
recorded as unsent and never aborts the operation that triggered it.

Sympa integration is a file drop rather than an API: `ExportSympaEmails` renders one address
per line and the task writes it atomically to `<output_dir>/<site>.txt` for Sympa to pick up.

`cheeto monitor power` is a standalone utility that appends local `ipmitool` DCMI power
readings to a CSV. It is unscheduled and never touches the database; it is not part of this
architecture.

## Configuration

One YAML file, `~/.config/cheeto/config.yaml` by default, overridden with `--config`.
Sections marked *profiled* are keyed by profile name and selected with `--profile` (falling
back to `default`), which is how one file holds production and test targets.

| Section | Profiled | Configures |
|---|---|---|
| `mongo` | yes | host/port, credentials, TLS + CA file, database name |
| `ldap` | yes | server URIs, `searchbase` and `user_base`, bind DN + password, TLS policy and CA, pool sizes, timeouts |
| `hippo` | no | `base_url`, API key, `site_aliases` (HiPPO cluster → cheeto site), `max_tries` |
| `ucdiam` | no | `base_url`, API key, `grace_days`, `expiry_offset_days`, timeout |
| `daemon` | yes, optional | broker URL and TLS, site list, History `author`, timezone, beat schedule file, task time limit, and the per-task `tasks` blocks |
| `api` | yes, optional | bind host/port, `api_key`, `root_path` and `prefix` for reverse proxying |

Only the first entry of `ldap.servers` is used for writes.
