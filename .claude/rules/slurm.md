# Slurm accounting & sacctmgr

**Scope:** apply when working on `cheeto/slurm.py`, `cheeto/models/slurm.py`,
`cheeto/operations/slurm.py`, `cheeto/queries/slurm.py`, `cheeto/cmds/slurm.py`,
`cheeto/cmds/ng/slurm.py`, `cheeto/cmds/ng/_slurm_show.py` (and the Slurm dataclasses in
`cheeto/puppet.py`).

Standards for code that models or drives Slurm's accounting database. Upstream docs:
[accounting](https://slurm.schedmd.com/accounting.html),
[sacctmgr](https://slurm.schedmd.com/sacctmgr.html),
[qos](https://slurm.schedmd.com/qos.html).

## Data model (mirror this in our models)

- Hierarchy is **cluster → account → user**. Order matters: a cluster must exist before
  accounts, accounts before users.
- An **association** is the tuple `(cluster, account, user, [partition])`. It is the unit
  that limits and QOS attach to — not the account or user directly.
- A **QOS** is a named bundle of limits/priority/flags. It attaches to associations
  (and partitions). One association can reference one QOS; a user can reach many QOS via
  different associations. QOS limits override association limits.
- **TRES** (trackable resources: `cpu`, `mem`, `gres/gpu`, …) are how limits are expressed.
- **Coordinators** are users with admin rights over an account.

Our beanie models (`cheeto/models/slurm.py`) map onto this: `SlurmAccount` is `(group, site)`,
`SlurmAssociation` is `(site, account, partition, qos)`, `SlurmQOS` holds
`group_limits`/`user_limits`/`job_limits` (lists of `SlurmAllocation`, each a `SlurmTRES`),
`priority`, and `flags` (validated against `SLURM_QOS_VALID_FLAGS` in `constants.py`).

## sacctmgr command grammar

`sacctmgr [opts] <verb> <entity> [specs]`. Verbs: `add`/`create`, `list`/`show`, `modify`,
`delete`/`remove`, `dump`, `load`. Entities: `account`, `user`, `qos`, `cluster`,
`association`, `tres`.

- **modify** uses `... set <updates> where <filters>` — `set` changes values, `where`
  selects rows. Order is `set` then `where` in our call sites.
- List/show for parsing: `-P`/`--parsable2` (pipe-delimited, no trailing pipe) and
  `-n`/`--noheader`. We always read with `csv.DictReader(..., delimiter='|')`.
- `-i` (immediate, skip confirmation) and `-Q` (quiet) are mandatory for non-interactive
  use; `SAcctMgr` bakes `-iQ` into every command.
- `+=` / `-=` on a TRES/QOS list appends/removes rather than replacing (e.g. `qos+=foo`).

## TRES rendering — the one format that matters

`cpu=<n>,mem=<MB>,gres/gpu=<n>`. **Unlimited is `-1`, never omitted.** `mem` is always
megabytes (convert via `size_to_megs`). Keep this consistent across the two renderers:

- `models/slurm.py::SlurmTRES.to_slurm()` → a single `str` (`cpu=…,mem=…,gres/gpu=…`).
- `puppet.py::SlurmQOSTRES.to_slurm()` / `SlurmQOS.to_slurm(modify=)` → a `list[str]` of
  sacctmgr arg tokens (`GrpTres=…`, `MaxTRESPerUser=…`, `MaxTresPerJob=…`, `Flags=…`,
  `Priority=…`); `SlurmQOSTRES.negate()` emits the all-`-1` clear string.

Limit buckets map as: `group_limits → GrpTRES`, `user_limits → MaxTRESPerUser`,
`job_limits → MaxTRESPerJob`. `GrpTRES` is the pooled cap across the association; `Max*` are
per-user / per-job. `DenyOnLimit` is our default flag (reject at submit instead of pend).

## Conventions in this repo

- Build commands through `SAcctMgr`/`SControl` (`cheeto/slurm.py`), which return **baked
  `sh.Command` objects** — never shell out by hand. See the `sh` rule for `sh` usage
  (including awaiting commands from async code paths).
- State is read into dicts (`get_slurm_qos_state`, `get_slurm_association_state`), diffed by
  the `reconcile_*` functions, and turned into a command list by `generate_commands`. New
  sync logic should follow that read → reconcile → emit-commands shape rather than mutating
  Slurm imperatively.
- QOS names are derived, not free-form: `get_qos_name(account, partition)`.
- Don't hand-edit generated clients; this is all first-party code.
