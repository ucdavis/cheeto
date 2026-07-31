# Architecture

cheeto is the system of record for HPC identity and resource allocation at the UC Davis HPC
Core Facility. MongoDB holds the authoritative state; every other system cheeto touches is
either a **projection** of that state (LDAP directories, Slurm's accounting database, puppet
YAML, Sympa list files) or an **ingest** into it (HiPPO provisioning events, UC Davis IAM
person records). No projection target is ever read back as truth — external state is read
only to compute a diff.

```{mermaid}
flowchart TB
    subgraph entry["Entry points"]
        cli["cheeto ng CLI"]
        beat["celery beat + workers"]
        api["puppet pull API"]
    end

    mongo[("MongoDB<br/>source of truth")]

    hippo["HiPPO"]
    iam["UC Davis IAM"]
    ldap["LDAP directory"]
    slurm["Slurm slurmdbd"]
    puppet["puppet.hpc git repo"]
    sympa["Sympa"]

    cli --> mongo
    beat --> mongo
    api --> mongo

    hippo -- "events (REST, polled)" --> beat
    beat -- "status postback, email" --> hippo
    iam -- "person records (REST)" --> beat
    beat -- "add/modify/delete (LDAP)" --> ldap
    beat -- "sacctmgr" --> slurm
    beat -- "commit + push (git/ssh)" --> puppet
    beat -- "list files" --> sympa
    puppetsrv["puppet server"] -- "HTTPS pull" --> api
```

## Layers

```
CLI (cheeto/cmds/ng/)  →  operations/ (write) + queries/ (read)  →  beanie Documents (cheeto/models/)
                       →  external clients: hippo.py, iam_async.py, ldap_async.py, slurm_sync.py, git_async.py
```

The CLI is a hierarchical `CmdTree` from [ponderosa](https://github.com/camillescott/ponderosa);
subcommands register themselves with `@commands.register('parent', 'child')` decorators in
`cheeto/cmds/`. The live command surface is `cheeto ng`.

Every mutation goes through an `Operation` subclass in `cheeto/operations/`, invoked as
`await Op.run(client, author, **kwargs)`. Operations are transactional by default — multi-
document writes run in a MongoDB session, which is why the deployment requires a replica set
— and each run inserts one `History` document inside that same transaction. A rolled-back
operation therefore logs nothing, and there is no supported path that mutates state without
an audit record. Reads used by the CLI, the LDAP projection, and the puppet exports live in
`cheeto/queries/`.

The v1 mongoengine layer survives in `cheeto/legacy/` solely for the v1→v2 migration, gated
behind the optional `legacy` extra. Nothing in a default install imports mongoengine.

## Two patterns worth internalizing

**Read → reconcile → emit commands.** The Slurm and LDAP syncs never mutate imperatively.
They read current external state into normalized dicts, read desired state out of MongoDB,
diff the two, and emit an ordered list of commands, which a separate apply step executes.
This makes every sync previewable (`cheeto ng slurm sync` is dry-run until `--apply`),
idempotent, and safe to interrupt. It also makes deletion auditable: both syncs abort before
mutating anything if the diff would delete more than `max_deletions` objects.

**Dirty watermarks, not flags.** LDAP-projected documents (`User`, `Group`, `Storage`) carry
an embedded `LDAPInfo` holding a content `fingerprint` and a per-site `synced` map of
sitename → timestamp. A record needs syncing at a site when its `modified_at` is newer than
that site's watermark, so each site's sync schedule advances independently and a boolean
"dirty" flag can't be consumed by the wrong site. Changes to related documents (SSH keys,
memberships, site info) propagate by touching the parent's watermark, deferred until the
enclosing transaction commits.

```{toctree}
:maxdepth: 1

services
scheduling
database
deployment
```
