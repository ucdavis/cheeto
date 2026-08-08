# Scheduling and daemon processes

The scheduled syncs are celery tasks. RabbitMQ is the broker; MongoDB is the result backend.
`cheeto daemon` runs every process type.

| Process | Command | Where it runs |
|---|---|---|
| beat | `cheeto daemon beat` | exactly one instance, on the hub |
| hub worker | `cheeto daemon worker` | hub; consumes the `cheeto` queue |
| site worker | `cheeto daemon worker --site <name>` | that cluster's Slurm controller; consumes `slurm.<name>` |
| API | `cheeto daemon api` | hub (uvicorn) |
| flower | `cheeto daemon flower` | hub; celery monitoring UI |

## Dispatch topology

```{mermaid}
flowchart LR
    beat["celery beat"] --> rabbit(["RabbitMQ"])

    rabbit --> qhub["queue: cheeto"]
    rabbit --> qhive["queue: slurm.hive"]
    rabbit --> qfarm["queue: slurm.farm"]
    rabbit --> qfrank["queue: slurm.franklin"]

    qhub --> hub["hub worker"]
    qhive --> whive["site worker (hive)"]
    qfarm --> wfarm["site worker (farm)"]
    qfrank --> wfrank["site worker (franklin)"]

    hub --> mongo[("MongoDB")]
    whive --> mongo
    wfarm --> mongo
    wfrank --> mongo
    mongo -.- meta["celery_taskmeta<br/>(results and failures)"]
```

Only `slurm_sync` is routed off the default queue. It shells out to the local `sacctmgr`, so
it must execute on the cluster's controller host; beat therefore emits one
`slurm_sync(site)` task per site addressed to `slurm.<site>`, and the worker on that host is
started with `-Q slurm.<site>`. Everything else runs on the hub — including `puppet_sync`,
because the puppet repo clone lives there.

## Tasks

| Beat entry | Task | Per site | Queue | Work |
|---|---|---|---|---|
| `hippo-process` | `cheeto.hippo_process` | no | `cheeto` | poll HiPPO's pending events, apply them, post status back |
| `iam-sync` | `cheeto.iam_sync` | no | `cheeto` | refresh IAM person data for all syncable users; advance offboarding |
| `reap-offboarded` | `cheeto.reap` | no | `cheeto` | flip expired offboarding users to inactive |
| `ldap-sync-<site>` | `cheeto.ldap_sync` | yes | `cheeto` | project users, groups, and automounts to LDAP; prune orphans |
| `slurm-sync-<site>` | `cheeto.slurm_sync` | yes | `slurm.<site>` | reconcile QOS, accounts, and associations via `sacctmgr` |
| `sympa-export-<site>` | `cheeto.sympa_export` | yes | `cheeto` | write `<output_dir>/<site>.txt` |
| `puppet-sync-<site>` | `cheeto.puppet_sync` | yes | `cheeto` | render legacy puppet YAML and push to the repo |

Schedules come from the `daemon.tasks` config block, one entry per task: a **number** is an
interval in seconds, a **string** is a five-field crontab, and an **absent or null**
`schedule` disables the task. Per-site tasks fan out over the task's own `sites` list if it
has one, else `daemon.sites`.

Interval tasks are published with `expires` set to one interval, so a backed-up queue drops
stale ticks instead of accumulating them; crontab tasks (typically daily) never expire,
because silently skipping a nightly IAM sync would be worse than running it late.

Any task can be submitted on demand with `cheeto daemon enqueue <task> [--site …]`, which
mirrors beat's routing but sets no expiry — an explicitly requested run should happen even if
the worker is busy.

## Serialization is deliberate

The syncs are idempotent but must never overlap on the same target: two concurrent
`ldap_sync` runs for one site would each compute a diff against state the other is still
changing. The worker config enforces one task at a time — `worker_concurrency=1`,
`worker_prefetch_multiplier=1`, `task_acks_late=False`. **Scale by adding site queues, not by
adding hub worker replicas**; a second hub worker reintroduces exactly the overlap the
configuration is there to prevent.

Each task body is a plain coroutine bridged through `run_op`, which per invocation builds a
fresh event loop, a fresh beanie client, and resolves the History author from
`daemon.author`. The client is not reused across runs because `AsyncMongoClient` is bound to
the loop it was created on.

## Observing failures

Task results, including tracebacks, land in `celery_taskmeta` and are visible in Flower. This
is the alerting surface: when an LDAP or Slurm sync would exceed its `max_deletions` guard it
aborts *before mutating anything* and fails the task, so a suspiciously large diff shows up
as a task failure rather than as data loss.

## The pull API

`cheeto daemon api` serves the two read-only puppet endpoints described in
[External services](services.md) under a single uvicorn process. The whole router depends on
an `X-API-Key` header compared with `secrets.compare_digest`.

One caveat worth stating plainly: **if `api.api_key` is unset in the config, the
authentication check becomes a no-op** and both endpoints are open to anyone who can reach
the port. `/puppet/root-keys/{site}` returns SSH public keys for accounts with root access,
so it should be treated as sensitive and never exposed without both a key and a network
restriction.
