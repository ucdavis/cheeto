# Deployment (UC Davis HPCCF)

This page describes the production deployment, as distinct from the architecture itself. The
repository contains no compose files, systemd units, or Kubernetes manifests — process
placement is operational configuration, recorded here.

Three sites are live: **hive**, **farm**, and **franklin**. All seven scheduled tasks are
enabled.

## Hosts

**`accounts.hpc.ucdavis.edu` — the hub.** All cheeto processes run in containers from the
`hpccf/cheeto` image (published to Docker Hub by CI on a `v*` tag): celery beat (exactly one
instance), the hub worker consuming the `cheeto` queue, the uvicorn API, and Flower.
**MongoDB and RabbitMQ run on the host**, not in containers — MongoDB as a replica set, since
cheeto's transactions require one.

**Slurm controller hosts — one site worker each.** Each cluster's controller also runs a
containerized `cheeto daemon worker --site <name>`:

| Site | Host | Queue |
|---|---|---|
| hive | `slurm.hive.hpc.ucdavis.edu` | `slurm.hive` |
| farm | `monitoring.farm.hpc.ucdavis.edu` | `slurm.farm` |
| franklin | `nas-8-0.franklin.hpc.ucdavis.edu` | `slurm.franklin` |

The image deliberately ships **no Slurm client**: Slurm's client tools must match the
controller's version and its munge/libslurm builds, so pinning them in the image would break
on every cluster upgrade. Instead each site container runs with `--network host` and
bind-mounts the host's `sacctmgr`, `scontrol`, `/usr/lib64/slurm`, and `slurm.conf`, plus the
munge key. The entrypoint installs the munge key at mode 0400 (copying rather than chmod'ing,
so a read-only mount or host-owned key is left untouched) and starts `munged` inside the
container before exec'ing cheeto.

**`ldap1.hpc.ucdavis.edu` and `ldap2.hpc.ucdavis.edu`** are a multi-master mirrored OpenLDAP
pair serving `dc=hpc,dc=ucdavis,dc=edu`. cheeto binds as `uid=cheeto,ou=Services,…` and
writes to the first configured server only; the pair replicates between themselves. Sites are
OUs beneath the searchbase, so both servers serve all three clusters.

**The puppet server** consumes the pull API — see below.

Site workers reach the hub over the campus network, so both hub services are **TLS**:
`amqps` on 5671 to RabbitMQ (configured through `daemon.broker_use_ssl`) and MongoDB with
`tls` plus a CA file.

```{mermaid}
flowchart TB
    subgraph hub["accounts.hpc.ucdavis.edu"]
        direction TB
        subgraph hubc["containers (hpccf/cheeto)"]
            beat["daemon beat"]
            hubw["daemon worker (-Q cheeto)"]
            api["daemon api (uvicorn)"]
            flower["daemon flower"]
        end
        rabbit(["RabbitMQ (host)"])
        mongo[("MongoDB replica set (host)")]
    end

    subgraph hive["slurm.hive.hpc.ucdavis.edu"]
        whive["worker --site hive"]
        shive["slurmctld / slurmdbd"]
    end
    subgraph farm["monitoring.farm.hpc.ucdavis.edu"]
        wfarm["worker --site farm"]
        sfarm["slurmctld / slurmdbd"]
    end
    subgraph frank["nas-8-0.franklin.hpc.ucdavis.edu"]
        wfrank["worker --site franklin"]
        sfrank["slurmctld / slurmdbd"]
    end

    ldap["ldap1 + ldap2.hpc.ucdavis.edu<br/>multi-master pair"]
    puppetsrv["puppet server"]
    gitrepo["puppet.hpc git remote"]
    hippo["HiPPO"]
    iam["UC Davis IAM"]

    beat --> rabbit
    rabbit --> hubw
    rabbit -- "amqps:5671" --> whive
    rabbit -- "amqps:5671" --> wfarm
    rabbit -- "amqps:5671" --> wfrank

    hubw --> mongo
    api --> mongo
    whive -- "mongodb+tls" --> mongo
    wfarm -- "mongodb+tls" --> mongo
    wfrank -- "mongodb+tls" --> mongo
    flower --> rabbit

    whive -- "sacctmgr (bind-mounted)" --> shive
    wfarm -- "sacctmgr (bind-mounted)" --> sfarm
    wfrank -- "sacctmgr (bind-mounted)" --> sfrank

    hubw -- "ldaps:636" --> ldap
    hubw -- "REST" --> hippo
    hubw -- "REST" --> iam
    hubw -- "git over ssh" --> gitrepo
    puppetsrv -- "HTTPS, every 60s" --> api
```

## Puppet, in both directions

The hub worker **pushes** legacy YAML: `puppet_sync` renders each site to
`domains/<fqdn>/merged/all.yaml` plus per-user public keys and commits them to a clone of the
puppet.hpc repo that lives on the hub. The clone must already exist — the daemon never clones
— and pushes authenticate with a deploy key mounted into the container.

The puppet server **pulls** the API: it fetches `/puppet/root-keys/{site}` and
`/puppet/storage/{site}` over HTTPS every 60 seconds and writes the responses to local JSON
files that hiera reads directly. Two reasons for that indirection:

1. **No hiera plugin.** Hiera reads ordinary JSON files, so nothing custom has to be
   installed, packaged, or kept compatible with the puppet server's Ruby.
2. **Availability.** The cached files are the fallback. If the API, the hub, or MongoDB is
   down, agent runs continue against the last good data instead of failing — which matters,
   because these responses configure root SSH access and storage mounts.

## Mount and secret contract

What each container needs from its host. This is the part that bites when rebuilding a host.

| Path | Where | Purpose |
|---|---|---|
| `/etc/cheeto/config.yaml` | all | the config file (read-only mount) |
| `/run/munge.key` | site workers | munge key; the entrypoint installs it 0400 and starts `munged` |
| `sacctmgr`, `scontrol`, `/usr/lib64/slurm`, `slurm.conf` | site workers | the host's Slurm client, version-matched to the controller |
| `/run/cheeto/git-ssh-key` | hub worker | deploy key for the puppet.hpc push; exported as `GIT_SSH_COMMAND` with `StrictHostKeyChecking=yes` against baked-in GitHub host keys |
| the puppet.hpc clone | hub worker | read-write; pre-cloned, at the path in `daemon.tasks.puppet_sync.repo` |
| `/var/lib/cheeto` | beat, hub worker | beat's schedule file and the Sympa export directory — must persist across container restarts |

Site workers additionally need `--network host` to reach the controller's munge socket.

## Operational notes

- Exactly one beat instance, cluster-wide. Two would double every scheduled sync.
- One worker per queue, concurrency 1. See
  [Scheduling](scheduling.md) for why this is load-bearing rather than conservative.
- Sync failures — including `max_deletions` aborts — surface in `celery_taskmeta` and Flower.
- Rolling out a new version is a tag push: CI builds and publishes `hpccf/cheeto:vX.Y.Z`, and
  each host pulls and restarts its containers. The hub and the site workers can be upgraded
  independently; tasks are versioned only by their name and arguments.
