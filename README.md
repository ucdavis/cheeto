# cheeto: UCD HPC Management Utilities

![Tests](https://github.com/ucdavis/cheeto/actions/workflows/test.yaml/badge.svg) ![Static Badge](https://img.shields.io/badge/Platforms-Linux-blue)

`cheeto` manages account and resource provisioning for the UC Davis HPC
Core Facility clusters. It owns the canonical user/group/storage/Slurm
state in MongoDB and syncs it outward: LDAP directories, Slurm accounting
databases, UC Davis IAM, the HiPPO provisioning API, Sympa mailing lists,
and puppet-consumed YAML. It also validates puppet YAML and renders
cloud-init (nocloud) installer templates.

## Architecture

### Layers

```
cheeto/cmds/        CLI (ponderosa CmdTree); `cheeto ng ...` is the async stack
cheeto/daemon/      persistent services: celery worker/beat tasks, FastAPI app
cheeto/operations/  write path: Operation classes (one per mutation/export),
                    transactional, recorded in the History collection
cheeto/queries/     read path: pure query helpers over the models
cheeto/models/      beanie (async MongoDB ODM) documents — the data model
cheeto/legacy/      v1 mongoengine models + migration, behind the optional
                    `legacy` extra (used only for the v1->v2 migration)
```

External integration modules:

- `cheeto/hippoapi/`, `cheeto/iamapi/` — generated httpx clients for the
  HiPPO and UC Davis IAM APIs (do not hand-edit).
- `cheeto/ldap_async.py` — bonsai-based async LDAP client/pool.
- `cheeto/slurm_sync.py` — async `sacctmgr` driver + reconcile, built on `sh`.
- `cheeto/git_async.py` — async git wrapper (used by the puppet sync).

### Data model (v2, `cheeto/models/`)

- **Site**: an HPC cluster. Users and groups carry per-site records
  (`UserSiteInfo`, `GroupMembership`); site-wide defaults (sticky groups,
  default Slurm account, home-storage provisioning defaults) are embedded
  on the Site document.
- **Slurm**: `SlurmAccount` is `(group, site)`; `SlurmAssociation` is
  `(site, account, partition, qos)`; `SlurmQOS` holds group/user/job TRES
  limit bundles. Sync follows read state → reconcile → emit `sacctmgr`
  commands.
- **Storage**: `StorageVolume` is the provisionable backing entity (ZFS
  dataset or QuoByte volume, optionally nested under a parent volume);
  `Storage` is the user-facing record `(volume, subpath)` plus at most one
  mount mechanism — an LDAP automount entry (`AutomountMap`) or an
  fstab-style `StaticMount`.
- **History**: every Operation records an audit entry (author, op name,
  describe() payload).

The v1 mongoengine model now lives in `cheeto/legacy/`, behind the optional
`legacy` extra (`poetry install --extras legacy`). It exists only so
`cheeto ng migrate ...` can migrate v1 data into the v2 collections; the v1
`cheeto db` CLI has been removed.

### Operations vs queries

All mutations go through `Operation` subclasses
(`cheeto/operations/*.py`), invoked as
`await Op.run(client, author, **kwargs)`. Multi-document writes run in
MongoDB transactions (replica set required). Reads used by the CLI, LDAP
projection, and exports live in `cheeto/queries/`.

## CLI

Entry point: `cheeto` (`cheeto.cmds.__main__:main`). Top-level groups:

```
cheeto config            show/write configuration
cheeto daemon            persistent services: celery worker/beat and REST API
cheeto ng                beanie/async operations:
  site | user | group | slurm | storage | history | migrate | hippo | iam | ldap
cheeto puppet            legacy puppet YAML validation/merging
cheeto nocloud           cloud-init nocloud template rendering
cheeto monitor           ad-hoc monitoring helpers
cheeto ipython           REPL with the environment loaded
```

Common flags on every command: `--config <path>` (default
`~/.config/cheeto/config.yaml`), `--profile/-p <name>`, `--log [file]`,
`--log-level`, `--quiet`.

Read-only site exports: `cheeto ng site export
{puppet-legacy,root-keys,sympa,storage}`.

## Daemons

`cheeto daemon` runs the scheduled syncs and the REST API as three
process types. Celery is the task manager: RabbitMQ (amqp) is the broker;
task results land in the application database's `celery_taskmeta`
collection via celery's mongodb result backend.

| Process | Command | Where |
|---|---|---|
| beat | `cheeto daemon beat` | exactly one instance, hub host |
| hub worker | `cheeto daemon worker` | hub host; consumes the `cheeto` queue |
| site worker | `cheeto daemon worker --site <name>` | each cluster head node; consumes `slurm.<name>` |
| api | `cheeto daemon api` | hub host (uvicorn) |
| flower | `cheeto daemon flower` | hub host (monitoring UI, default :5555) |

Task → queue topology: the hub worker runs HiPPO event processing, IAM
sync, LDAP sync, account reaping, Sympa list exports, and the legacy
puppet repo sync (`puppet_sync`, which commits/pushes each site's
`domains/<fqdn>/merged/all.yaml` into a pre-cloned puppet.hpc repo).
`slurm_sync` must execute on each cluster's head node (it drives the
local `sacctmgr`), so beat routes one `slurm_sync(site)` task per site
to that site's `slurm.<site>` queue.

Schedules come from the `daemon.tasks` config block: a numeric value is
an interval in seconds, a string is a 5-field crontab, and an absent task
is disabled. Interval tasks expire after one period so a backed-up queue
drops stale ticks rather than piling them up; workers run one task at a
time (`worker_concurrency=1`, prefetch 1). Scale by adding site queues,
not by running multiple hub workers — a second hub worker would allow
overlapping syncs.

Each task run executes in a fresh event loop with a fresh beanie client
(`AsyncMongoClient` is loop-bound). Operations are attributed to the
`daemon.author` user in History. A `slurm_sync`/`ldap_sync` run that
would exceed its `max_deletions` guard fails the task — visible in
`celery_taskmeta` — instead of deleting.

### REST API

FastAPI app served by `cheeto daemon api`:

```
GET /puppet/root-keys/{site}   root authorized_keys for site admins (text)
GET /puppet/storage/{site}     legacy puppet zfs/nfs storage structure (JSON)
```

If `api.api_key` is set in the config, requests must send a matching
`X-API-Key` header; unknown sites return 404.

### Monitoring (Flower)

`cheeto daemon flower` launches [Flower](https://flower.readthedocs.io/), the
celery monitoring UI, on the configured celery app — so it inherits the
`broker_url`, `broker_use_ssl` (TLS), and mongodb result backend without extra
flags:

```
cheeto daemon flower                       # http://127.0.0.1:5555
cheeto daemon flower --basic-auth user:pass --address 0.0.0.0
cheeto daemon flower -- --max_tasks=10000  # pass extra flags through to flower
```

It binds `127.0.0.1:5555` by default; gate it with `--basic-auth` and/or a
reverse proxy (`--url-prefix`) before exposing it. In the container, run it as
the `flower` role and publish 5555:

```
docker run --rm -p 5555:5555 \
    -v /etc/cheeto/config.yaml:/etc/cheeto/config.yaml:ro \
    cheeto daemon flower --address 0.0.0.0
```

### RabbitMQ TLS

The broker connection can run over TLS (`amqps`). It's off by default; add a
`broker_use_ssl` block to the daemon config to enable it (maps to celery's
`broker_use_ssl`). The Mongo result backend has its own TLS (`mongo.tls`) and
is independent.

**Client (cheeto)** — `config.yaml`:

```yaml
daemon:
  default:
    broker_url: amqps://USER:PASS@broker.example.edu:5671//   # amqps + TLS port 5671
    broker_use_ssl:
      ca_file: /etc/cheeto/rabbitmq/ca.pem        # verify the broker's cert
      cert_reqs: required                          # none | optional | required
      # mutual TLS only — omit unless the broker requires client certs:
      cert_file: /etc/cheeto/rabbitmq/client.pem
      key_file:  /etc/cheeto/rabbitmq/client.key
```

Use the `amqps://` scheme and port 5671; the broker certificate's SAN must
match the host in `broker_url` (verified against `ca_file` when
`cert_reqs: required`). Server-only TLS = just `ca_file` + `cert_reqs`. Every
worker/beat picks this up via `configure_celery_app`.

**Server (RabbitMQ broker)** — `rabbitmq.conf`:

```
listeners.ssl.default            = 5671
ssl_options.cacertfile           = /etc/rabbitmq/ca.pem
ssl_options.certfile             = /etc/rabbitmq/server.pem
ssl_options.keyfile              = /etc/rabbitmq/server.key
ssl_options.verify               = verify_peer     # verify_none for server-only TLS
ssl_options.fail_if_no_peer_cert = true            # true => require client certs (mutual TLS)
# optional: drop `listeners.tcp.default` to disable plaintext 5672
```

Provision a CA + server cert/key (SAN = broker FQDN; for mutual TLS, issue
client certs from the same CA). Restart RabbitMQ and confirm with
`rabbitmq-diagnostics listeners` (expect `amqp/ssl` on 5671); open the
firewall for 5671.

### Container

The `Dockerfile` (base `python:3.13-slim`) builds one image that runs any
daemon role; pick the role as the command. Mount the config at
`/etc/cheeto/config.yaml` (or set `CHEETO_CONFIG`):

```
docker build -t cheeto .
docker run --rm -v /etc/cheeto/config.yaml:/etc/cheeto/config.yaml:ro \
    cheeto daemon worker          # hub worker (also: beat | api --host 0.0.0.0)
```

The image deliberately does **not** bundle a Slurm client — Slurm's RPC is
only compatible across a few major releases, so the **site worker** binds the
head node's own Slurm install (matching the cluster's version) plus the
shared munge key. Mount the key at `/run/munge.key` (read-only is fine); the
entrypoint copies it into place and starts `munged` whenever it is present:

```
docker run --rm --network host \
    -v /etc/cheeto/config.yaml:/etc/cheeto/config.yaml:ro \
    -v /etc/munge/munge.key:/run/munge.key:ro \
    -v /usr/bin/sacctmgr:/usr/bin/sacctmgr:ro \
    -v /usr/bin/scontrol:/usr/bin/scontrol:ro \
    -v /usr/lib64/slurm:/usr/lib64/slurm:ro \
    -v /etc/slurm/slurm.conf:/etc/slurm/slurm.conf:ro \
    cheeto daemon worker --site <name>
```

Slurm install paths vary by head-node distro (the example is RHEL-style);
adjust the binary/lib mounts and `LD_LIBRARY_PATH` to match, and ensure
`libslurm.so.*` is reachable. The hub worker, beat, and api need none of the
Slurm/munge mounts.

The **hub worker** runs `puppet_sync`, which commits and pushes to the puppet.hpc
repo over SSH. The image bundles `git` + `openssh-client` and bakes GitHub's
published host keys; supply a deploy key (and, if you cloned it elsewhere, the
repo) at runtime:

```
docker run --rm \
    -v /etc/cheeto/config.yaml:/etc/cheeto/config.yaml:ro \
    -v /etc/cheeto/puppet-deploy-key:/run/cheeto/git-ssh-key:ro \
    -v /var/lib/cheeto/puppet.hpc:/var/lib/cheeto/puppet.hpc \
    -e GIT_AUTHOR_NAME="cheeto-daemon" \
    -e GIT_AUTHOR_EMAIL="cheeto-daemon@hpc.ucdavis.edu" \
    cheeto daemon worker          # hub worker
```

When the deploy key is present at `/run/cheeto/git-ssh-key` (override the path
with `GIT_SSH_KEY`), the entrypoint copies it to a private `0400` location and
exports `GIT_SSH_COMMAND` so `git` uses it with verified host keys — the SSH
user is always `git` (from the `git@github.com:…` remote), so no SSH `User`
config is needed. The repo at `daemon.tasks.puppet_sync.repo` must be **cloned
out-of-band** with a `git@github.com:…` (SSH) origin and mounted read-write; the
daemon never clones. `GIT_AUTHOR_NAME`/`GIT_AUTHOR_EMAIL` set the commit
identity (without them git can't commit). To verify a host key beyond GitHub's
baked set (e.g. during a key rotation), mount an extra file at
`/run/cheeto/known_hosts:ro` — it augments, not replaces, the baked keys.

## Configuration

YAML at `~/.config/cheeto/config.yaml` (override with `--config`).
Sections: `ldap`, `mongo`, `daemon`, and `api` are profiled
(`default` plus named profiles, selected with `--profile`); `hippo` and
`ucdiam` are global. `daemon` and `api` are optional.

```yaml
mongo:
  default:
    uri: 127.0.0.1
    port: 27017
    tls: false
    user: ''
    password: ''
    database: hpccf_v2
ldap:
  default:
    servers: [ldaps://ldap1.example.edu]
    searchbase: dc=hpc,dc=ucdavis,dc=edu
    login_dn: cn=admin,dc=hpc,dc=ucdavis,dc=edu
    password: '...'
    user_base: ou=users,dc=hpc,dc=ucdavis,dc=edu
hippo:
  base_url: https://hippo.ucdavis.edu
  api_key: '...'
  site_aliases: {caesfarm: farm}
  max_tries: 10
ucdiam:
  base_url: https://iet-ws.ucdavis.edu/api
  api_key: '...'
daemon:
  default:
    broker_url: amqp://cheeto:...@rabbit.example.edu:5672/cheeto
    author: cheeto-daemon
    sites: [farm, hive]
    beat_schedule_filename: /var/lib/cheeto/celerybeat-schedule
    tasks:
      hippo:      {schedule: 300, post_back: true}
      iam_sync:   {schedule: '0 2 * * *', concurrency: 4}
      ldap_sync:  {schedule: 600, max_deletions: 50}
      slurm_sync: {schedule: 600, apply: true, max_deletions: 50}
      reap:       {schedule: '0 3 * * *'}
      sympa:      {schedule: 3600, output_dir: /var/lib/cheeto/sympa}
      puppet_sync: {schedule: 1800, repo: /var/lib/cheeto/puppet.hpc}
api:
  default:
    host: 0.0.0.0
    port: 8810
    api_key: '...'
```

## Deployment

Requirements:

- Python >=3.12,<3.14; Poetry 2.x.
- MongoDB with a replica set (transactions are required, even
  single-node).
- RabbitMQ reachable from the hub and every cluster head node (daemon
  only).
- System packages for the LDAP/Kerberos bindings: `libldap-dev`,
  `libsasl2-dev`, `libkrb5-dev`.

Install:

```bash
poetry install
poetry run cheeto --version
```

Daemon setup:

1. Write the `daemon`/`api` config blocks on each host (profiles let one
   file serve dev and prod). Ensure `beat_schedule_filename` and the
   sympa `output_dir` are writable by the service user.
2. Create the daemon author account once (a system user named by
   `daemon.author`, e.g. `cheeto-daemon`) so History entries attribute
   correctly.
3. Run one unit per process type. Minimal systemd service sketch:

```ini
[Unit]
Description=cheeto hub worker
After=network-online.target

[Service]
User=cheeto
ExecStart=/usr/local/bin/cheeto daemon worker --profile prod --log
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

   On the hub: `worker`, `beat` (add `--pidfile`), and `api`. On each
   cluster head node: `worker --site <name>` (needs local `sacctmgr`;
   `slurm_sync` honors the `sudo` task option).

Operational notes:

- Task results and failures: `db.celery_taskmeta` in the application
  database (pruned automatically after `result_expires`).
- One-off manual runs of any sync remain available via the CLI
  (`cheeto ng slurm sync`, `cheeto ng ldap sync-site`,
  `cheeto ng iam sync-all`, `cheeto ng hippo process`).
- The API serves credential material (root keys); set `api.api_key`
  and/or bind it to a management network.

## Development

```bash
poetry install
poetry run pytest                                  # full suite
poetry run pytest cheeto/tests/test_beanie.py -k name -v
```

The test suite starts an ephemeral `mongod` (port 28080, replica set) via
a session fixture in `cheeto/tests/conftest.py`; `mongod` and `mongosh`
must be on PATH. No RabbitMQ is needed — celery wiring is tested in eager
mode and the API via an in-process ASGI transport.

Versioning: `poetry version patch|minor|major` (syncs
`cheeto/__init__.py`).

See `CLAUDE.md` and `.claude/rules/` for module-specific development
conventions (Slurm accounting model, `sh` subprocess usage, beanie
patterns).
