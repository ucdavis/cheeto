# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

cheeto is a Python CLI tool for managing HPC infrastructure at UC Davis. It handles user/group provisioning, SLURM resource management, LDAP synchronization, storage/automount configuration, and integration with the HiPPO provisioning API. It also validates and generates Puppet YAML and renders cloud-init (nocloud) templates.

## Build and Development

**Package manager:** Poetry (v2.0.1) | **Python:** >=3.12, <3.14

```bash
poetry install                              # Install dependencies
poetry run cheeto <subcommand>              # Run CLI
poetry run pytest                           # Run all tests
poetry run pytest cheeto/tests/test_db.py::test_name -v  # Single test
poetry run pytest -k "pattern"              # Tests matching pattern
poetry version patch|minor|major            # Bump version (syncs cheeto/__init__.py)
```

Tests require MongoDB with replica set support (for transactions). The test suite automatically starts an ephemeral `mongod` on port 28080 with replica set `mongoengine` via the session-scoped `start_mongodb` fixture in `cheeto/tests/conftest.py`. CI installs MongoDB 8.0 plus `libldap-dev`, `libsasl2-dev`, `libkrb5-dev`.

## Architecture

### Layer structure

```
CLI (cheeto/cmds/, cheeto/cmds/ng/)  →  Operations (cheeto/operations/) + Queries (cheeto/queries/)  →  beanie Documents (cheeto/models/)
                                     →  External APIs: HiPPO (cheeto/hippo.py), IAM (cheeto/iam_async.py), LDAP (cheeto/ldap_async.py)
```

The v1 mongoengine layer (`cheeto/legacy/`) is retained **only** for the v1→v2 data
migration and is gated behind the optional `legacy` extra (`pip install cheeto[legacy]`);
nothing in the default install imports mongoengine.

### CLI framework

Uses **ponderosa** `CmdTree` for hierarchical subcommands. Commands are registered via `@commands.register('parent', 'child')` decorators in `cheeto/cmds/`. Each command module registers its subtree on the shared `commands` object from `cheeto/cmds/__init__.py`. Argument groups use `@arggroup` and postprocessors run setup logic (like DB connections) before command execution. Entry point: `cheeto.cmds.__main__:main`.

### Data models

- **beanie documents** (`cheeto/models/`): the live async MongoDB persistence layer. Global-plus-site pattern: identity on `User`/`Group`, per-site config on `UserSiteInfo`/`GroupMembership` edges. See `.cursor/rules/database.mdc`.
- **Marshmallow dataclasses** (`cheeto/types.py`, `cheeto/puppet.py`, `cheeto/config.py`): validation + YAML serialization for puppet/HiPPO/config data. Base class `_BaseModel` in `types.py` (`to_dict()`/`dumps()`/`save_yaml()`/`load_yaml()`, ruamel.yaml round-trip).
- **v1 mongoengine documents** (`cheeto/legacy/database/`): the old `GlobalUser`/`SiteUser`, `GlobalGroup`/`SiteGroup`, etc. Retained only for migration; importing them requires the `legacy` extra.

### Key modules by role

- **`cheeto/operations/`**: write-side `Operation` classes (the mutation API; each `run()` logs to History). **`cheeto/queries/`**: read-side query helpers.
- **`cheeto/cmds/ng/`**: the `cheeto ng` CLI (beanie/async) — the live command surface.
- **`cheeto/hippo.py`**: shared HiPPO API client/notifier; event handling lives in `cheeto/operations/hippo.py`.
- **`cheeto/ldap_async.py`** + **`cheeto/operations/ldap.py`**: sync beanie state to LDAP directories (bonsai async client).
- **`cheeto/legacy/migrate.py`** + **`cheeto/cmds/ng/migrate.py`**: the v1→v2 migration ops and their `cheeto ng migrate` CLI (gated behind the `legacy` extra).
- **`cheeto/puppet.py`**: Puppet YAML schema and validation with deep-merge support (`puppet_merge` in `cheeto/yaml.py`).
- **`cheeto/config.py`**: YAML config loaded from `~/.config/cheeto/config.yaml` with profile support. Sections: `ldap`, `mongo`, `hippo`, `ucdiam`, plus optional profiled `daemon` and `api`.
- **`cheeto/daemon/`**: Persistent services (`cheeto daemon worker|beat|api`). Celery periodic tasks (RabbitMQ broker, mongodb result backend) run the syncs on schedules from `daemon.tasks` config; `slurm_sync` is routed to per-site `slurm.<site>` queues consumed by cluster head-node workers (`--site`). Task bodies are plain coroutines bridged via `run_op` (fresh event loop + fresh `connect_beanie` per run — AsyncMongoClient is loop-bound). `daemon/api.py` is the FastAPI app (`/puppet/root-keys/{site}`, optional X-API-Key auth).

### Generated API clients

`cheeto/hippoapi/` and `cheeto/iamapi/` are generated httpx-based clients for the HiPPO and UC Davis IAM APIs. Do not hand-edit these.

### Module-specific rules

Scoped reference rules live in `.claude/rules/` (mirrored as Cursor `.mdc` rules in `.cursor/rules/`). Read the relevant one before editing the modules it covers:

- **`.claude/rules/slurm.md`** — Slurm accounting / sacctmgr usage (the account→association→QOS model, TRES format, `.to_slurm()` renderers). Covers `cheeto/slurm_sync.py`, `cheeto/models/slurm.py`, `cheeto/{operations,queries}/slurm.py`, `cheeto/cmds/ng/slurm.py`, `cheeto/cmds/ng/_slurm_show.py`, and the Slurm dataclasses in `cheeto/puppet.py`.
- **`.claude/rules/sh.md`** — running external commands with the `sh` library, including asyncio usage. Covers `cheeto/slurm_sync.py`, `cheeto/git_async.py`, `cheeto/mail.py`, `cheeto/monitor.py`.

### Domain concepts

- **Sites**: HPC clusters. Users and groups have per-site records.
- **Sponsor groups**: PI-led groups that control SLURM account access.
- **User types**: `user`, `admin`, `system`, `class`, `shared` — each with different UID ranges defined in `cheeto/constants.py`.
- **Access types**: `login-ssh`, `ondemand`, `compute-ssh`, `root-ssh`, `sudo`, `slurm`.
- **Transactions**: Multi-document `Operation`s run inside a beanie/pymongo session (the `Operation` base's `transactional` flag); see `cheeto/operations/base.py`.

### v1 → v2 status

v2 (beanie/async) is the live stack. The v1 mongoengine code has been retired to
`cheeto/legacy/` and is kept only for the v1→v2 migration, gated behind the optional
`legacy` extra. Install it with `poetry install --extras legacy` (or `pip install
'cheeto[legacy]'`); `cheeto ng migrate …` and importing `cheeto.legacy.*` require it and
raise a clear error otherwise (`cheeto/legacy/__init__.py::require_legacy`).

Prefer consulting `beanie` documentation before introspecting its code: https://beanie-odm.dev/.
Remember that `beanie` extends `pydantic` for its models: https://pydantic.dev/docs/

Prefer defining Indexes via the `Settings` subclass rather than using the `Indexed` class. Prefer the `Annotated` pattern for field metadata.

Two beanie traps with CI tripwires (`TestNoLinksInEmbeddedModels` in `cheeto/tests/test_beanie.py`):
- `Link`/`BackLink` may only be declared on `Document` classes. Beanie never walks embedded `BaseModel`s, so a nested Link silently stores an inline document snapshot. Embedded models reference documents with `DocRef` (`cheeto/models/base.py`) — a bare ObjectId with a coercing validator.
- `@before_event`/`@after_event` hook methods must NOT be underscore-prefixed — beanie's `init_actions` silently skips private attributes and the hook never fires.

## Jira scoping

This repo's Jira work lives in a single project. Scope all Atlassian MCP tool calls to these values:

- **Workspace**: `hpccf.atlassian.net`
- **cloudId**: `593ed228-c5aa-419c-92ab-19f05d49f796`
- **Project key**: `CHEETO` (id `10213`)

Rules for using `mcp__claude_ai_Atlassian__*` tools in this repo:

- When a tool requires `cloudId`, use `593ed228-c5aa-419c-92ab-19f05d49f796`.
- When creating or editing issues, the `project.key` must be `CHEETO`.
- For `searchJiraIssuesUsingJql`, always prefix the JQL with `project = CHEETO AND ...`.
- When fetching or transitioning issues, verify the issue key starts with `CHEETO-` before acting. If it doesn't, stop and ask the user — don't assume.
- Do not operate on any other project (e.g. HPC, HIVE, DM, HPC2) from this working directory, even if a user message references a non-`CHEETO-` issue key. If the user clearly asks you to cross-project, confirm first.
