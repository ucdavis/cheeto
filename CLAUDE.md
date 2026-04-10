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
CLI (cheeto/cmds/)  →  Database CRUD (cheeto/database/crud.py)  →  MongoEngine Documents (cheeto/database/*.py)
                    →  External APIs: HiPPO (cheeto/hippo.py), IAM (cheeto/iam.py), LDAP (cheeto/ldap.py)
```

### CLI framework

Uses **ponderosa** `CmdTree` for hierarchical subcommands. Commands are registered via `@commands.register('parent', 'child')` decorators in `cheeto/cmds/`. Each command module registers its subtree on the shared `commands` object from `cheeto/cmds/__init__.py`. Argument groups use `@arggroup` and postprocessors run setup logic (like DB connections) before command execution. Entry point: `cheeto.cmds.__main__:main`.

### Data models — two parallel systems

- **Marshmallow dataclasses** (`cheeto/types.py`, `cheeto/models/`): Validation and YAML serialization for puppet/HiPPO data. Base class `_BaseModel` in `types.py` provides `to_dict()`, `dumps()`, `save_yaml()`, `load_yaml()` with ruamel.yaml round-trip support.
- **MongoEngine documents** (`cheeto/database/`): MongoDB persistence. Uses a global-plus-site pattern: `GlobalUser`/`SiteUser` and `GlobalGroup`/`SiteGroup`, where global records hold identity data and site records hold per-cluster configuration. Database files use `mongoengine` ODM — see `.cursor/rules/database.mdc`.

### Key modules by role

- **`cheeto/database/crud.py`** (~52KB): All database query/mutation functions — the primary data operations interface.
- **`cheeto/cmds/database.py`** (~77KB): CLI commands for user, group, site, storage, and SLURM management. Aliased as `cheeto db`.
- **`cheeto/hippo.py`**: Event-driven provisioning — pulls pending events from HiPPO API, dispatches handlers for CreateAccount, AddAccountToGroup, UpdateSshKey, RemoveAccountFromGroup, CreateGroup.
- **`cheeto/database/ldap.py`**: Syncs database state to LDAP directories.
- **`cheeto/puppet.py`**: Puppet YAML schema and validation with deep-merge support (`puppet_merge` in `cheeto/yaml.py`).
- **`cheeto/config.py`**: YAML config loaded from `~/.config/cheeto/config.yaml` with profile support. Sections: `ldap`, `mongo`, `hippo`, `ucdiam`.

### Generated API clients

`cheeto/hippoapi/` and `cheeto/iamapi/` are generated httpx-based clients for the HiPPO and UC Davis IAM APIs. Do not hand-edit these.

### Domain concepts

- **Sites**: HPC clusters. Users and groups have per-site records.
- **Sponsor groups**: PI-led groups that control SLURM account access.
- **User types**: `user`, `admin`, `system`, `class`, `shared` — each with different UID ranges defined in `cheeto/constants.py`.
- **Access types**: `login-ssh`, `ondemand`, `compute-ssh`, `root-ssh`, `sudo`, `slurm`.
- **Transactions**: Multi-document operations use `mongoengine.context_managers.run_in_transaction`.

### Current branch (asyncify)

The `asyncify` branch is adding async support. `beanie` (async MongoDB ODM) is in dependencies alongside `mongoengine`, and `cheeto/models/` contains the emerging async model layer.

Prefer consulting `beanie` documentation before introspecting its code: https://beanie-odm.dev/.
Remember that `beanie` extends `pydantic` for its models: https://pydantic.dev/docs/

Prefer defining Indexes via the `Settings` subclass rather than using the `Indexed` class. Prefer the `Annotated` pattern for field metadata.
