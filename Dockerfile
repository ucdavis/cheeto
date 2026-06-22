# syntax=docker/dockerfile:1
#
# cheeto image. Runs any daemon role:
#   cheeto daemon worker [--site <name>] | beat | api
#
# It deliberately does NOT bundle a Slurm client. Slurm's RPC is only
# compatible across ~2-3 major releases and Debian's slurm-client (22.05) is
# too old for a modern slurmdbd, so the per-site Slurm worker bind-mounts the
# head node's own sacctmgr/scontrol + libs + slurm.conf at runtime (version
# always matches the cluster). munge is installed here; the shared key is
# mounted and munged is started by the entrypoint. git + openssh-client are
# installed for the hub worker's puppet repo sync; its deploy key is mounted and
# the entrypoint wires GIT_SSH_COMMAND. See the README "Container" section for
# the runtime mount/env contract.

# ---------------------------------------------------------------------------
# Builder: compile C-extension deps (python-ldap, gssapi, bonsai, pyescrypt)
# and install cheeto + its main deps into an in-project venv.
# ---------------------------------------------------------------------------
FROM python:3.13-slim-bookworm AS builder

ENV POETRY_VERSION=2.0.1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_NO_INTERACTION=1 \
    PIP_NO_CACHE_DIR=1

# build-essential + headers for the C extensions; git for the git-pinned
# mongoengine dependency (see pyproject.toml).
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        git \
        libldap-dev \
        libsasl2-dev \
        libkrb5-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install "poetry==${POETRY_VERSION}"

WORKDIR /app
COPY pyproject.toml poetry.lock README.md ./
COPY cheeto ./cheeto

# Installs the root package + main deps into /app/.venv, honoring the
# lockfile (including the git-pinned mongoengine). The dev group is optional
# and excluded by --only main.
RUN poetry install --only main

# ---------------------------------------------------------------------------
# Runtime: just the venv + the shared libraries the C extensions need at
# runtime, plus munge (munged + libmunge2 + the munge user).
# ---------------------------------------------------------------------------
FROM python:3.13-slim-bookworm

# ca-certificates provides the public CA bundle libldap verifies the LDAPS
# server cert against — bonsai sets no CA of its own, so without it the TLS
# handshake fails as LDAP_SERVER_DOWN. git + openssh-client drive the puppet
# repo sync (cheeto/git_async.py pushes over SSH); git only *Recommends*
# openssh-client, which --no-install-recommends skips, so name it explicitly.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        libldap-2.5-0 \
        libsasl2-2 \
        libkrb5-3 \
        libgssapi-krb5-2 \
        libgomp1 \
        munge \
        git \
        openssh-client \
    && rm -rf /var/lib/apt/lists/* \
    # Drop any key the package may have generated: the entrypoint starts
    # munged only when a key is *mounted*, so non-Slurm roles never run it.
    && rm -f /etc/munge/munge.key

COPY --from=builder /app /app

# Slurm client lib dirs the head-node mount lands in (harmless when empty);
# libslurm dlopens its plugins from the compiled PluginDir, so the mount must
# match the host path (commonly /usr/lib64/slurm) — adjust per head-node distro.
ENV PATH="/app/.venv/bin:${PATH}" \
    LD_LIBRARY_PATH="/usr/lib64:/usr/lib64/slurm" \
    SLURM_CONF="/etc/slurm/slurm.conf" \
    CHEETO_CONFIG="/etc/cheeto/config.yaml"

# GitHub's published SSH host keys (https://api.github.com/meta .ssh_keys), baked
# so the puppet-sync git push verifies the host without a TOFU prompt. The
# entrypoint points UserKnownHostsFile at this file (plus an optional mounted
# augment). Refresh on rebuild if GitHub rotates its host keys.
COPY <<-"EOF" /etc/cheeto/ssh/known_hosts.github
github.com ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl
github.com ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBEmKSENjQEezOmxkZMy7opKgwFB9nkt5YRrYMjNuG5N87uRgg6CLrbo5wAdT/y6v0mKV0U2w0WZ2YB/++Tpockg=
github.com ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCj7ndNxQowgcQnjshcLrqPEiiphnt+VTTvDP6mHBL9j1aNUkY4Ue1gvwnGLVlOhGeYrnZaMgRK6+PKCUXaDbC7qtbW8gIkhL7aGCsOr/C56SJMy/BCZfxd1nWzAOxSDPgVsmerOBYfNqltV9/hWCqBywINIR+5dIg6JTJ72pcEpEjcYgXkE2YEFXV1JHnsKgbLWNlhScqb2UmyRkQyytRLtL+38TGxkxCflmO+5Z8CSSNY7GidjMIZ7Q4zMjA2n1nGrlTDkzwDCsw+wqFPGQA179cnfGWOWRVruj16z6XyvxvjJwbz0wQZ75XK5tKSb7FNyeIEs4TT4jk+S4dhPeAUC5y+bDYirYgM4GC7uEnztnZyaVWQ7B381AK4Qdrwt51ZqExKbQpTUNn+EjqoTwvqNj4kqx5QUCI0ThS/YkOxJCXmPUWZbhjpCg56i+2aB6CmK2JGhn57K5mj0MNdBXA4/WnwH6XoPWJzK5Nyu2zB3nAZp+S5hpQs+p1vN1/wsjk=
EOF

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["--help"]
