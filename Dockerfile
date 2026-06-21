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
# mounted and munged is started by the entrypoint. See the README "Container"
# section for the runtime mount/env contract.

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

RUN apt-get update && apt-get install -y --no-install-recommends \
        libldap-2.5-0 \
        libsasl2-2 \
        libkrb5-3 \
        libgssapi-krb5-2 \
        libgomp1 \
        munge \
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

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["--help"]
