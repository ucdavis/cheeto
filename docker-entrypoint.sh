#!/bin/sh
# Start munged only when the cluster's shared key is mounted. The per-site
# Slurm worker needs munge to authenticate sacctmgr to the external slurmdbd;
# the hub worker / beat / api don't, so the same image serves every role and
# only the Slurm worker mounts a key.
#
# MUNGE_KEY is the path the key is bind-mounted at (read-only is fine). We
# copy it into /etc/munge with munge:munge 0400 rather than chmod/chown the
# mount in place — the mount may be read-only, and chowning a bind-mount would
# mutate the host's key ownership.
set -e

MUNGE_KEY="${MUNGE_KEY:-/run/munge.key}"

if [ -f "$MUNGE_KEY" ]; then
    install -o munge -g munge -m 0400 "$MUNGE_KEY" /etc/munge/munge.key
    chown munge:munge /etc/munge
    chmod 0700 /etc/munge
    mkdir -p /run/munge
    chown munge:munge /run/munge
    chmod 0755 /run/munge
    # munged daemonizes; runuser is provided by util-linux in the base image.
    runuser -u munge -- munged
fi

exec cheeto "$@"
