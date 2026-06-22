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

# Git over SSH for the puppet repo sync (hub worker only). Activates only when a
# deploy key is mounted, so beat / api / site workers are unaffected. We copy the
# key to a 0400 private path (ssh rejects group/world-readable keys, and the mount
# may be read-only) and wire it via GIT_SSH_COMMAND, which cheeto/git_async.py
# inherits from the environment — no ~/.ssh/config needed. The SSH user is always
# `git` (from the git@github.com remote URL), so no User directive is required.
GIT_SSH_KEY="${GIT_SSH_KEY:-/run/cheeto/git-ssh-key}"

if [ -f "$GIT_SSH_KEY" ]; then
    mkdir -p /root/.ssh
    chmod 0700 /root/.ssh
    install -m 0400 "$GIT_SSH_KEY" /root/.ssh/git_id
    # Verify the host against the baked GitHub keys, plus an optional mounted
    # augment (a listed-but-absent file is simply ignored).
    known='/etc/cheeto/ssh/known_hosts.github'
    [ -f /run/cheeto/known_hosts ] && known="$known /run/cheeto/known_hosts"
    export GIT_SSH_COMMAND="ssh -i /root/.ssh/git_id -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile='$known'"
    # git refuses to commit without an author identity.
    git config --global user.name  "${GIT_AUTHOR_NAME:-cheeto-daemon}"
    git config --global user.email "${GIT_AUTHOR_EMAIL:-cheeto-daemon@hpc.ucdavis.edu}"
fi

exec cheeto "$@"
