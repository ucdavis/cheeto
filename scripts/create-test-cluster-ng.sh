#!/bin/zsh
#
# create-test-cluster.sh, ported to the v2 async/beanie stack (`cheeto ng`).
#
# Mapping notes vs the original `cheeto db` script:
#   * `--user/--site/--group` are single-valued in ng, so the v1 lines that
#     listed several users (`-u camw omen`) become one command per user.
#   * There is no `group add site` in v2 — a group's presence on a site is
#     implied by its per-site membership/sponsor edges, so the group only has
#     to *exist*. `ng group new {lab,system}` creates it (gid auto-assigned).
#   * v1 storage "source collections" map to v2 StorageVolumes
#     (`ng storage volume new`): host/prefix/quota/options/ranges carry over as
#     host/host-path/quota/export-options/export-ranges.
#   * GAPS — these have no `ng` CLI equivalent yet (they are only created by
#     `cheeto ng migrate`): creating an AutomountMap, and creating a generic
#     (non-home) Storage record. The home/group automount tables and the
#     `hpccfgrp` group-storage record from the v1 script therefore can't be
#     fully reproduced here; the closest expressible pieces (the backing
#     volumes + home provisioning) are included and the gaps are flagged below.
#   * Assumes the `camw` / `omen` users already exist as User documents (same
#     assumption the v1 script made). On a fresh beanie DB, create them first
#     with `cheeto ng user new generic ...` (example commented at the bottom).

cd

# v2 prerequisite: seed the standard access/status groups (idempotent).
cheeto ng group seed-access-status

cheeto ng site new --site test-cluster --fqdn test-cluster.hpc.ucdavis.edu

cheeto ng user add site -u camw -s test-cluster
cheeto ng user add site -u omen -s test-cluster

# v1 `group add site` -> create the groups (site association is implicit via
# the membership/sponsor edges added below). Skip these if the groups already
# exist (e.g. migrated).
cheeto ng group new lab -g hpccfgrp
cheeto ng group new system -g sponsors

cheeto ng group add member  -g hpccfgrp -s test-cluster -u camw
cheeto ng group add sponsor -g hpccfgrp -s test-cluster -u camw
cheeto ng group add sponsor -g hpccfgrp -s test-cluster -u omen

cheeto ng group add sponsor -g sponsors -s test-cluster -u camw
cheeto ng group add sponsor -g sponsors -s test-cluster -u omen
cheeto ng group add member  -g sponsors -s test-cluster -u camw
cheeto ng group add member  -g sponsors -s test-cluster -u omen

# v1 storage "source collections" -> v2 backing volumes.
cheeto ng storage volume new group -s test-cluster --backend zfs \
    --host nas0 --host-path /nas0/export/group --quota 100G \
    --export-options 'rw,no_root_squash,sync,no_subtree_check,crossmnt' \
    --export-ranges 10.0.0.0/16
cheeto ng storage volume new home -s test-cluster --backend zfs \
    --host nas0 --host-path /nas0/export/home --quota 20G \
    --export-options 'rw,no_root_squash,sync,no_subtree_check,crossmnt' \
    --export-ranges 10.0.0.0/16

# v1 created group/home/share automount maps here. There is no ng CLI to
# create an AutomountMap (only `cheeto ng migrate` does). Once one exists you
# would wire homes to it via `ng site storage set-defaults --home-automount-map`
# and drop the `--no-mount` below.

# Site home-provisioning defaults (parent volume + quota for new homes).
cheeto ng site storage set-defaults -s test-cluster --home-volume home --home-quota 20G

# v1 group storage `hpccfgrp` -> the backing volume (nested under `group`) is
# expressible; the Storage *record* (owner/group + automount mount) is not yet
# (no ng CLI; created via migrate).
cheeto ng storage volume new hpccfgrp -s test-cluster --backend zfs \
    --host nas0 --host-path /nas0/export/group/hpccfgrp --parent group --quota 1T

# Provision per-user home storage from the site defaults. `--no-mount` because
# there is no AutomountMap (see gap above); drop it once a home automount map
# exists and is set as the site default.
cheeto ng storage new home -u camw -s test-cluster --no-mount
cheeto ng storage new home -u omen -s test-cluster --no-mount

# If camw/omen don't exist yet on a fresh beanie DB, create them first, e.g.:
#   cheeto ng user new generic -u camw -s ... --email cswel@ucdavis.edu \
#       --fullname 'Camille Scott' --uid <UID> --access login-ssh slurm
#   cheeto ng user new generic -u omen --email omen@ucdavis.edu \
#       --fullname 'Omen Test' --uid <UID> --access login-ssh slurm
#
# The puppet.hpc repo for `cheeto ng site sync-old-puppet` is cloned out-of-band:
#   git clone <puppet.hpc remote> ~/puppet.hpc
#   cheeto ng site sync-old-puppet -s test-cluster ~/puppet.hpc
