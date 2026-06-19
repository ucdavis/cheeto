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
#     host/host-path/quota/export-options/export-ranges. The server-side NFS
#     export options live on the volume; the client-side autofs mount options
#     live on the automount map.
#   * REMAINING GAP: there is still no ng CLI to create a generic (non-home)
#     Storage *record*, so the `hpccfgrp` group-storage record from the v1
#     script can't be created here — only its backing volume. The `group`
#     automount table is created below, ready for it.
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

# v1 storage "source collections" -> v2 backing volumes (server-side NFS export
# options/ranges).
cheeto ng storage volume new group -s test-cluster --backend zfs \
    --host nas0 --host-path /nas0/export/group --quota 100G \
    --export-options 'rw,no_root_squash,sync,no_subtree_check,crossmnt' \
    --export-ranges 10.0.0.0/16
cheeto ng storage volume new home -s test-cluster --backend zfs \
    --host nas0 --host-path /nas0/export/home --quota 20G \
    --export-options 'rw,no_root_squash,sync,no_subtree_check,crossmnt' \
    --export-ranges 10.0.0.0/16

# v1 group/home/share automount tables -> `ng storage automount-map new`.
# --prefix is the client-side autofs mount root; --options are mount options.
cheeto ng storage automount-map new home  -s test-cluster --prefix /home  --options fstype=nfs4
cheeto ng storage automount-map new group -s test-cluster --prefix /group --options fstype=nfs4
cheeto ng storage automount-map new share -s test-cluster --prefix /share --options fstype=nfs4

# Site home-provisioning defaults: parent volume + quota + the home automount
# table, so `ng storage new home` mounts each home via autofs at /home/<user>.
cheeto ng site storage set-defaults -s test-cluster \
    --home-volume home --home-quota 20G --home-automount-map home

# v1 group storage `hpccfgrp` -> the backing volume (nested under `group`) is
# expressible; the group Storage *record* still has no creation CLI (see gap
# above). Once it exists you would attach it to the `group` table with
# `cheeto ng storage set-mount --name hpccfgrp --automount-map group`.
cheeto ng storage volume new hpccfgrp -s test-cluster --backend zfs \
    --host nas0 --host-path /nas0/export/group/hpccfgrp --parent group --quota 1T

# Provision per-user home storage from the site defaults (mounts via the `home`
# automount table set above).
cheeto ng storage new home -u camw -s test-cluster
cheeto ng storage new home -u omen -s test-cluster

# Inspect / adjust storage with the rest of the new CLI:
#   cheeto ng storage list -s test-cluster              # mount-type column
#   cheeto ng storage show camw -s test-cluster         # full detail + volume
#   cheeto ng storage automount-map show home -s test-cluster   # table entries
#   # bulk re-point every storage under a volume subtree:
#   cheeto ng storage volume set-mounts home --automount-map home -s test-cluster
#   # or change/clear a single storage's mount:
#   cheeto ng storage set-mount --name camw --static-mount <name> -s test-cluster

# If camw/omen don't exist yet on a fresh beanie DB, create them first, e.g.:
#   cheeto ng user new generic -u camw --email cswel@ucdavis.edu \
#       --fullname 'Camille Scott' --uid <UID> --access login-ssh slurm
#   cheeto ng user new generic -u omen --email omen@ucdavis.edu \
#       --fullname 'Omen Test' --uid <UID> --access login-ssh slurm
#
# The puppet.hpc repo for `cheeto ng site sync-old-puppet` is cloned out-of-band:
#   git clone <puppet.hpc remote> ~/puppet.hpc
#   cheeto ng site sync-old-puppet -s test-cluster ~/puppet.hpc
