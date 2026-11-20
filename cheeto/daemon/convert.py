"""Document -> response-model converters shared by several routers.

Every function here is synchronous and expects its input hydrated (links
resolved by the query layer); none of them queries."""

from ..models.slurm import SlurmAccount, SlurmAllocation, SlurmQOS, SlurmTRES
from ..models.storage import Storage, StorageVolume
from ..queries.slurm import total_tres
from .schemas import (
    AllocationOut,
    QOSOut,
    SlurmAccountLimitsOut,
    SlurmAccountOut,
    StorageOut,
    TRES,
    VolumeOut,
)


def storage_out(s: Storage) -> StorageOut:
    """`s` comes from `list_site_storages` (links hydrated)."""
    if s.automount_map is not None:
        mount_type, mount_source = 'automount', s.automount_map.name
    elif s.static_mount is not None:
        mount_type, mount_source = 'static', s.static_mount.name
    else:
        mount_type, mount_source = None, None
    try:
        mount_path = s.mount_path or None
    except (ValueError, RuntimeError):
        # A record whose mount path does not resolve (drift) must not fail
        # the whole listing.
        mount_path = None
    return StorageOut(
        name=s.name,
        category=s.category,
        owner=s.owner.name,
        group=s.group.name,
        volume=s.volume.name,
        host=s.host,
        host_path=s.host_path,
        subpath=s.subpath or None,
        quota=s.quota,
        mount_type=mount_type,
        mount_source=mount_source,
        mount_path=mount_path,
        mount_options=list(s.mount_options),
        globus=s.globus,
    )


def storages_out(storages) -> list[StorageOut]:
    """Skip a record with a dangling owner, group or volume link."""
    return [
        storage_out(s) for s in storages
        if s.owner is not None and s.group is not None and s.volume is not None
    ]


def volume_out(v: StorageVolume) -> VolumeOut:
    """`v` comes from `list_site_volumes` (`parent` resolved in place)."""
    return VolumeOut(
        name=v.name,
        backend=v.backend,
        host=v.host,
        host_path=v.host_path,
        quota=v.quota,
        parent=getattr(v.parent, 'name', None),
        zfs_dataset=v.zfs.dataset_name if v.zfs is not None else None,
        provisioned_at=v.provisioned_at,
    )


def tres_out(tres: SlurmTRES) -> TRES:
    return TRES(cpus=tres.cpus, gpus=tres.gpus, mem=tres.mem)


def allocation_out(a: SlurmAllocation) -> AllocationOut:
    return AllocationOut(tres=tres_out(a.tres), expires_at=a.expires_at)


def qos_out(qos: SlurmQOS) -> QOSOut:
    """`qos` has its allocation links fetched."""
    return QOSOut(
        name=qos.name,
        priority=qos.priority,
        flags=list(qos.flags),
        group_limits=[allocation_out(a) for a in qos.group_limits],
        user_limits=[allocation_out(a) for a in qos.user_limits],
        job_limits=[allocation_out(a) for a in qos.job_limits],
        group_total=tres_out(total_tres(qos.group_limits)),
        user_total=tres_out(total_tres(qos.user_limits)),
        job_total=tres_out(total_tres(qos.job_limits)),
    )


def account_out(account: SlurmAccount) -> SlurmAccountOut:
    """`account` has `group` and `coordinators` fetched."""
    return SlurmAccountOut(
        name=account.group.name,
        limits=SlurmAccountLimitsOut(**account.limits.model_dump()),
        coordinators=sorted(
            name for c in account.coordinators
            if (name := getattr(c, 'name', None)) is not None
        ),
    )
