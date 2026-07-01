from argparse import Namespace
from pathlib import Path

from beanie import PydanticObjectId
from ponderosa import ArgParser, arggroup
from rich.panel import Panel
from rich.table import Table

from .. import commands
from ...args import regex_argtype
from ...constants import QOS_TRES_REGEX, SLURM_QOS_VALID_FLAGS
from ...log import Console
from ...models.group import Group
from ...models.site import Site
from ...models.slurm import SlurmAllocation, SlurmPartition, SlurmQOS, SlurmTRES
from ...operations import (
    AddQOSAllocation,
    CreateSlurmAccount,
    CreateSlurmAssociation,
    CreateSlurmPartition,
    CreateSlurmQOS,
    EditSlurmAccount,
    EditSlurmAllocation,
    EditSlurmQOS,
    ProvisionSlurmAllocation,
    RemoveSlurmAssociation,
    RemoveSlurmPartition,
    RemoveSlurmQOS,
    SyncSlurm,
)
from ...slurm_sync import DumpSAcctMgr, SlurmSyncAborted
from ...queries.slurm import (
    QOSAllocation,
    list_allocations_at_site,
    list_associations_at_site,
    list_partitions_at_site,
    list_qos_at_site,
    partition_at_site,
    qos_at_site,
    slurm_account_at_site,
    total_tres,
)
from ...operations.base import UNSET
from ...types import parse_qos_tres
from ...yaml import print_yaml
from ._args import EXPIRABLE_CLEAR, expirable_args, group_args, site_args
from ._slurm_show import _tres_compact


_QOS_ALLOC_FIELDS = ('group_limits', 'user_limits', 'job_limits')


def _tres_from_string(s: str | None) -> SlurmTRES | None:
    if s is None:
        return None
    return SlurmTRES(**parse_qos_tres(s))


def _expirable_kwarg(value):
    """Translate the parsed expirable_value into an Operation kwarg.

    Argparse default of None means 'flag not given' -> UNSET (leave alone).
    EXPIRABLE_CLEAR -> None (set the field to null).
    A datetime -> the datetime itself.
    """
    if value is None:
        return UNSET
    if value == EXPIRABLE_CLEAR:
        return None
    return value


@commands.register('ng', 'slurm',
                   help='Slurm resource operations')
def slurm_cmd(args: Namespace):
    pass


# Explicit no-op parents so `ng slurm --help` shows help text next to each
# subcommand group instead of just the bare names.
@commands.register('ng', 'slurm', 'partition',
                   help='Slurm partition operations')
def slurm_partition_cmd(args: Namespace):
    pass


@commands.register('ng', 'slurm', 'qos',
                   help='Slurm QOS operations')
def slurm_qos_cmd(args: Namespace):
    pass


@commands.register('ng', 'slurm', 'association', aliases=['assoc'],
                   help='Slurm association operations')
def slurm_association_cmd(args: Namespace):
    pass


@commands.register('ng', 'slurm', 'account',
                   help='Slurm account operations')
def slurm_account_cmd(args: Namespace):
    pass


@site_args.apply(required=True)
@commands.register('ng', 'slurm', 'partition', 'new',
                   help='Create a new Slurm partition')
async def slurm_partition_new(args: Namespace):
    console = Console()
    partition = await CreateSlurmPartition.run(
        args.db, args.author,
        name=args.name, site_name=args.site,
    )
    console.print(f'Created partition [green]{partition.name}[/] on site {args.site}')


@slurm_partition_new.args()
def _(parser: ArgParser):
    parser.add_argument('--name', '-n', required=True)


@site_args.apply(required=True)
@commands.register('ng', 'slurm', 'qos', 'new',
                   help='Create a new Slurm QOS')
async def slurm_qos_new(args: Namespace):
    console = Console()

    def _alloc(tres_str: str | None) -> list[SlurmAllocation]:
        tres = _tres_from_string(tres_str)
        if tres is None:
            return []
        return [SlurmAllocation(tres=tres, comment=args.comment)]

    qos = await CreateSlurmQOS.run(
        args.db, args.author,
        name=args.name, site_name=args.site,
        group_limits=_alloc(args.group_limits),
        user_limits=_alloc(args.user_limits),
        job_limits=_alloc(args.job_limits),
        priority=args.priority, flags=args.flags,
    )
    console.print(f'Created QOS [green]{qos.name}[/] on site {args.site}')


@slurm_qos_new.args()
def _(parser: ArgParser):
    parser.add_argument('--name', '-n', required=True)
    parser.add_argument('--priority', type=int, default=0)
    parser.add_argument('--flags', nargs='+', default=None)
    parser.add_argument('--group-limits', default=None,
                        type=regex_argtype(QOS_TRES_REGEX),
                        help='Initial group-limits TRES (e.g. "cpus=128 mem=1T")')
    parser.add_argument('--user-limits', default=None,
                        type=regex_argtype(QOS_TRES_REGEX),
                        help='Initial user-limits TRES')
    parser.add_argument('--job-limits', default=None,
                        type=regex_argtype(QOS_TRES_REGEX),
                        help='Initial job-limits TRES')
    parser.add_argument('--comment', default='initial allocation',
                        help='Comment recorded on each initial allocation')


@group_args.apply(required=True)
@site_args.apply(required=True)
@commands.register('ng', 'slurm', 'association', 'new',
                   help='Create a new Slurm association')
async def slurm_association_new(args: Namespace):
    console = Console()
    assoc = await CreateSlurmAssociation.run(
        args.db, args.author,
        site_name=args.site,
        account_group_name=args.group,
        partition_name=args.partition,
        qos_name=args.qos,
    )
    console.print(
        f'Created association: group=[green]{args.group}[/] '
        f'partition=[green]{args.partition}[/] qos=[green]{args.qos}[/]'
    )


@slurm_association_new.args()
def _(parser: ArgParser):
    parser.add_argument('--partition', required=True)
    parser.add_argument('--qos', required=True)


@commands.register('ng', 'slurm', 'allocation', aliases=['alloc'],
                   help='Slurm allocation operations')
def slurm_allocation_cmd(args: Namespace):
    pass


@site_args.apply(required=True)
@expirable_args.apply(scope='allocation')
@commands.register('ng', 'slurm', 'allocation', 'add',
                   help='Add an allocation to an existing QOS')
async def slurm_allocation_add(args: Namespace):
    console = Console()
    tres = _tres_from_string(args.tres)
    alloc = await AddQOSAllocation.run(
        args.db, args.author,
        qos_name=args.qos, site_name=args.site,
        field=args.field, tres=tres, comment=args.comment,
        expires_at=_expirable_kwarg(args.expires_at),
        provisioned_at=_expirable_kwarg(args.provisioned_at),
    )
    console.print(
        f'Added allocation [green]{alloc.id}[/] to '
        f'{args.qos}.{args.field}'
    )


@slurm_allocation_add.args()
def _(parser: ArgParser):
    parser.add_argument('--qos', required=True,
                        help='QOS name to append the allocation to')
    parser.add_argument('--field', required=True,
                        choices=list(_QOS_ALLOC_FIELDS),
                        help='Which QOS limits list to append to')
    parser.add_argument('--tres', required=True,
                        type=regex_argtype(QOS_TRES_REGEX),
                        help='TRES string (e.g. "cpus=128 gpus=8 mem=1T")')
    parser.add_argument('--comment', default='',
                        help='Descriptive comment')


@expirable_args.apply(scope='allocation')
@commands.register('ng', 'slurm', 'allocation', 'edit',
                   help='Edit an existing allocation by id')
async def slurm_allocation_edit(args: Namespace):
    console = Console()
    tres = _tres_from_string(args.tres)
    alloc = await EditSlurmAllocation.run(
        args.db, args.author,
        allocation_id=args.id, tres=tres, comment=args.comment,
        expires_at=_expirable_kwarg(args.expires_at),
        provisioned_at=_expirable_kwarg(args.provisioned_at),
    )
    console.print(f'Updated allocation [green]{alloc.id}[/]')


@slurm_allocation_edit.args()
def _(parser: ArgParser):
    parser.add_argument('--id', required=True,
                        help='ObjectId of the allocation to edit')
    parser.add_argument('--tres', default=None,
                        type=regex_argtype(QOS_TRES_REGEX),
                        help='New TRES string (replaces existing)')
    parser.add_argument('--comment', default=None,
                        help='New comment (replaces existing)')


# ---------------------------------------------------------------------------
# partition remove
# ---------------------------------------------------------------------------


@site_args.apply(required=True)
@commands.register('ng', 'slurm', 'partition', 'remove',
                   help='Remove a Slurm partition')
async def slurm_partition_remove(args: Namespace):
    console = Console()
    await RemoveSlurmPartition.run(
        args.db, args.author,
        name=args.name, site_name=args.site, force=args.force,
    )
    console.print(f'Removed partition [green]{args.name}[/] on site {args.site}')


@slurm_partition_remove.args()
def _(parser: ArgParser):
    parser.add_argument('--name', '-n', required=True)
    parser.add_argument('--force', action='store_true', default=False,
                        help='Cascade: also remove associations that reference '
                             'this partition')


# ---------------------------------------------------------------------------
# qos remove / edit / show
# ---------------------------------------------------------------------------


@site_args.apply(required=True)
@commands.register('ng', 'slurm', 'qos', 'remove',
                   help='Remove a Slurm QOS and its owned allocations')
async def slurm_qos_remove(args: Namespace):
    console = Console()
    await RemoveSlurmQOS.run(
        args.db, args.author,
        name=args.name, site_name=args.site, force=args.force,
    )
    console.print(f'Removed QOS [green]{args.name}[/] on site {args.site}')


@slurm_qos_remove.args()
def _(parser: ArgParser):
    parser.add_argument('--name', '-n', required=True)
    parser.add_argument('--force', action='store_true', default=False,
                        help='Cascade: also remove associations that reference '
                             'this QOS')


@site_args.apply(required=True)
@commands.register('ng', 'slurm', 'qos', 'edit',
                   help='Edit QOS priority and/or flags (allocation edits '
                        'use `ng slurm allocation edit`)')
async def slurm_qos_edit(args: Namespace):
    console = Console()
    qos = await EditSlurmQOS.run(
        args.db, args.author,
        name=args.name, site_name=args.site,
        priority=args.priority, flags=args.flags,
    )
    console.print(f'Updated QOS [green]{qos.name}[/]')


@slurm_qos_edit.args()
def _(parser: ArgParser):
    parser.add_argument('--name', '-n', required=True)
    parser.add_argument('--priority', type=int, default=None,
                        help='New priority value')
    parser.add_argument('--flags', nargs='+', default=None,
                        choices=list(SLURM_QOS_VALID_FLAGS),
                        help='Replacement flag list')


def _qos_to_dict(qos: SlurmQOS) -> dict:
    def _alloc_dicts(allocs):
        return [
            {
                'id': str(a.id),
                'tres': {'cpus': a.tres.cpus, 'gpus': a.tres.gpus,
                         'mem': a.tres.mem},
                'comment': a.comment,
            }
            for a in allocs
        ]
    return {
        'name': qos.name,
        'site': qos.site.name if hasattr(qos.site, 'name') else None,
        'priority': qos.priority,
        'flags': list(qos.flags),
        'group_limits': _alloc_dicts(qos.group_limits),
        'user_limits': _alloc_dicts(qos.user_limits),
        'job_limits': _alloc_dicts(qos.job_limits),
        'group_total_tres': total_tres(qos.group_limits).model_dump(),
    }


def _render_qos_panel(data: dict) -> Panel:
    table = Table(show_header=False, box=None, pad_edge=False, padding=(0, 1))
    table.add_column(style='bold cyan', no_wrap=True)
    table.add_column()

    for key in ('name', 'site', 'priority'):
        table.add_row(key, str(data.get(key)))
    table.add_row('flags', ', '.join(data['flags']) or '[dim](none)[/]')

    def _fmt(v) -> str:
        return '∞' if v is None else str(v)

    for limit_key in ('group_limits', 'user_limits', 'job_limits'):
        allocs = data[limit_key]
        if not allocs:
            table.add_row(limit_key, '[dim](none)[/]')
            continue
        sub = Table(box=None, pad_edge=False, padding=(0, 1))
        sub.add_column('id', style='dim')
        sub.add_column('cpus')
        sub.add_column('gpus')
        sub.add_column('mem')
        sub.add_column('comment')
        for a in allocs:
            sub.add_row(
                a['id'], _fmt(a['tres']['cpus']), _fmt(a['tres']['gpus']),
                str(a['tres']['mem'] or ''), a['comment'],
            )
        table.add_row(limit_key, sub)

    gt = data['group_total_tres']
    table.add_row(
        'group total',
        f'cpus={_fmt(gt["cpus"])}, gpus={_fmt(gt["gpus"])}, '
        f'mem={_fmt(gt["mem"])}',
    )

    return Panel(table, title=f'[bold]QOS:[/] [green]{data["name"]}[/]',
                 border_style='cyan', expand=False)


@site_args.apply(required=True)
@commands.register('ng', 'slurm', 'qos', 'show',
                   help='Show one or all QOSes at a site')
async def slurm_qos_show(args: Namespace):
    console = Console()
    site = await Site.find_one(Site.name == args.site)
    if site is None:
        console.print(f'[red]Site {args.site} not found[/]')
        return 1

    if args.name:
        qos = await qos_at_site(site, args.name)
        if qos is None:
            console.print(f'[red]QOS {args.name} not found on {args.site}[/]')
            return 1
        data = _qos_to_dict(qos)
        if args.yaml:
            print_yaml(data)
        else:
            console.print(_render_qos_panel(data))
        return 0

    qoses = await list_qos_at_site(site)
    if not qoses:
        console.print(f'[dim](no QOSes on {args.site})[/]')
        return 0
    if args.yaml:
        print_yaml([_qos_to_dict(q) for q in qoses])
        return 0

    table = Table(title=f'QOSes on {args.site}')
    table.add_column('name', style='green')
    table.add_column('priority', style='yellow')
    table.add_column('flags', style='dim')
    table.add_column('group total tres', style='bold')
    for qos in qoses:
        tt = total_tres(qos.group_limits)
        table.add_row(
            qos.name, str(qos.priority),
            ', '.join(qos.flags),
            _tres_compact(tt),
        )
    console.print(table)


@slurm_qos_show.args()
def _(parser: ArgParser):
    parser.add_argument('--name', '-n', default=None,
                        help='Specific QOS to show; omit to list all at the site')
    parser.add_argument('--yaml', action='store_true', default=False,
                        help='Output as YAML')


# ---------------------------------------------------------------------------
# association remove / show
# ---------------------------------------------------------------------------


@site_args.apply(required=True)
@commands.register('ng', 'slurm', 'association', 'remove',
                   help='Remove Slurm associations for a group at a site. '
                        '--partition and --qos are optional filters that '
                        'narrow the set; omit them to remove every '
                        'association for the group at the site.')
async def slurm_association_remove(args: Namespace):
    console = Console()
    count = await RemoveSlurmAssociation.run(
        args.db, args.author,
        site_name=args.site,
        account_group_name=args.group,
        partition_name=args.partition,
        qos_name=args.qos,
    )
    scope = [f'group=[green]{args.group}[/]']
    if args.partition:
        scope.append(f'partition={args.partition}')
    if args.qos:
        scope.append(f'qos={args.qos}')
    console.print(
        f'Removed [yellow]{count}[/] association(s): ' + ' '.join(scope),
    )


@slurm_association_remove.args()
def _(parser: ArgParser):
    parser.add_argument('--group', '-g', required=True)
    parser.add_argument('--partition', default=None,
                        help='Optional: narrow to this partition')
    parser.add_argument('--qos', default=None,
                        help='Optional: narrow to this QOS')


@site_args.apply(required=True)
@commands.register('ng', 'slurm', 'association', 'show',
                   help='List Slurm associations (optionally filtered)')
async def slurm_association_show(args: Namespace):
    console = Console()
    site = await Site.find_one(Site.name == args.site)
    if site is None:
        console.print(f'[red]Site {args.site} not found[/]')
        return 1

    group = partition = qos = None
    if args.group:
        group = await Group.find_one(Group.name == args.group)
        if group is None:
            console.print(f'[red]Group {args.group} not found[/]')
            return 1
    if args.partition:
        partition = await SlurmPartition.find_one(
            SlurmPartition.name == args.partition,
            SlurmPartition.site.id == site.id,
        )
        if partition is None:
            console.print(f'[red]Partition {args.partition} not found on {args.site}[/]')
            return 1
    if args.qos:
        qos = await SlurmQOS.find_one(
            SlurmQOS.name == args.qos,
            SlurmQOS.site.id == site.id,
        )
        if qos is None:
            console.print(f'[red]QOS {args.qos} not found on {args.site}[/]')
            return 1

    assocs = await list_associations_at_site(
        site, group=group, partition=partition, qos=qos,
    )
    if not assocs:
        console.print(f'[dim](no matching associations)[/]')
        return 0

    def _assoc_dict(a):
        acc_group = a.account.group
        return {
            'group': acc_group.name if hasattr(acc_group, 'name') else str(acc_group.ref.id),
            'partition': a.partition.name,
            'qos': a.qos.name,
            'qos_priority': a.qos.priority,
            'group_total_tres': total_tres(a.qos.group_limits).model_dump(),
        }

    if args.yaml:
        print_yaml([_assoc_dict(a) for a in assocs])
        return 0

    table = Table(title=f'Associations on {args.site}')
    table.add_column('group', style='green')
    table.add_column('partition', style='cyan')
    table.add_column('qos', style='magenta')
    table.add_column('priority', style='yellow')
    table.add_column('group total tres', style='bold')
    for a in assocs:
        d = _assoc_dict(a)
        tt = d['group_total_tres']
        parts = []
        if tt['cpus'] != -1:
            parts.append(f'{tt["cpus"]}c')
        if tt['gpus'] != -1:
            parts.append(f'{tt["gpus"]}g')
        if tt['mem'] is not None:
            parts.append(str(tt['mem']))
        table.add_row(
            d['group'], d['partition'], d['qos'], str(d['qos_priority']),
            '/'.join(parts) if parts else '∞',
        )
    console.print(table)


@slurm_association_show.args()
def _(parser: ArgParser):
    parser.add_argument('--group', '-g', default=None)
    parser.add_argument('--partition', default=None)
    parser.add_argument('--qos', default=None)
    parser.add_argument('--yaml', action='store_true', default=False,
                        help='Output as YAML')


# ---------------------------------------------------------------------------
# provision (composite create-qos + create-association)
# ---------------------------------------------------------------------------


@group_args.apply(required=True)
@site_args.apply(required=True)
@commands.register('ng', 'slurm', 'provision',
                   help='Provision a group for Slurm on a partition: creates a '
                        'QOS (if absent) and an association in one step')
async def slurm_provision(args: Namespace):
    console = Console()
    tres = _tres_from_string(args.group_limits)
    assoc = await ProvisionSlurmAllocation.run(
        args.db, args.author,
        site_name=args.site,
        account_group_name=args.group,
        partition_name=args.partition,
        qos_name=args.qos,
        group_limits_tres=tres,
        comment=args.comment,
        priority=args.priority,
        flags=args.flags,
    )
    console.print(
        f'Provisioned allocation: group=[green]{args.group}[/] '
        f'partition=[green]{args.partition}[/] '
        f'qos=[green]{args.qos or args.group + "-" + args.partition + "-qos"}[/]'
    )


@slurm_provision.args()
def _(parser: ArgParser):
    parser.add_argument('--partition', required=True)
    parser.add_argument('--qos', default=None,
                        help='QOS name (default: {group}-{partition}-qos)')
    parser.add_argument('--group-limits', default=None,
                        type=regex_argtype(QOS_TRES_REGEX),
                        help='Group-limits TRES for the new QOS (e.g. '
                             '"cpus=128 mem=1T")')
    parser.add_argument('--comment', default='initial allocation',
                        help='Comment on the initial allocation')
    parser.add_argument('--priority', type=int, default=0,
                        help='QOS priority (ignored if QOS already exists)')
    parser.add_argument('--flags', nargs='+', default=None,
                        choices=list(SLURM_QOS_VALID_FLAGS),
                        help='QOS flags (ignored if QOS already exists)')


# ---------------------------------------------------------------------------
# partition show
# ---------------------------------------------------------------------------


def _partition_to_dict(p: SlurmPartition) -> dict:
    return {
        'name': p.name,
        'site': p.site.name if hasattr(p.site, 'name') else None,
        'created_at': p.created_at,
        'updated_at': p.updated_at,
    }


@site_args.apply(required=True)
@commands.register('ng', 'slurm', 'partition', 'show',
                   help='Show one partition or list partitions at a site, '
                        'optionally filtered by group')
async def slurm_partition_show(args: Namespace):
    console = Console()
    site = await Site.find_one(Site.name == args.site)
    if site is None:
        console.print(f'[red]Site {args.site} not found[/]')
        return 1

    if args.name:
        part = await partition_at_site(site, args.name)
        if part is None:
            console.print(f'[red]Partition {args.name} not found on {args.site}[/]')
            return 1
        data = _partition_to_dict(part)
        if args.yaml:
            print_yaml(data)
        else:
            table = Table(show_header=False, box=None, pad_edge=False, padding=(0, 1))
            table.add_column(style='bold cyan', no_wrap=True)
            table.add_column()
            for key in ('name', 'site', 'created_at', 'updated_at'):
                table.add_row(key, str(data[key]))
            console.print(Panel(table,
                                title=f'[bold]Partition:[/] [green]{part.name}[/]',
                                border_style='cyan', expand=False))
        return 0

    group = None
    if args.group:
        group = await Group.find_one(Group.name == args.group)
        if group is None:
            console.print(f'[red]Group {args.group} not found[/]')
            return 1

    parts = await list_partitions_at_site(site, group=group)
    if not parts:
        console.print('[dim](no matching partitions)[/]')
        return 0

    data = [_partition_to_dict(p) for p in parts]
    if args.yaml:
        print_yaml(data)
        return 0

    title = f'Partitions on {args.site}'
    if args.group:
        title += f' for group {args.group}'
    table = Table(title=title)
    table.add_column('name', style='green')
    table.add_column('created_at', style='dim')
    for d in data:
        table.add_row(d['name'], str(d['created_at']))
    console.print(table)


@slurm_partition_show.args()
def _(parser: ArgParser):
    parser.add_argument('--name', '-n', default=None,
                        help='Specific partition to show; omit to list')
    parser.add_argument('--group', '-g', default=None,
                        help='Restrict to partitions this group has '
                             'associations on')
    parser.add_argument('--yaml', action='store_true', default=False,
                        help='Output as YAML')


# ---------------------------------------------------------------------------
# allocation show
# ---------------------------------------------------------------------------


def _alloc_to_dict(qa: QOSAllocation | None,
                   *,
                   alloc: SlurmAllocation | None = None) -> dict:
    """Render an allocation. If `qa` is given, includes QOS context;
    if only `alloc` is given (e.g. lookup-by-id without parent), QOS
    context is omitted."""
    a = alloc if qa is None else qa.allocation
    data = {
        'id': str(a.id),
        'tres': {'cpus': a.tres.cpus, 'gpus': a.tres.gpus, 'mem': a.tres.mem},
        'comment': a.comment,
        'created_at': a.created_at,
        'updated_at': a.updated_at,
        'provisioned_at': a.provisioned_at,
        'expires_at': a.expires_at,
    }
    if qa is not None:
        data['qos'] = qa.qos.name
        data['field'] = qa.field
    return data


@site_args.apply()
@commands.register('ng', 'slurm', 'allocation', 'show',
                   aliases=['list'],
                   help='Show a single allocation by id, or list allocations '
                        'at a site filtered by group/partition/qos/field')
async def slurm_allocation_show(args: Namespace):
    console = Console()

    # Direct lookup by id — no site needed.
    if args.id:
        try:
            oid = PydanticObjectId(args.id)
        except Exception:
            console.print(f'[red]{args.id!r} is not a valid ObjectId[/]')
            return 1
        alloc = await SlurmAllocation.get(oid)
        if alloc is None:
            console.print(f'[red]Allocation {args.id} not found[/]')
            return 1
        data = _alloc_to_dict(None, alloc=alloc)
        if args.yaml:
            print_yaml(data)
        else:
            table = Table(show_header=False, box=None, pad_edge=False, padding=(0, 1))
            table.add_column(style='bold cyan', no_wrap=True)
            table.add_column()
            for key in ('id', 'comment', 'created_at', 'updated_at'):
                table.add_row(key, str(data[key]))
            tres = data['tres']
            def _fmt(v) -> str:
                return '∞' if v is None else str(v)
            table.add_row(
                'tres',
                f'cpus={_fmt(tres["cpus"])}, gpus={_fmt(tres["gpus"])}, '
                f'mem={_fmt(tres["mem"])}',
            )
            for key in ('provisioned_at', 'expires_at'):
                value = data[key]
                table.add_row(key, str(value) if value is not None else '[dim](unset)[/]')
            console.print(Panel(table,
                                title=f'[bold]Allocation:[/] [green]{alloc.id}[/]',
                                border_style='cyan', expand=False))
        return 0

    # Filter-mode requires --site.
    if not args.site:
        console.print('[red]--site is required when not looking up by --id[/]')
        return 1
    site = await Site.find_one(Site.name == args.site)
    if site is None:
        console.print(f'[red]Site {args.site} not found[/]')
        return 1

    group = partition = qos = None
    if args.group:
        group = await Group.find_one(Group.name == args.group)
        if group is None:
            console.print(f'[red]Group {args.group} not found[/]')
            return 1
    if args.partition:
        partition = await partition_at_site(site, args.partition)
        if partition is None:
            console.print(f'[red]Partition {args.partition} not found on {args.site}[/]')
            return 1
    if args.qos:
        qos = await qos_at_site(site, args.qos)
        if qos is None:
            console.print(f'[red]QOS {args.qos} not found on {args.site}[/]')
            return 1

    qas = await list_allocations_at_site(
        site, group=group, partition=partition, qos=qos, field=args.field,
    )
    if not qas:
        console.print('[dim](no matching allocations)[/]')
        return 0

    if args.yaml:
        print_yaml([_alloc_to_dict(qa) for qa in qas])
        return 0

    def _fmt_dt(dt) -> str:
        return dt.strftime('%Y-%m-%d') if dt is not None else ''

    table = Table(title=f'Allocations on {args.site}')
    table.add_column('id', style='dim')
    table.add_column('qos', style='magenta')
    table.add_column('field', style='cyan')
    table.add_column('cpus')
    table.add_column('gpus')
    table.add_column('mem')
    table.add_column('provisioned', style='green')
    table.add_column('expires', style='yellow')
    table.add_column('comment', style='dim')
    def _fmt_tres(v) -> str:
        return '∞' if v is None else str(v)

    for qa in qas:
        a = qa.allocation
        table.add_row(
            str(a.id), qa.qos.name, qa.field,
            _fmt_tres(a.tres.cpus), _fmt_tres(a.tres.gpus), str(a.tres.mem or ''),
            _fmt_dt(a.provisioned_at), _fmt_dt(a.expires_at),
            a.comment,
        )
    console.print(table)


@slurm_allocation_show.args()
def _(parser: ArgParser):
    parser.add_argument('--id', default=None,
                        help='Show one allocation by ObjectId (skips other filters)')
    parser.add_argument('--group', '-g', default=None,
                        help='Restrict to QOSes this group has associations on')
    parser.add_argument('--partition', default=None,
                        help='Restrict to QOSes referenced by associations on '
                             'this partition')
    parser.add_argument('--qos', default=None,
                        help='Restrict to a single QOS')
    parser.add_argument('--field', default=None,
                        choices=list(_QOS_ALLOC_FIELDS),
                        help='Restrict to a single limit list')
    parser.add_argument('--yaml', action='store_true', default=False,
                        help='Output as YAML')


@site_args.apply(required=True)
@commands.register('ng', 'slurm', 'sync',
                   help="Reconcile a site's Slurm state onto the controller "
                        "via sacctmgr (dry-run unless --apply)")
async def slurm_sync(args: Namespace):
    console = Console()
    max_deletions = (
        None if args.max_deletions is not None and args.max_deletions < 0
        else args.max_deletions
    )

    # Offline mode: diff the site's desired state against captured
    # `sacctmgr show -P` dumps instead of a live controller. Preview only —
    # there's no controller to apply against.
    sacctmgr = None
    offline = bool(
        args.from_qos_dump or args.from_assoc_dump or args.from_user_dump
    )
    if offline:
        if args.apply:
            console.print(
                '[red]--apply cannot be combined with --from-*-dump; dump '
                'mode is a preview against captured output only.[/]'
            )
            return 1
        sacctmgr = DumpSAcctMgr.from_files(
            args.from_qos_dump, args.from_assoc_dump, args.from_user_dump,
        )

    try:
        result = await SyncSlurm.run(
            args.db, args.author,
            sitename=args.site,
            sacctmgr=sacctmgr,
            sudo=args.sudo,
            apply=args.apply,
            concurrency=args.concurrency,
            max_deletions=max_deletions,
            dump_commands=args.dump_commands,
        )
    except SlurmSyncAborted as e:
        console.print(f'[red]{e}[/]')
        for cmd in e.would_delete:
            console.print(f'  [yellow]{cmd}[/]')
        return 1

    plan = result['plan']
    if not plan:
        console.print(
            f'[green]Slurm state for {args.site} already in sync; '
            f'nothing to do.[/]'
        )
        return

    if offline:
        mode = 'DRY-RUN (offline; diffed against dump)'
    elif result['apply']:
        mode = 'APPLIED'
    else:
        mode = 'DRY-RUN (use --apply to execute)'
    console.print(f'[bold]Slurm sync for {args.site}[/] — {mode}')
    for label, cmds in plan.items():
        tally = result['tally'].get(label)
        suffix = (
            f' [dim](ok={tally["ok"]} failed={tally["failed"]})[/]'
            if tally else ''
        )
        console.print(f'\n[bold cyan]{label}[/] ({len(cmds)}){suffix}')
        for c in cmds:
            console.print(f'  {c}')
    if args.dump_commands:
        console.print(f'\nWrote commands to [green]{args.dump_commands}[/]')


@slurm_sync.args()
def _(parser: ArgParser):
    parser.add_argument('--apply', action='store_true', default=False,
                        help='Execute the reconciliation (default: dry-run preview)')
    parser.add_argument('--sudo', action='store_true', default=False,
                        help='Invoke sacctmgr via sudo')
    parser.add_argument('--concurrency', type=int, default=8,
                        help='Max concurrent sacctmgr commands per batch')
    parser.add_argument('--max-deletions', type=int, default=50,
                        help='Abort before applying if the plan would delete '
                             'more than this many entities; -1 disables the cap')
    parser.add_argument('--dump-commands', type=Path, default=None,
                        help='Write the planned sacctmgr commands to this file')
    parser.add_argument('--from-qos-dump', type=Path, default=None,
                        help='Read current QOS state from a saved '
                             '`sacctmgr show -P qos` dump instead of the '
                             'controller (offline preview; implies dry-run)')
    parser.add_argument('--from-assoc-dump', type=Path, default=None,
                        help='Read current association state from a saved '
                             '`sacctmgr show -P associations` dump instead of '
                             'the controller (offline preview; implies dry-run)')
    parser.add_argument('--from-user-dump', type=Path, default=None,
                        help='Read current user default accounts from a saved '
                             '`sacctmgr show -P user format=User,DefaultAccount` '
                             'dump instead of the controller (offline preview)')


# ---------------------------------------------------------------------------
# slurm account
# ---------------------------------------------------------------------------


def _account_limit_kwargs(args: Namespace) -> dict:
    return {
        'max_user_jobs': args.max_user_jobs,
        'max_group_jobs': args.max_group_jobs,
        'max_submit_jobs': args.max_submit_jobs,
        'max_job_length': args.max_job_length,
    }


@arggroup('slurm account', desc='Slurm account limits and coordinators')
def slurm_account_args(parser: ArgParser) -> None:
    parser.add_argument('--max-user-jobs', type=int, default=None,
                        help='Max concurrent jobs per user (-1 = unlimited)')
    parser.add_argument('--max-group-jobs', type=int, default=None,
                        help='Max concurrent jobs for the whole account '
                             '(-1 = unlimited)')
    parser.add_argument('--max-submit-jobs', type=int, default=None,
                        help='Max jobs the account may have queued at once '
                             '(-1 = unlimited)')
    parser.add_argument('--max-job-length', default=None,
                        help='Max wall time per job (e.g. "7-00:00:00"; '
                             '-1 = unlimited)')
    parser.add_argument('--coordinator', action='append', default=None,
                        dest='coordinators', metavar='USERNAME',
                        help='Account coordinator username (repeatable). On '
                             'edit, replaces the existing coordinator list.')


@group_args.apply(required=True)
@site_args.apply(required=True)
@slurm_account_args.apply()
@commands.register('ng', 'slurm', 'account', 'new',
                   help='Create the Slurm account for a group on a site')
async def slurm_account_new(args: Namespace):
    console = Console()
    await CreateSlurmAccount.run(
        args.db, args.author,
        site_name=args.site, group_name=args.group,
        coordinators=args.coordinators,
        **_account_limit_kwargs(args),
    )
    console.print(
        f'Created Slurm account for group [green]{args.group}[/] '
        f'on site [green]{args.site}[/]'
    )



@group_args.apply(required=True)
@site_args.apply(required=True)
@slurm_account_args.apply()
@commands.register('ng', 'slurm', 'account', 'edit',
                   help="Edit a group's Slurm account limits/coordinators "
                        'on a site')
async def slurm_account_edit(args: Namespace):
    console = Console()
    await EditSlurmAccount.run(
        args.db, args.author,
        site_name=args.site, group_name=args.group,
        coordinators=args.coordinators,
        **_account_limit_kwargs(args),
    )
    console.print(
        f'Updated Slurm account for group [green]{args.group}[/] '
        f'on site [green]{args.site}[/]'
    )

def _account_to_dict(account) -> dict:
    limits = account.limits
    coordinators = [
        c.name if hasattr(c, 'name') else str(c.ref.id)
        for c in account.coordinators
    ]
    return {
        'group': account.group.name if hasattr(account.group, 'name') else None,
        'site': account.site.name if hasattr(account.site, 'name') else None,
        'limits': {
            'max_user_jobs': limits.max_user_jobs,
            'max_group_jobs': limits.max_group_jobs,
            'max_submit_jobs': limits.max_submit_jobs,
            'max_job_length': limits.max_job_length,
        },
        'coordinators': coordinators,
    }


def _render_account_panel(data: dict) -> Panel:
    table = Table(show_header=False, box=None, pad_edge=False, padding=(0, 1))
    table.add_column(style='bold cyan', no_wrap=True)
    table.add_column()
    table.add_row('group', str(data['group']))
    table.add_row('site', str(data['site']))
    lim = data['limits']
    table.add_row(
        'limits',
        f"max_user_jobs={lim['max_user_jobs']}, "
        f"max_group_jobs={lim['max_group_jobs']}, "
        f"max_submit_jobs={lim['max_submit_jobs']}, "
        f"max_job_length={lim['max_job_length']}",
    )
    coords = data['coordinators']
    table.add_row(
        'coordinators', ', '.join(coords) if coords else '[dim](none)[/]',
    )
    return Panel(
        table,
        title=f'[bold]Slurm account:[/] [green]{data["group"]}[/] '
              f'@ [green]{data["site"]}[/]',
        border_style='cyan', expand=False,
    )


@group_args.apply(required=True)
@site_args.apply(required=True)
@commands.register('ng', 'slurm', 'account', 'show',
                   help="Show a group's Slurm account on a site")
async def slurm_account_show(args: Namespace):
    console = Console()
    site = await Site.find_one(Site.name == args.site)
    if site is None:
        console.print(f'[red]Site {args.site} not found[/]')
        return 1
    group = await Group.find_one(Group.name == args.group)
    if group is None:
        console.print(f'[red]Group {args.group} not found[/]')
        return 1
    account = await slurm_account_at_site(group, site, fetch_links=True)
    if account is None:
        console.print(
            f'[dim](no Slurm account for {args.group} on {args.site})[/]'
        )
        return 0
    data = _account_to_dict(account)
    if args.yaml:
        print_yaml(data)
    else:
        console.print(_render_account_panel(data))
    return 0


@slurm_account_show.args()
def _(parser: ArgParser):
    parser.add_argument('--yaml', action='store_true', default=False,
                        help='Output as YAML')
