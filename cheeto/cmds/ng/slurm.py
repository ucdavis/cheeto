from argparse import Namespace

from ponderosa import ArgParser

from .. import commands
from ...args import regex_argtype
from ...constants import QOS_TRES_REGEX
from ...log import Console
from ...models.slurm import SlurmAllocation, SlurmTRES
from ...operations import (
    AddQOSAllocation,
    CreateSlurmAssociation,
    CreateSlurmPartition,
    CreateSlurmQOS,
    EditSlurmAllocation,
)
from ...types import parse_qos_tres
from ._args import group_args, site_args


_QOS_ALLOC_FIELDS = ('group_limits', 'user_limits', 'job_limits')


def _tres_from_string(s: str | None) -> SlurmTRES | None:
    if s is None:
        return None
    return SlurmTRES(**parse_qos_tres(s))


@commands.register('ng', 'slurm',
                   help='Slurm resource operations')
def slurm_cmd(args: Namespace):
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


@commands.register('ng', 'slurm', 'allocation',
                   help='Slurm allocation operations')
def slurm_allocation_cmd(args: Namespace):
    pass


@site_args.apply(required=True)
@commands.register('ng', 'slurm', 'allocation', 'add',
                   help='Add an allocation to an existing QOS')
async def slurm_allocation_add(args: Namespace):
    console = Console()
    tres = _tres_from_string(args.tres)
    alloc = await AddQOSAllocation.run(
        args.db, args.author,
        qos_name=args.qos, site_name=args.site,
        field=args.field, tres=tres, comment=args.comment,
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


@commands.register('ng', 'slurm', 'allocation', 'edit',
                   help='Edit an existing allocation by id')
async def slurm_allocation_edit(args: Namespace):
    console = Console()
    tres = _tres_from_string(args.tres)
    alloc = await EditSlurmAllocation.run(
        args.db, args.author,
        allocation_id=args.id, tres=tres, comment=args.comment,
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
