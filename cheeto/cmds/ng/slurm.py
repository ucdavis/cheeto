from argparse import Namespace

from ponderosa import ArgParser

from .. import commands
from ...log import Console
from ...operations import CreateSlurmAssociation, CreateSlurmPartition, CreateSlurmQOS
from ._args import group_args, site_args


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
    qos = await CreateSlurmQOS.run(
        args.db, args.author,
        name=args.name, site_name=args.site,
        priority=args.priority, flags=args.flags,
    )
    console.print(f'Created QOS [green]{qos.name}[/] on site {args.site}')


@slurm_qos_new.args()
def _(parser: ArgParser):
    parser.add_argument('--name', '-n', required=True)
    parser.add_argument('--priority', type=int, default=0)
    parser.add_argument('--flags', nargs='+', default=None)


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
