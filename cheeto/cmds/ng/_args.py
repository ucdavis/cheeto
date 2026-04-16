"""Shared argparse argument groups for the `cheeto ng` command tree.

Each arggroup adds a single flag argument to a parser so commands can
compose them via decorator stacking, matching the convention established
by the old `cheeto db` commands.
"""

from ponderosa import ArgParser, arggroup


@arggroup('site')
def site_args(parser: ArgParser, required: bool = False):
    parser.add_argument('--site', '-s', default=None, required=required,
                        help='Site name')


@arggroup('user')
def user_args(parser: ArgParser, required: bool = False):
    parser.add_argument('--user', '-u', default=None, required=required,
                        help='Username')


@arggroup('group')
def group_args(parser: ArgParser, required: bool = False):
    parser.add_argument('--group', '-g', default=None, required=required,
                        help='Group name')


@arggroup('email')
def email_args(parser: ArgParser,
               required: bool = True,
               default: str | None = None):
    if default is not None:
        parser.add_argument('--email', default=default,
                            help='Email address')
    else:
        parser.add_argument('--email', required=required, default=None,
                            help='Email address')


@arggroup('fullname')
def fullname_args(parser: ArgParser, required: bool = True):
    parser.add_argument('--fullname', default=None, required=required,
                        help='Full display name')


@arggroup('password')
def password_args(parser: ArgParser):
    parser.add_argument('--password', action='store_true', default=False,
                        help='Generate a random password for the user')
