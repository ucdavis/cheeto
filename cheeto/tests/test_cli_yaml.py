"""Coverage tripwire: every informational `cheeto ng` command (a runnable
show/list leaf) must accept --yaml. Guards against new show/list commands
shipping without machine-readable output."""

import argparse


def _is_container(parser):
    return any(isinstance(a, argparse._SubParsersAction) for a in parser._actions)


def _has_yaml(parser):
    return any('--yaml' in getattr(a, 'option_strings', [])
               for a in parser._actions)


def _path_tokens(parser):
    toks = parser.prog.split()
    return toks[toks.index('ng') + 1:] if 'ng' in toks else toks


def test_show_list_commands_support_yaml():
    import cheeto.cmds.ng  # noqa: F401 — registers the full ng command tree
    from cheeto.cmds import commands

    parsers = commands.gather_subtree('ng')
    # Informational leaf = a runnable command (not a subcommand container)
    # with 'show' or 'list' somewhere in its path.
    info = [
        p for p in parsers
        if not _is_container(p) and ({'show', 'list'} & set(_path_tokens(p)))
    ]
    assert info, 'expected to discover ng show/list commands'

    missing = sorted(' '.join(_path_tokens(p)) for p in info if not _has_yaml(p))
    assert not missing, f'show/list commands missing --yaml: {missing}'
