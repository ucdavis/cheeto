"""Grammar tripwire: every `cheeto ng` leaf command must follow the
`category verb object` grammar — the token after the category is one of
the sanctioned verbs — or be an explicitly documented task exception.
Guards the CLI normalization against drift.

Grammar:
  new    — create a standalone document (`user new class`, `slurm new qos`)
  delete — destroy a document and cascade its references (`user delete`)
  add    — attach a relationship or sub-resource (`group add member`)
  remove — detach one (`user remove access`)
  set    — overwrite a single property (`user set status`)
  edit   — multi-field modification (`slurm edit qos`)
  show   — single-object view (dual-mode: omitted identifier lists)
  list   — collection view, plural object token (`storage list volumes`)
  sync   — reconcile against an external system (`ldap sync user`)
  export — render state for an external consumer (`site export root-keys`)
"""

import argparse

VERBS = {
    'new', 'delete', 'add', 'remove', 'set', 'edit', 'show', 'list',
    'sync', 'export',
}

# Operational task commands that don't map onto the CRUD verbs. Keep this
# list short and deliberate — anything new should justify itself here.
EXCEPTIONS = {
    ('history',),
    ('iam', 'reap'),
    ('ldap', 'bootstrap'),
    ('ldap', 'backfill'),
    ('ldap', 'prune'),
    ('ldap', 'clear-tree'),
    ('hippo', 'process'),
    ('slurm', 'provision'),
    ('storage', 'rehome'),
    ('storage', 'backfill-hosts'),
    ('group', 'seed-access-status'),
    ('group', 'seed-type'),
    ('group', 'backfill-sites'),
    ('user', 'backfill-sites'),
    ('user', 'clear-offboarding-site-statuses'),
    ('user', 'redundant-site-statuses'),
}


def _is_container(parser):
    return any(isinstance(a, argparse._SubParsersAction)
               for a in parser._actions)


def _path_tokens(parser):
    toks = parser.prog.split()
    return tuple(toks[toks.index('ng') + 1:]) if 'ng' in toks else tuple(toks)


def test_ng_leaves_follow_grammar():
    import cheeto.cmds.ng  # noqa: F401 — registers the full ng command tree
    from cheeto.cmds import commands

    parsers = commands.gather_subtree('ng')
    leaves = [p for p in parsers if not _is_container(p)]
    assert leaves, 'expected to discover ng leaf commands'

    violations = []
    for p in leaves:
        path = _path_tokens(p)
        if not path:
            continue   # bare `ng`
        if path in EXCEPTIONS:
            continue
        # category [sub...] verb [object]: token after the category must be
        # a sanctioned verb.
        if len(path) < 2 or path[1] not in VERBS:
            violations.append(' '.join(path))

    assert not violations, (
        'ng commands violating the category-verb-object grammar '
        f'(add a verb path or a documented EXCEPTION): {sorted(violations)}'
    )


# Object tokens under `ng slurm` that must carry a short alias on every
# verb they appear under, so `slurm show part` works wherever
# `slurm show partition` does.
SLURM_OBJECT_ALIASES = {
    'allocation': 'alloc',
    'association': 'assoc',
    'partition': 'part',
}


def test_ng_slurm_objects_have_matching_aliases():
    import cheeto.cmds.ng  # noqa: F401 — registers the full ng command tree
    from cheeto.cmds import commands

    missing = []
    for parser in commands.gather_subtree('ng'):
        path = _path_tokens(parser)
        if not path or path[0] != 'slurm':
            continue
        for action in parser._actions:
            if not isinstance(action, argparse._SubParsersAction):
                continue
            # argparse maps every name and alias to the same subparser
            # object, so group the names by that object.
            names_by_parser: dict[int, set[str]] = {}
            for name, sub in action.choices.items():
                names_by_parser.setdefault(id(sub), set()).add(name)
            for names in names_by_parser.values():
                for obj, alias in SLURM_OBJECT_ALIASES.items():
                    if obj in names and alias not in names:
                        missing.append(f'{" ".join(path)} {obj} (needs {alias})')

    assert not missing, (
        f'ng slurm commands missing their object alias: {sorted(missing)}'
    )
