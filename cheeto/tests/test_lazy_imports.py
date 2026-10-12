"""PEP 810 lazy-import tripwire (Python 3.15+).

``cheeto/__init__.py`` scopes lazy imports to cheeto's own import statements
and switches them on under ``CHEETO_LAZY_IMPORTS=1``. Every import-time
registry (the ponderosa command tree, the celery tasks, the Operation
registry, the beanie model rebuilds) must look identical in eager and lazy
mode, and ``cheeto --help`` must not load the heavy dependencies.

Lazy state is process-global and the rest of the suite reifies everything,
so each probe runs in a fresh subprocess.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    not hasattr(sys, 'set_lazy_imports'),
    reason='PEP 810 lazy imports need Python 3.15+',
)

REPO = Path(__file__).resolve().parents[2]

TOP_COMMANDS = {'config', 'daemon', 'help', 'ipython', 'monitor', 'ng',
                'nocloud', 'puppet'}
TASKS = {'cheeto.hippo_process', 'cheeto.iam_sync', 'cheeto.reap',
         'cheeto.ldap_sync', 'cheeto.slurm_sync', 'cheeto.sympa_export',
         'cheeto.puppet_sync'}
# Packages that `cheeto --help` must not load once lazy imports are on.
HEAVY = ('pymongo', 'beanie', 'celery', 'bonsai', 'IPython', 'sh')

# Everything lives inside main() so the probe's own imports are eager under
# any mode; only cheeto's import statements are under test.
PROBE = r'''
def main():
    import argparse, contextlib, io, json, sys
    from cheeto.cmds.__main__ import commands
    # Read the mode after the cheeto import: cheeto/__init__.py is what
    # installs the filter and flips the switch.
    out = {'mode': sys.get_lazy_imports(),
           'filter': sys.get_lazy_imports_filter() is not None}

    def subcommands(parser):
        for action in parser._actions:
            if isinstance(action, argparse._SubParsersAction):
                return sorted(action.choices)
        return []

    with contextlib.redirect_stdout(io.StringIO()), contextlib.suppress(SystemExit):
        commands.run(['--help'])
    out['heavy_after_help'] = [m for m in __HEAVY__ if m in sys.modules]
    out['top'] = subcommands(commands.root.parser)
    out['ng'] = sorted(p.prog for p in commands.gather_subtree('ng'))

    from cheeto.daemon.app import app
    out['tasks'] = sorted(t for t in app.tasks if t.startswith('cheeto.'))

    from cheeto.operations import operation_names
    out['ops'] = operation_names()

    from cheeto.models import ALL_MODELS
    out['incomplete_models'] = [m.__name__ for m in ALL_MODELS
                                if not m.__pydantic_complete__]
    print(json.dumps(out))

main()
'''.replace('__HEAVY__', repr(HEAVY))


def probe(**env: str) -> dict:
    base = {k: v for k, v in os.environ.items()
            if k not in ('CHEETO_LAZY_IMPORTS', 'PYTHON_LAZY_IMPORTS')}
    proc = subprocess.run([sys.executable, '-c', PROBE], cwd=REPO,
                          env=base | env, capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


@pytest.fixture(scope='module')
def eager() -> dict:
    return probe(CHEETO_LAZY_IMPORTS='0')


def test_eager_probe_is_normal(eager):
    assert eager['mode'] == 'normal'
    assert eager['filter'], 'the scoping filter is installed even when off'
    assert TOP_COMMANDS <= set(eager['top'])
    assert TASKS <= set(eager['tasks'])
    assert eager['ops'] and eager['ng']
    assert eager['incomplete_models'] == []


@pytest.mark.parametrize('env', [
    pytest.param({'CHEETO_LAZY_IMPORTS': '1'}, id='self-enabled'),
    pytest.param({'PYTHON_LAZY_IMPORTS': 'all'}, id='env-all'),
])
def test_registries_survive_lazy_imports(env, eager):
    lazy = probe(**env)
    assert lazy['mode'] == 'all'
    assert lazy['filter']
    for key in ('top', 'ng', 'tasks', 'ops'):
        assert lazy[key] == eager[key], key
    assert lazy['incomplete_models'] == []
    # The point of the exercise: `cheeto --help` stays light. A leak here
    # means an arg adder touches a heavy name at decoration time.
    assert lazy['heavy_after_help'] == []


def test_explicit_normal_wins_over_switch():
    out = probe(CHEETO_LAZY_IMPORTS='1', PYTHON_LAZY_IMPORTS='normal')
    assert out['mode'] == 'normal'
