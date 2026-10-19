__version__ = '2.7.1'


def _setup_lazy_imports() -> None:
    """PEP 810 lazy imports (Python 3.15+): defer cheeto's own module-level
    imports until first use, so `cheeto --help` does not load pymongo, beanie,
    celery or bonsai.

    A filter scopes laziness to import statements *written in* cheeto modules;
    third-party packages still import eagerly inside themselves, so their
    import-time registries keep working. A cheeto module whose imports exist
    for their side effects sets ``__eager_imports__ = True`` before them; on a
    package ``__init__`` the marker covers every module in that package.
    Registration-only imports use ``importlib.import_module`` (always eager).

    Switch on with CHEETO_LAZY_IMPORTS=1 (the Docker image does). Off with
    CHEETO_LAZY_IMPORTS=0, or an explicit PYTHON_LAZY_IMPORTS=normal /
    -X lazy_imports=normal. The filter is installed whenever the interpreter
    supports it, so an operator's -X lazy_imports=all is scoped the same way.
    No-op before Python 3.15.
    """
    import os
    import sys

    if not hasattr(sys, 'set_lazy_imports_filter'):
        return

    if sys.get_lazy_imports_filter() is None:
        def cheeto_lazy_imports_filter(importing_module, imported_module, fromlist):
            if not importing_module:
                return False
            if importing_module != 'cheeto' and not importing_module.startswith('cheeto.'):
                return False
            # The importing module and each enclosing package already sit in
            # sys.modules when their bodies run, so a marker set before the
            # imports is visible here.
            name = importing_module
            while name:
                mod = sys.modules.get(name)
                if mod is not None and vars(mod).get('__eager_imports__', False):
                    return False
                name = name.rpartition('.')[0]
            return True

        sys.set_lazy_imports_filter(cheeto_lazy_imports_filter)

    explicit_normal = (os.environ.get('PYTHON_LAZY_IMPORTS') == 'normal'
                       or sys._xoptions.get('lazy_imports') == 'normal')
    wanted = os.environ.get('CHEETO_LAZY_IMPORTS', '0').lower() in ('1', 'true', 'yes', 'on')
    if wanted and not explicit_normal and sys.get_lazy_imports() != 'all':
        sys.set_lazy_imports('all')


_setup_lazy_imports()
