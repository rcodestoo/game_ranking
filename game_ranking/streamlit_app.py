import sys
import os
import runpy

_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _here)

# Python 3.13 leaves None sentinels in sys.modules when an import fails mid-way.
# On the next hot-reload, runpy.run_path trips over those sentinels with KeyError.
# Clear all project-local module entries before each run so imports start clean.
_LOCAL_PREFIXES = ('calculation', 'pipelines', 'app', 'config')
for _key in list(sys.modules.keys()):
    if _key.split('.')[0] in _LOCAL_PREFIXES:
        del sys.modules[_key]

runpy.run_path(os.path.join(_here, "app", "main.py"), run_name="__main__")
