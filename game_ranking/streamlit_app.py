import sys
import os
import runpy
import logging

_here = os.path.dirname(os.path.abspath(__file__))

# ── Logging setup ─────────────────────────────────────────────────────────────
# Configures INFO-level logging for all project modules to stderr (terminal).
# Runs once per process; idempotent on hot-reload because of the handler check.
def _setup_logging() -> None:
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )
    handler = logging.StreamHandler()
    handler.setFormatter(fmt)
    for name in ("calculation", "pipelines", "app"):
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            logger.addHandler(handler)
            logger.propagate = False  # don't double-log via Streamlit's root handler

_setup_logging()
sys.path.insert(0, _here)

# Python 3.13 leaves None sentinels in sys.modules when an import fails mid-way.
# On the next hot-reload, runpy.run_path trips over those sentinels with KeyError.
# Clear all project-local module entries before each run so imports start clean.
_LOCAL_PREFIXES = ('calculation', 'pipelines', 'app', 'config')
for _key in list(sys.modules.keys()):
    if _key.split('.')[0] in _LOCAL_PREFIXES:
        del sys.modules[_key]

runpy.run_path(os.path.join(_here, "app", "main.py"), run_name="__main__")
