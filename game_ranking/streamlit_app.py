import sys
import os
import runpy

_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _here)

runpy.run_path(os.path.join(_here, "app", "main.py"), run_name="__main__")
