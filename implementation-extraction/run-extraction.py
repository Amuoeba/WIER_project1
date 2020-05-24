# Imports from external libraries
import sys
# Imports from internal libraries
from automatic_extractor import run as auto_run
from regex_extractor import run as regex_run
from xpath_extractor import run as xpath_run


run_modes = ["A","B","C"]

mode = sys.argv[1]
if mode not in run_modes:
    raise ValueError(f"Wrong extraction mode. Available modes are: {run_modes}")
print(f"Running extraction {mode}")

if mode == "A":
    regex_run()
elif mode == "B":
    xpath_run()
elif mode == "C":
    auto_run()
else:
    raise ValueError(f"Wrong extraction mode. Available modes are: {run_modes}")