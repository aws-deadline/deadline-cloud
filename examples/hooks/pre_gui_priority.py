#!/usr/bin/env python3
"""Pre-GUI hook: set job priority based on deadline proximity.

Place this script alongside a hooks.yaml in your job bundle:

    hooks.yaml
    ----------
    version: "1.0"
    preGUI:
      - command: python3
        args: [pre_gui_priority.py]
        timeout: 5

The hook reads the current date and sets priority 90 if a deadline is
approaching (within 2 days), otherwise 50. It also pre-fills the job name
with a datestamp so artists can see when the job was submitted.
"""

import json
import sys
from datetime import date

TODAY = date.today()
DEADLINE = date(TODAY.year, TODAY.month, 28)  # example: end-of-month deadline
DAYS_REMAINING = (DEADLINE - TODAY).days

if DAYS_REMAINING <= 2:
    priority = 90
    name_suffix = " [URGENT]"
elif DAYS_REMAINING <= 7:
    priority = 70
    name_suffix = " [due soon]"
else:
    priority = 50
    name_suffix = ""

metadata = json.load(sys.stdin)
job_name = metadata.get("jobName", "Job")

output = {
    "name": f"{job_name}{name_suffix} ({TODAY.isoformat()})",
    "parameters": {
        "deadline:priority": priority,
    },
}

json.dump(output, sys.stdout)
