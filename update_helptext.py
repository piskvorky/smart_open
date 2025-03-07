#!/usr/bin/env python3
"""Write out help.txt based on the current codebase."""

import subprocess
from pathlib import Path

# get the latest helptext
helptext = subprocess.check_output(
    ["/usr/bin/env", "python3", "-c", 'help("smart_open")'],
    text=True,
).strip()

# remove the user-specific FILE and VERSION section at the bottom to make this script reproducible
lines = helptext.splitlines()[:-5]

Path("help.txt").write_text("\n".join(lines))
