#!/usr/bin/env -S uv run --script
# /// script
# requires-python = "==3.10.*"
# dependencies = ["smart_open[all]"]
# [tool.uv.sources]
# smart_open = { path = ".", editable = true }
# ///
"""Write out help.txt based on the current codebase.

Run it as ./update_helptext.py, which needs uv: python update_helptext.py ignores the Python and extras pinned above.
"""

import pydoc
from pathlib import Path

import smart_open

# open() documents each transport whose dependencies are installed, and pydoc formats signatures per Python version, so the script pins both
helptext = pydoc.render_doc(smart_open, "Help on %s:", renderer=pydoc.plaintext).strip()

# remove the user-specific FILE and VERSION section at the bottom to make this script reproducible
lines = helptext.splitlines()[:-5]

Path(__file__).with_name("help.txt").write_text("\n".join(line.rstrip() for line in lines))
