#!/usr/bin/env python3
"""Add a new section on the top of CHANGELOG.md.

Usage:
    python release/update_changelog.py 7.2.0

This creates a new section "# 7.2.0, {date}" in CHANGELOG.md
based on the GitHub diff {latest_version_in_changelog}...develop.
"""

import json
import sys
from datetime import datetime
from pathlib import Path

owner = "piskvorky"
repo = "smart_open"
head_branch = "develop"

# get the new version (used for the header only)
if len(sys.argv) != 2:
    msg = "Use `python release/update_changelog.py X.Y.Z` to generate a new CHANGELOG.md entry before releasing vX.Y.Z"
    raise ValueError(msg)
new_version = sys.argv[1].removeprefix("v")

# read current CHANGELOG.md
changelog_path = Path(__file__).parents[1] / "CHANGELOG.md"
changelog_lines = changelog_path.read_text().strip().splitlines()

# extract the latest version from CHANGELOG.md's first line
if not changelog_lines[0].startswith("# "):
    msg = "First line of CHANGELOG.md is not a header"
    raise ValueError(msg)
latest_version = changelog_lines[0].split(",", 1)[0].removeprefix("# ")


def get_json(url):
    """Perform a GET + json.loads."""
    import requests
    print("Requesting", url)
    resp = requests.get(url)
    resp.raise_for_status()
    return json.loads(resp.text)

# fetch diff for {latest_version}...{head_branch}
diff = get_json(
    f"https://api.github.com/repos/{owner}/{repo}/compare/v{latest_version}...{head_branch}",
)

# iterate over diff commits and fetch corresponding PRs (if any)
new_changelog_lines = []
for commit in diff["commits"]:
    pulls = get_json(
        f"https://api.github.com/repos/{owner}/{repo}/commits/{commit['sha']}/pulls",
    )
    if not pulls:
        continue
    pull = pulls[0]
    title = pull["title"]
    pull_number = pull["number"]
    pull_url = pull["html_url"]
    user_name = pull["user"]["login"]
    user_url = pull["user"]["html_url"]
    new_changelog_lines.append(
        f"- {title} (PR [#{pull_number}]({pull_url}), [@{user_name}]({user_url}))"
    )

if new_changelog_lines:
    print("Writing", changelog_path)
    date = datetime.now().strftime("%Y-%m-%d")
    new_changelog = [
        f"# {new_version}, {date}",
        "",
        *new_changelog_lines,
        "",
        *changelog_lines,
    ]
    changelog_path.write_text("\n".join(new_changelog))
