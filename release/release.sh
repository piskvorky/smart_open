#!/usr/bin/env bash
# See release/README.md for usage

set -euxo pipefail

read -p "Enter new version for the CHANGELOG.md header: " new_version

# Make sure you're on `develop` and you're up to date locally
git checkout develop
git pull
# Prepare `CHANGELOG.md` for the new release
python release/update_changelog.py "${new_version}"
# Commit `CHANGELOG.md` to `develop` and push
git add CHANGELOG.md
git commit -m "Update CHANGELOG.md"
git push
# Make sure you're on `master` and you're up to date locally
git checkout master
git pull
# Merge `develop` into `master` and push
git pull . develop --no-ff --no-edit
git push
# Open the new GitHub Release page for convenience
new_release_url="https://github.com/piskvorky/smart_open/releases/new"
echo "Follow release/README.md instructions: opening ${new_release_url}..."
sleep 1
xdg-open "${new_release_url}" || open "${new_release_url}"  # linux || macos
