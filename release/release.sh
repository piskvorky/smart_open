#!/usr/bin/env bash
# See release/README.md for usage

set -euxo pipefail

# Only allow the direct successors of the latest vX.Y.Z[.postN] tag: patch (default), minor, major, post release
git fetch --tags
read -ra allowed < <(git tag | python3 -c '
import re, sys
tags = [re.fullmatch(r"v(\d+)\.(\d+)\.(\d+)(?:\.post(\d+))?", t.strip()) for t in sys.stdin]
x, y, z, p = max(tuple(map(int, m.groups("0"))) for m in tags if m)
print(f"v{x}.{y}.{z + 1} v{x}.{y + 1}.0 v{x + 1}.0.0 v{x}.{y}.{z}.post{p + 1}")
')
read -p "Enter new version for the CHANGELOG.md header, one of ${allowed[*]} [${allowed[0]}]: " new_version
new_version="${new_version:-${allowed[0]}}"

# Ensure version has 'v' prefix
if [[ ! "${new_version}" == v* ]]; then
    new_version="v${new_version}"
fi
if [[ ! " ${allowed[*]} " == *" ${new_version} "* ]]; then
    echo "Refusing to release ${new_version}: expected one of ${allowed[*]}" >&2
    exit 1
fi
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
git merge develop --no-ff -m "Merge branch 'develop' into master"
git push
# Make sure you're on `develop` and you're up to date locally
git checkout develop
git pull
# Merge `master` back into `develop` and push
git merge master --no-ff -m "Merge branch 'master' into develop"
git push
# Open the new GitHub Release page for convenience
new_release_url="https://github.com/piskvorky/smart_open/releases/new?target=master&tag=${new_version}"
echo "Follow release/README.md instructions: opening ${new_release_url}..."
sleep 1
xdg-open "${new_release_url}" || open "${new_release_url}"  # linux || macos
