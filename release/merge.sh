#
# This script performs the following tasks:
#
# - Merges the current release branch into master
# - Applies a tag to master
# - Merges
# - Pushes the updated master branch and its tag to upstream
#
# - develop: Our development branch.  We merge all PRs into this branch.
# - release-$version: A local branch containing commits specific to this release.
#   This is a local-only branch, we never push this anywhere.
# - master: Our "clean" release branch.  Contains tags.
#
# The relationships between the three branches are illustrated below:
#
#   github.com PRs
#          \
# develop --+--+----------------------------------+---
#               \                                /
#   (new branch) \ commits (CHANGELOG.md, etc)  /
#                 \   v                        /
# release          ---*-----X (delete branch) / (merge 2)
#                         \                  /
#                (merge 1) \       TAG      /
#                           \       v      /
# master  -------------------+------*-----+-----------
#
# Use it like this:
#
#     bash release/merge.sh
#
# Expects smart_open/version.py to be correctly incremented for the new release.
#
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

version="$(python smart_open/version.py)"

read -p "Push version $version to github.com and PyPI? yes or no: " reply
if [ "$reply" != "yes" ]
then
    echo "aborted by user"
    exit 1
fi

#
# Delete the local develop branch in case one is left lying around.
#
set +e
git branch -D develop
git branch -D master
set -e

git checkout upstream/master -b master
git merge --no-ff release-${version}
git tag -a "v${version}" -m "v${version}"

git checkout upstream/develop -b develop
git merge --no-ff master

#
# N.B. these push steps are non-reversible.
#
git checkout master
git push --tags upstream master

git checkout develop
dev_version="$version.dev0"
sed -i '' s/$(python smart_open/version.py)/$dev_version/ smart_open/version.py
git commit smart_open/version.py -m "bump version to $dev_version"
git push upstream develop

python release/update_release_notes.py "$version"
