#
# Prepare a new release of smart_open.  Use it like this:
#
#     bash release/prepare.sh 1.2.3
#
# where 1.2.3 is the new version to release.
#
# Does the following:
#
# - Creates a clean virtual environment
# - Runs tests
# - Creates a local release git branch
# - Bumps VERSION accordingly
# - Opens CHANGELOG.md for editing, commits updates
#
# Once you're happy, run merge.sh to continue with the release.
#
set -euo pipefail

version="$1"
echo "version: $version"

script_dir="$(dirname "${BASH_SOURCE[0]}")"
cd "$script_dir"

git fetch upstream

rm -rf sandbox.venv
virtualenv sandbox.venv -p $(which python3)

set +u  # work around virtualenv awkwardness
source sandbox.venv/bin/activate
set -u

cd ..
pip install -e .[test]  # for smart_open
pip install .[test]  # for gensim
python setup.py test  # for gensim

#
# Delete the release branch in case one is left lying around.
#
git checkout upstream/master
set +e
git branch -D release-"$version"
set -e

git checkout upstream/master -b release-"$version"
echo "$version" > smart_open/VERSION
git commit smart_open/VERSION -m "bump version to $version"

echo "Next, update CHANGELOG.md."  
echo "Consider running summarize_pr.sh for each PR merged since the last release."
read -p "Press Enter to continue..."

$EDITOR CHANGELOG.md
git commit CHANGELOG.md -m "updated CHANGELOG.md for version $version"

echo "Have a look at the current branch, and if all looks good, run merge.sh"

cd "$script_dir"
rm -rf sandbox.venv
