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
set -euxo pipefail

version="$1"
echo "version: $version"

script_dir="$(dirname "${BASH_SOURCE[0]}")"
cd "$script_dir"

git fetch upstream

#
# Using the current environment, that has smart_open installed
#
cd ..
python -m doctest README.rst
cd "$script_dir"

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
echo "__version__ = '$version'" > smart_open/version.py
git commit smart_open/version.py -m "bump version to $version"

echo "Next, update CHANGELOG.md."
echo "Consider running summarize_pr.sh for each PR merged since the last release."
read -p "Press Enter to continue..."

${EDITOR:-vim} CHANGELOG.md
set +e
git commit CHANGELOG.md -m "updated CHANGELOG.md for version $version"
set -e

python -c 'help("smart_open")' > help.txt

#
# The below command will fail if there are no changes to help.txt.
# We can safely ignore that failure.
#
set +e
git commit help.txt -m "updated help.txt for version $version"
set -e

echo "Have a look at the current branch, and if all looks good, run merge.sh"

cd "$script_dir"
rm -rf sandbox.venv
