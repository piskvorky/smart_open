#
# Prepare a new release of smart_open.  Use it like this:
#
#     export SMART_OPEN_RELEASE=2.3.4
#     bash release/prepare.sh
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

version="$SMART_OPEN_RELEASE"
echo "version: $version"

script_dir="$(dirname "${BASH_SOURCE[0]}")"
cd "$script_dir"

#
# We will need these for the doctests.
#
export AWS_ACCESS_KEY_ID=$(aws --profile smart_open configure get aws_access_key_id)
export AWS_SECRET_ACCESS_KEY=$(aws --profile smart_open configure get aws_secret_access_key)

git fetch upstream

#
# Using the current environment, that has smart_open installed
#
cd ..
python -m doctest README.rst
cd "$script_dir"

#
# These seem to be messing with moto, so get rid of them
#
export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=

rm -rf sandbox.venv
virtualenv sandbox.venv -p $(which python3)

set +u  # work around virtualenv awkwardness
source sandbox.venv/bin/activate
set -u

cd ..
pip install -e .[all,test]
pytest smart_open

#
# Delete the release branch in case one is left lying around.
#
git checkout upstream/develop
set +e
git branch -D release-"$version"
set -e

git checkout upstream/develop -b release-"$version"
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
