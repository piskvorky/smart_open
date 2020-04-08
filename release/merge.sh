#
# Performs the following tasks:
#
# - Merges the current release branch into develop
# - Applies a tag to develop
# - Pushes the updated develop branch and its tag to upstream
#
# Use it like this:
#
#     bash release/merge.sh
#
# Expects smart_open/version.py to be correctly incremented for the new release.
#
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

#
# env -i python seems to return the wrong Python version on MacOS...
#
my_python=$(which python3)
version=$($my_python -c "from smart_open.version import __version__; print(__version__)")

read -p "Push version $version to github.com? yes or no: " reply
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
set -e

git checkout upstream/develop -b develop
git merge --no-ff release-${version}
git tag -a ${version} -m "${version}"
git push --tags upstream develop

#
# TODO: we should be able to automate the release note stuff.  It's just a
# copypaste of CHANGELOG.md.
#
echo "The release is almost done!  Two more steps to go:"
echo "1) Update release notes at https://github.com/RaRe-Technologies/smart_open/releases/tag/$version"
echo "2) Push the new release to PyPI: run 'bash push_pypi.sh'"
