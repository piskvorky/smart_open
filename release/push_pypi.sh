#
# Upload the current release of smart_open to PyPI.
# Run this _after_ you've run the other scripts, e.g. prepare.sh and merge.sh.
#
set -euo pipefail

version="$SMART_OPEN_RELEASE"

script_dir="$(dirname "${BASH_SOURCE[0]}")"
cd "$script_dir"

rm -rf sandbox.venv
virtualenv sandbox.venv -p $(which python3)

set +u  # work around virtualenv awkwardness
source sandbox.venv/bin/activate
set -u

cd ..
pip install twine
python setup.py sdist

read -p "Push version $version to PyPI? This step is non-reversible.  Answer yes or no: " reply
if [ "$reply" != "yes" ]
then
    echo "aborted by user"
    exit 1
fi
twine upload "dist/smart_open-$version.tar.gz"

cd "$script_dir"
rm -rf sandbox.venv
