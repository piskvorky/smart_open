script_dir="$(dirname "${BASH_SOURCE[0]}")"

export AWS_ACCESS_KEY_ID=$(aws --profile smart_open configure get aws_access_key_id)
export AWS_SECRET_ACCESS_KEY=$(aws --profile smart_open configure get aws_secret_access_key)

#
# Using the current environment, which has smart_open installed.
#
cd "$script_dir/.."
python -m doctest README.rst
