script_dir="$(dirname "${BASH_SOURCE[0]}")"

#
# Using the current environment, which has smart_open installed.
#
cd "$script_dir/.."
python -c 'help("smart_open")' > help.txt
git commit help.txt -m "updated help.txt"
