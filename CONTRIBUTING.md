# Quickstart

Clone the repo and use a python installation to create a venv:

```sh
git clone git@github.com:RaRe-Technologies/smart_open.git
cd smart_open
python -m venv .venv
```

Activate the venv to start working and install test deps:

```sh
.venv/bin/activate
pip install -e ".[test]"
```

Tests should pass:

```sh
pytest
```

That's it! When you're done, deactivate the venv:

```sh
deactivate
```
