# Quickstart

Clone the repo and use a python installation to create a venv:

```sh
git clone git@github.com:RaRe-Technologies/smart_open.git
cd smart_open
python -m venv .venv
```

Activate the venv to start working and install test deps + pre-commit hooks:

```sh
. .venv/bin/activate
make install
```

Run the test suite:

```sh
make test
```

Run linting (ruff, pydoclint, yamlfmt, etc.) — the same hooks CI runs:

```sh
make lint
```

See all targets:

```sh
make help
```

That's it! When you're done, deactivate the venv:

```sh
deactivate
```
