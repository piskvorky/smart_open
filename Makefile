.PHONY: lint
## Run linting
lint:
	pre-commit run --all-files

.PHONY: test
## Run tests
test:
	python -m pytest tests -v

.PHONY: install
## Install this repo, plus test extras, in editable mode
install:
	pip install -e .[test]
	pre-commit install

.PHONY: help
## Print Makefile documentation
help:
	@perl -0 -nle 'printf("%-25s - %s\n", "$$2", "$$1") while m/^##\s*([^\r\n]+)\n^([\w-]+):[^=]/gm' $(MAKEFILE_LIST) | sort
.DEFAULT_GOAL := help
