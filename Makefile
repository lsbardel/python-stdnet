REPO_NAME = quantmind

# Fixed - dont modify these lines ==================================
K8S_NS ?= prod
LOCAL_DOCKER_NETWORK = services_default
# ==================================================================

GIT_SHA := $(shell git rev-parse HEAD)
TIMESTAMP := $(shell date -u)


.PHONY: help clean deploy env freeze install image serverless test

help:
	@echo ======================== METACORE ====================================================
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'
	@echo ======================================================================================

clean:			## remove python cache files
	../devops/dev/clean.sh


cloc:			## Count lines of code
	cloc --exclude-dir=tools,venv,node-modules,build,.pytest_cache,.mypy_cache,target .


install:		## install python dependencies in venv
	@pip install -U pip twine
	@pip install -U -r ./dev/requirements-dev.txt
	@pip install -U -r ./dev/requirements.txt


lint: 			## run linters
	isort .
	./dev/run-black.sh
	flake8
