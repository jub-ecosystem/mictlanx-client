#!/bin/bash

REPOSITORY_NAME=${1:-mictlanxtestpypi}
poetry build
poetry config "repositories.${REPOSITORY_NAME}" https://test.pypi.org/legacy/
poetry publish -r "${REPOSITORY_NAME}"
