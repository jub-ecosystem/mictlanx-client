#!/bin/bash
docker rm -f mictlanx-docs || true
docker run -d --name mictlanx-docs --rm -it -p 8000:8000 -v ${PWD}:/docs squidfunk/mkdocs-material
