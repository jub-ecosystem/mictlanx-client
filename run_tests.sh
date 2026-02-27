#!/bin/bash
coverage run -m pytest ./tests/ -s
coverage report -m
