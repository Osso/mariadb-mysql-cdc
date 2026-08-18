#!/usr/bin/env sh
set -eu

cargo test
python3 -m unittest tests/test_deploy_script.py
