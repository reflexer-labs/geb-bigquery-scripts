#!/bin/bash
set -E

python3 -m venv venv
source venv/bin/activate
pip install -r python/requirements.txt |grep -v 'already satisfied'
python python/combine_all.py
deactivate
