#!/bin/bash
set -E

if [ "$#" -ne 2 ]; then
    echo "Usage: ./run_incentives_query.sh <query_file> <output_file>"
    exit
fi

python3 -m venv venv
source venv/bin/activate
pip3 install -r python/requirements.txt |grep -v 'already satisfied'
python3 python/incentives_query.py $1 $2 $3
deactivate
