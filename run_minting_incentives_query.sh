#!/bin/bash
set -E

if [ "$#" -ne 3 ]; then
    echo "Usage: ./run_minting_incentives_query.sh <query_file> <exclusions file> <output_file>"
    exit
fi

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt |grep -v 'already satisfied'
python minting_incentives_query.py $1 $2 $3
deactivate
