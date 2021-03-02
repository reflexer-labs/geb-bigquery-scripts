#!/bin/bash
set -e
if [ "$#" -ne 2 ]; then
    echo "Usage: ./run_staking_rewards.sh <exclusions file> <output_file>"
    exit
fi

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt |grep -v 'already satisfied'
python staking_incentives.py $1 $2
deactivate
