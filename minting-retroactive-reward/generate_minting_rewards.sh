#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: ./generate_minting_rewards.sh <output_file>"
fi

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt |grep -v 'already satisfied'
python minting_incentives.py $1
deactivate
