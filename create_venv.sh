#!/bin/bash

'''
Create and set up a new virtual environment.
To run: ./create_venv.sh my_new_venv
'''

if [ -z "$1" ]; then
    echo "Please provide a name for the virtual environment."
    exit 1
fi

python3 -m venv $1
source $1/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
