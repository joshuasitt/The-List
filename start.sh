#!/bin/bash
cd "$(dirname "$0")"

if [ ! -d "venv" ]; then
  echo "First run — installing dependencies..."
  python3 -m venv venv
  source venv/bin/activate
  pip install -r requirements.txt -q
else
  source venv/bin/activate
fi

python3 server.py
