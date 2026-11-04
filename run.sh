#!/bin/bash
set -euo pipefail

cd /home/ec2-user/Scoring_POC
source .venv/bin/activate

# Step 1: Extract data from Snowflake and upload to S3
extract_output=$(python extract_snowflake.py)
printf '%s\n' "$extract_output"

# Step 2: Get the S3 path created by the extraction
S3_INPUT_PATH=$(printf '%s\n' "$extract_output" | sed -n 's#^Extraction completed: s3://#s3a://#p' | tail -n 1)

if [[ -z "$S3_INPUT_PATH" ]]; then
    echo "ERROR: Could not determine the new S3 input path."
    exit 1
fi

export S3_INPUT_PATH

# Step 3: Run predictions on the newly extracted data
python apps.py