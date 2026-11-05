#!/bin/bash
set -euo pipefail

cd /home/ec2-user/Scoring_POC
source .venv/bin/activate

# Snowflake connection details
export SNOWFLAKE_ACCOUNT="HZAUAGW-WR58100"
export SNOWFLAKE_USER="SAURABHJ0709"

# Retrieve Snowflake password securely from AWS Parameter Store
SNOWFLAKE_PASSWORD=$(
    aws ssm get-parameter \
        --name "/scoring-poc/snowflake/password" \
        --with-decryption \
        --query "Parameter.Value" \
        --output text \
        --region ap-southeast-1
)

export SNOWFLAKE_PASSWORD


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