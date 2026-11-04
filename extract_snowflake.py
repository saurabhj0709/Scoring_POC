import os
from io import BytesIO
from datetime import datetime, timezone

import boto3
import snowflake.connector


# AWS S3 configuration
bucket = "ml-poc-raw-sg-saurabh00100"
run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
s3_prefix = f"raw/snapshot_pull_test/{run_id}/"

# Connect to Snowflake
conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    warehouse="COMPUTE_WH",
    database="CUSTOMER_ATTRITION_ANALYTICS",
    schema="CHURN_DATA"
)

s3 = boto3.client("s3", region_name="ap-southeast-1")

try:
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT *
            FROM CUSTOMER_ATTRITION_ANALYTICS.CHURN_DATA.CUSTOMER_SPEND
        """)

        # Fetch data in batches and upload as Parquet
        for i, batch in enumerate(cursor.fetch_pandas_batches()):
            buffer = BytesIO()

            batch.to_parquet(
                buffer,
                index=False,
                compression="snappy"
            )

            buffer.seek(0)

            key = f"{s3_prefix}part-{i:05d}.parquet"
            s3.upload_fileobj(buffer, bucket, key)

            print(f"Uploaded: {key}")

    finally:
        cursor.close()

finally:
    conn.close()

print(f"Extraction completed: s3://{bucket}/{s3_prefix}")