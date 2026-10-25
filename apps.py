import argparse
import os
import pickle
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StructField, StructType


base_cols = [
    "TENURE_MONTHS", "CREDIT_LIMIT", "BALANCE", "TXN_COUNT_30D",
    "SPEND_PREV_30D", "SPEND_30D", "INACTIVE_DAYS",
    "SUPPORT_CALLS_90D", "PRODUCTS", "DIGITAL_LOGINS_30D",
]

feature_cols = base_cols + [
    "CREDIT_UTILIZATION",
    "SPEND_CHANGE_PCT",
    "AVG_SPEND_PER_TXN",
    "SUPPORT_CALLS_PER_PRODUCT",
    "ENGAGEMENT_PER_INACTIVE_DAY",
]


def main():
    parser = argparse.ArgumentParser(description="Score customer churn from S3 Parquet files")
    parser.add_argument(
        "--input",
        default=os.getenv(
            "S3_INPUT_PATH", "s3a://ml-poc-raw-sg-saurabh00100/raw/snapshot/"
        ),
        help="Input S3 Parquet prefix",
    )
    parser.add_argument(
        "--output",
        default=os.getenv(
            "S3_OUTPUT_PREFIX", "s3a://ml-poc-raw-sg-saurabh00100/output/"
        ),
        help="S3 output prefix; each run gets its own subfolder",
    )
    parser.add_argument(
        "--model", default=os.getenv("MODEL_PATH", "model.pkl"),
        help="Path to the trained model",
    )
    args = parser.parse_args()

    if not Path(args.model).is_file():
        raise FileNotFoundError(f"Required model file not found: {args.model}")

    with open(args.model, "rb") as model_file:
        clf = pickle.load(model_file)

    os.environ["PYSPARK_PYTHON"] = sys.executable

    spark = (
        SparkSession.builder
        .appName("CustomerChurnScoring")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.5.0")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.endpoint.region", "ap-southeast-1")
        .getOrCreate()
    )

    try:
        data = spark.read.parquet(args.input)
        missing_cols = [name for name in base_cols if name not in data.columns]
        if missing_cols:
            raise ValueError(f"Input Parquet is missing required columns: {missing_cols}")

        df_new = data
        for col_name in base_cols:
            df_new = df_new.withColumn(col_name, F.col(col_name).cast("double"))

        df_new = (
            df_new.withColumn(
                "CREDIT_UTILIZATION",
                F.col("BALANCE") / F.greatest(F.col("CREDIT_LIMIT"), F.lit(1.0)),
            )
            .withColumn(
                "SPEND_CHANGE_PCT",
                (F.col("SPEND_30D") - F.col("SPEND_PREV_30D"))
                / F.greatest(F.col("SPEND_PREV_30D"), F.lit(1.0)),
            )
            .withColumn(
                "AVG_SPEND_PER_TXN",
                F.when(
                    F.col("TXN_COUNT_30D") > 0,
                    F.col("SPEND_30D")
                    / F.greatest(F.col("TXN_COUNT_30D"), F.lit(1.0)),
                ).otherwise(F.lit(0.0)),
            )
            .withColumn(
                "SUPPORT_CALLS_PER_PRODUCT",
                F.col("SUPPORT_CALLS_90D")
                / F.greatest(F.col("PRODUCTS"), F.lit(1.0)),
            )
            .withColumn(
                "ENGAGEMENT_PER_INACTIVE_DAY",
                (F.col("TXN_COUNT_30D") + F.col("DIGITAL_LOGINS_30D"))
                / (F.col("INACTIVE_DAYS") + 1),
            )
        )

        spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "5000")

        def predict_batch(iterator):
            for pdf in iterator:
                pdf["PREDICTED_CHURN_90D"] = clf.predict(
                    pdf[feature_cols]
                ).astype("int32")
                yield pdf

        output_schema = StructType(
            df_new.schema.fields
            + [StructField("PREDICTED_CHURN_90D", IntegerType(), True)]
        )
        df_result = df_new.mapInPandas(predict_batch, schema=output_schema)

        # A unique run folder protects the input and any earlier prediction output.
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        run_id += "_" + uuid4().hex[:8]
        output_path = f"{args.output.rstrip('/')}/run_{run_id}/"

        df_result.write.mode("errorifexists").option("header", "true").csv(output_path)
        print(f"Predictions saved to: {output_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
