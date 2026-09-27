import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality


# ---------------------------------------------------------
# Initialize Glue job
# ---------------------------------------------------------

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# ---------------------------------------------------------
# 1. Read raw healthcare data from Glue Data Catalog
# ---------------------------------------------------------

raw_data = glueContext.create_dynamic_frame.from_catalog(
    database="healthcare_db",
    table_name="raw",
    transformation_ctx="raw_data"
)


# ---------------------------------------------------------
# 2. Define Data Quality rules
# ---------------------------------------------------------

healthcare_ruleset = r"""
Rules = [
    IsPrimaryKey "patient_id",
    IsComplete "name",
    IsComplete "age",
    ColumnValues "age" between 0 and 120,
    ColumnValues "admission_date" matches "\d{4}-\d{2}-\d{2}",
    ColumnValues "gender" in ["M", "F"],
    ColumnValues "hospital_id" in ["HOSP_A", "HOSP_B", "HOSP_C"]
]
"""


# ---------------------------------------------------------
# 3. Evaluate Data Quality at row level
# ---------------------------------------------------------

dq_results = EvaluateDataQuality().process_rows(
    frame=raw_data,
    ruleset=healthcare_ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "healthcare_data_quality",
        "enableDataQualityResultsPublishing": True
    },
    additional_options={
        "performanceTuning.caching": "CACHE_NOTHING"
    }
)


# ---------------------------------------------------------
# 4. Get row-level Data Quality outcomes
# ---------------------------------------------------------

row_level_outcomes = SelectFromCollection.apply(
    dfc=dq_results,
    key="rowLevelOutcomes",
    transformation_ctx="row_level_outcomes"
)


# Convert DynamicFrame to Spark DataFrame
row_df = row_level_outcomes.toDF()


# ---------------------------------------------------------
# 5. Split Passed and Failed records
# ---------------------------------------------------------

passed_df = row_df.filter(
    row_df["DataQualityEvaluationResult"] == "Passed"
)

failed_df = row_df.filter(
    row_df["DataQualityEvaluationResult"] == "Failed"
)


# ---------------------------------------------------------
# 6. Remove Data Quality metadata before saving
# ---------------------------------------------------------

dq_columns = [
    "DataQualityRulesPass",
    "DataQualityRulesFail",
    "DataQualityRulesSkip",
    "DataQualityEvaluationResult"
]

for column_name in dq_columns:

    if column_name in passed_df.columns:
        passed_df = passed_df.drop(column_name)

    if column_name in failed_df.columns:
        failed_df = failed_df.drop(column_name)


# ---------------------------------------------------------
# 7. Write PASSED records to validated/
# ---------------------------------------------------------

passed_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(
        "s3://rohal-healthcare-raw-2026/validated/"
    )


# ---------------------------------------------------------
# 8. Write FAILED records to rejected/
# ---------------------------------------------------------

failed_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(
        "s3://rohal-healthcare-raw-2026/rejected/"
    )


# ---------------------------------------------------------
# Finish Glue job
# ---------------------------------------------------------

job.commit()