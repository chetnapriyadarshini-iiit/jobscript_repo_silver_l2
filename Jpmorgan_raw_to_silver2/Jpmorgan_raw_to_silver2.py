import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("JpMorrgan_raw_to_silver_partioned", args={})

silvermain_glue_path1 = "s3://majorproject02-jpmorgan-silver/partitioned_by_department/"
silvermain_glue_path2 = "s3://majorproject02-jpmorgan-silver/partitioned_by_state_department/"
silver_spark_path = "s3://majorproject02-jpmorgan-silver/cleaned_data_stored_as_spark_df/"
silvermain_glue_path3 = "s3://majorproject02-jpmorgan-silver/salary_focus/"
GLUE_DB = "majorproject02_jpmorgan_db"

df = spark.read.parquet(silver_spark_path)
spark.sql(f"CREATE DATABASE IF NOT EXISTS {GLUE_DB}")  # idempotent: safe to run multiple times

dyf_main = DynamicFrame.fromDF(df, glueContext , "dyf_main")

# Enhanced Glue Dynamic Frame write
glueContext.write_dynamic_frame.from_options(
    frame=dyf_main,
    connection_type="s3",
    connection_options={
        "path": silvermain_glue_path1,
        "partitionKeys": ["department"]
    },
    format="parquet",
    format_options={
        "compression": "snappy",
        "useGlueParquetWriter": True
    },
    transformation_ctx="write_silver_dyf_etl"
)
# Enhanced Glue Dynamic Frame write
glueContext.write_dynamic_frame.from_options(
    frame=dyf_main,
    connection_type="s3",
    connection_options={
        "path": silvermain_glue_path2,
        "partitionKeys": ["state","department"]
    },
    format="parquet",
    format_options={
        "compression": "snappy",
        "useGlueParquetWriter": True
    },
    transformation_ctx="write_silver_dyf_etl2"
)

def sink_with_catalog(path: str, table_name: str, partition_keys: list, ctx_name: str):
    """
    Create a Glue catalog sink that:
      - writes Parquet to S3
      - updates/creates the Glue Catalog table (schema + partitions)
    partition_keys: list of partition columns ([] for non-partitioned KPIs)
    """
    sink = glueContext.getSink(
        path=path,
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",      # merge schema/partitions if table exists
        partitionKeys=partition_keys,
        compression="snappy",
        enableUpdateCatalog=True,                 # <-- critical: tells Glue to maintain the Catalog table
        transformation_ctx=ctx_name
    )
    sink.setCatalogInfo(catalogDatabase=GLUE_DB, catalogTableName=table_name)
    sink.setFormat("glueparquet")                 # optimized writer for Parquet + Catalog
    return sink


sink_with_catalog(silvermain_glue_path3, "silver_l1_high_salary", ["state","department"], "sink_l1_sal").writeFrame(dyf_high_salary)
job.commit()