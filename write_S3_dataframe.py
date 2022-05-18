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
job.init(args['JOB_NAME'], args)

# Read CSV file from S3 into a dynamic frame
dynamic_frame_0 = glueContext.create_dynamic_frame_from_options(
    connection_type = "s3",
    connection_options= {
    "paths": ["s3://s3fjd43/data/simple_csv_data.csv"]},
    format = "csv",
    format_options = {
        "withHeader": True
    }
)

# Convert dynamic frame to dataframe
data_frame_0 = dynamic_frame_0.toDF()

# Write the data to S3
data_frame_0.write.parquet(
    "s3://s3fjd43/write_S3_dataframe/",
    mode = "overwrite"
)
