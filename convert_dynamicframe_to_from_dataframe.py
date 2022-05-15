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
    connection_options= {"paths": ["s3://s3fjd43/data/simple_csv_data.csv"]},
    format = "csv",
    format_options = {
        "withHeader": True
    }
)

# Convert dynamic frame to dataframe
data_frame_0 = dynamic_frame_0.toDF()

# data_frame_0 is not avaiable to work with as a standard pyspark data frame
# ...

# Convert dataframe frame back to dynamic frame. The resulting dynamic frame is dynamic_frame_1
DynamicFrame.fromDF(data_frame_0, glueContext, "dynamic_frame_1")

job.commit()
