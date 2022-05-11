import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read CSV file from S3 bucket
datasource0 = glueContext.create_dynamic_frame.from_options('s3', {'paths': ['s3://s3-bucket-s3bucket-jey7guiz8qft/simple_data/simple_data.csv']}, 'csv', {'withHeader': True})

# Show records in the dynamicframe
datasource0.show()

job.commit()