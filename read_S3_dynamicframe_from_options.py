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

# Read CSV file from S3 into a dynamic frame
dynamic_frame_0 = glueContext.create_dynamic_frame_from_options("s3", {"paths": ["s3://s3fjd43/data/simple_csv_data.csv"]}, format="csv", withHeader=True)

# Show / log the records from the dynamic frame. Confirming the read was successful
dynamic_frame_0.show()

job.commit()
