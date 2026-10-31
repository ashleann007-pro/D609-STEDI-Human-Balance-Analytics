import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1793461460274 = glueContext.create_dynamic_frame.from_catalog(database="stedi_human_balance", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1793461460274")

# Script generated for node Customer Trusted
CustomerTrusted_node1793461606500 = glueContext.create_dynamic_frame.from_catalog(database="stedi_human_balance", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1793461606500")

# Script generated for node Join Accelerometer with Trusted Customers
JoinAccelerometerwithTrustedCustomers_node1793461706137 = Join.apply(frame1=AccelerometerLanding_node1793461460274, frame2=CustomerTrusted_node1793461606500, keys1=["user"], keys2=["email"], transformation_ctx="JoinAccelerometerwithTrustedCustomers_node1793461706137")

# Script generated for node Select Accelerometer Fields
SelectAccelerometerFields_node1793461839569 = SelectFields.apply(frame=JoinAccelerometerwithTrustedCustomers_node1793461706137, paths=["user", "timestamp", "x", "y", "z"], transformation_ctx="SelectAccelerometerFields_node1793461839569")

# Script generated for node Accelerometer Trusted S3
EvaluateDataQuality().process_rows(frame=SelectAccelerometerFields_node1793461839569, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1793461250738", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrustedS3_node1793462025050 = glueContext.write_dynamic_frame.from_options(frame=SelectAccelerometerFields_node1793461839569, connection_type="s3", format="glueparquet", connection_options={"path": "s3://stedi-human-balance-d609-2026/accelerometer_trusted/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AccelerometerTrustedS3_node1793462025050")

job.commit()