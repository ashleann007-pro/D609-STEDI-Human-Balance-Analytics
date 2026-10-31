import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1793463350483 = glueContext.create_dynamic_frame.from_catalog(database="stedi_human_balance", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1793463350483")

# Script generated for node Customer Trusted
CustomerTrusted_node1793463301759 = glueContext.create_dynamic_frame.from_catalog(database="stedi_human_balance", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1793463301759")

# Script generated for node Filter Customers With Accelerometer Data
SqlQuery1185 = '''
SELECT c.*
FROM c
WHERE EXISTS (
    SELECT 1
    FROM a
    WHERE c.email = a.user
)

'''
FilterCustomersWithAccelerometerData_node1793463392856 = sparkSqlQuery(glueContext, query = SqlQuery1185, mapping = {"c":CustomerTrusted_node1793463301759, "a":AccelerometerTrusted_node1793463350483}, transformation_ctx = "FilterCustomersWithAccelerometerData_node1793463392856")

# Script generated for node Customer Curated S3
EvaluateDataQuality().process_rows(frame=FilterCustomersWithAccelerometerData_node1793463392856, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1793463258415", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCuratedS3_node1793463518717 = glueContext.getSink(path="s3://stedi-human-balance-d609-2026/customer_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCuratedS3_node1793463518717")
CustomerCuratedS3_node1793463518717.setCatalogInfo(catalogDatabase="stedi_human_balance",catalogTableName="customer_curated")
CustomerCuratedS3_node1793463518717.setFormat("glueparquet", compression="snappy")
CustomerCuratedS3_node1793463518717.writeFrame(FilterCustomersWithAccelerometerData_node1793463392856)
job.commit()