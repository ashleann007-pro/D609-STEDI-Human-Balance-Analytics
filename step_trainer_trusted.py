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

# Script generated for node Customer Curated
CustomerCurated_node1793464245671 = glueContext.create_dynamic_frame.from_catalog(database="stedi_human_balance", table_name="customer_curated", transformation_ctx="CustomerCurated_node1793464245671")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1793464209430 = glueContext.create_dynamic_frame.from_catalog(database="stedi_human_balance", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1793464209430")

# Script generated for node Filter Step Trainer By Curated Customers
SqlQuery1085 = '''
SELECT s.*
FROM s
WHERE s.serialnumber IN (
    SELECT c.serialnumber
    FROM c
)
'''
FilterStepTrainerByCuratedCustomers_node1793464293831 = sparkSqlQuery(glueContext, query = SqlQuery1085, mapping = {"s":StepTrainerLanding_node1793464209430, "c":CustomerCurated_node1793464245671}, transformation_ctx = "FilterStepTrainerByCuratedCustomers_node1793464293831")

# Script generated for node Step Trainer Trusted S3
EvaluateDataQuality().process_rows(frame=FilterStepTrainerByCuratedCustomers_node1793464293831, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1793463258415", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrustedS3_node1793464334591 = glueContext.getSink(path="s3://stedi-human-balance-d609-2026/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrustedS3_node1793464334591")
StepTrainerTrustedS3_node1793464334591.setCatalogInfo(catalogDatabase="stedi_human_balance",catalogTableName="step_trainer_trusted")
StepTrainerTrustedS3_node1793464334591.setFormat("glueparquet", compression="snappy")
StepTrainerTrustedS3_node1793464334591.writeFrame(FilterStepTrainerByCuratedCustomers_node1793464293831)
job.commit()