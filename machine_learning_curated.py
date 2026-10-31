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
AccelerometerTrusted_node1793464823803 = glueContext.create_dynamic_frame.from_catalog(database="stedi_human_balance", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1793464823803")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1793464855083 = glueContext.create_dynamic_frame.from_catalog(database="stedi_human_balance", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1793464855083")

# Script generated for node Join Accelerometer and Step Trainer
SqlQuery1138 = '''
SELECT
    a.user,
    a.timestamp,
    a.x,
    a.y,
    a.z,
    s.sensorreadingtime,
    s.serialnumber,
    s.distancefromobject
FROM a
INNER JOIN s
    ON a.timestamp = s.sensorreadingtime
'''
JoinAccelerometerandStepTrainer_node1793464893224 = sparkSqlQuery(glueContext, query = SqlQuery1138, mapping = {"a":AccelerometerTrusted_node1793464823803, "s":StepTrainerTrusted_node1793464855083}, transformation_ctx = "JoinAccelerometerandStepTrainer_node1793464893224")

# Script generated for node Machine Learning Curated S3
EvaluateDataQuality().process_rows(frame=JoinAccelerometerandStepTrainer_node1793464893224, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1793463258415", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCuratedS3_node1793464954057 = glueContext.getSink(path="s3://stedi-human-balance-d609-2026/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCuratedS3_node1793464954057")
MachineLearningCuratedS3_node1793464954057.setCatalogInfo(catalogDatabase="stedi_human_balance",catalogTableName="machine_learning_curated")
MachineLearningCuratedS3_node1793464954057.setFormat("glueparquet", compression="snappy")
MachineLearningCuratedS3_node1793464954057.writeFrame(JoinAccelerometerandStepTrainer_node1793464893224)
job.commit()