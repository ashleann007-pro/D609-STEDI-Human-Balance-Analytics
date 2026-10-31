
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read customer data from the landing zone
customers = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_human_balance",
    table_name="customer_landing"
).toDF()

# Keep customers who consented to research
customers_trusted = customers.filter(
    col("sharewithresearchasofdate").isNotNull()
)

# Write the trusted customer records to S3
customers_trusted.write.mode("overwrite").parquet(
    "s3://stedi-human-balance-d609-2026/customer_trusted/"
)

print("Trusted customer count:", customers_trusted.count())

job.commit()
