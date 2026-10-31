
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col
from awsglue.dynamicframe import DynamicFrame

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

# Convert the filtered DataFrame to a DynamicFrame
customers_trusted_dyf = DynamicFrame.fromDF(
    customers_trusted,
    glueContext,
    "customers_trusted_dyf"
)

# Write trusted customers and update the Glue Data Catalog
sink = glueContext.getSink(
    path="s3://stedi-human-balance-d609-2026/customer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_trusted_sink"
)

sink.setCatalogInfo(
    catalogDatabase="stedi_human_balance",
    catalogTableName="customer_trusted"
)

sink.setFormat("glueparquet", compression="snappy")
sink.writeFrame(customers_trusted_dyf)

job.commit()
