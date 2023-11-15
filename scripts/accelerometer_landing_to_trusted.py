import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer_trusted
customer_trusted_node1699989193106 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-human-analytics-lakehouse/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1699989193106",
)

# Script generated for node accelerometer_landing
accelerometer_landing_node1699990704823 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometer_landing_node1699990704823",
)

# Script generated for node filter_customers
SqlQuery666 = """
select a.*
from accelerometer_landing as a
join customer_trusted as c
on a.user = c.email
"""
filter_customers_node1699989245373 = sparkSqlQuery(
    glueContext,
    query=SqlQuery666,
    mapping={
        "customer_trusted": customer_trusted_node1699989193106,
        "accelerometer_landing": accelerometer_landing_node1699990704823,
    },
    transformation_ctx="filter_customers_node1699989245373",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1699990838873 = glueContext.getSink(
    path="s3://stedi-human-analytics-lakehouse/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="accelerometer_trusted_node1699990838873",
)
accelerometer_trusted_node1699990838873.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
accelerometer_trusted_node1699990838873.setFormat("json")
accelerometer_trusted_node1699990838873.writeFrame(filter_customers_node1699989245373)
job.commit()
