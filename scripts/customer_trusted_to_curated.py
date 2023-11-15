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
customer_trusted_node1700010375574 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1700010375574",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1700010444325 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1700010444325",
)

# Script generated for node SQL Query
SqlQuery767 = """
with emails_from_users_with_acc_data as (
    select
        distinct user as email
    from accelerometer_trusted)
select
    /* select only fields from customer_trusted wo PII fields */
    c.serialnumber,
    c.registrationdate,
    c.lastupdatedate,
    c.sharewithresearchasofdate,
    c.sharewithpublicasofdate,
    c.sharewithfriendsasofdate
from customer_trusted as c
join emails_from_users_with_acc_data as e
on c.email = e.email
"""
SQLQuery_node1700010485566 = sparkSqlQuery(
    glueContext,
    query=SqlQuery767,
    mapping={
        "accelerometer_trusted": accelerometer_trusted_node1700010444325,
        "customer_trusted": customer_trusted_node1700010375574,
    },
    transformation_ctx="SQLQuery_node1700010485566",
)

# Script generated for node customer_curated
customer_curated_node1700010838403 = glueContext.getSink(
    path="s3://stedi-human-analytics-lakehouse/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_curated_node1700010838403",
)
customer_curated_node1700010838403.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
customer_curated_node1700010838403.setFormat("json")
customer_curated_node1700010838403.writeFrame(SQLQuery_node1700010485566)
job.commit()
