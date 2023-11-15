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

# Script generated for node customer_curated
customer_curated_node1700060455519 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="customer_curated_node1700060455519",
)

# Script generated for node step_trainer_landing
step_trainer_landing_node1700060429354 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing",
    transformation_ctx="step_trainer_landing_node1700060429354",
)

# Script generated for node SQL Query
SqlQuery59 = """
select s.*
from step_trainer_landing as s
join customer_curated as c
on s.serialnumber = c.serialnumber
"""
SQLQuery_node1700060499403 = sparkSqlQuery(
    glueContext,
    query=SqlQuery59,
    mapping={
        "step_trainer_landing": step_trainer_landing_node1700060429354,
        "customer_curated": customer_curated_node1700060455519,
    },
    transformation_ctx="SQLQuery_node1700060499403",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1700060560222 = glueContext.getSink(
    path="s3://stedi-human-analytics-lakehouse/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_node1700060560222",
)
step_trainer_trusted_node1700060560222.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
step_trainer_trusted_node1700060560222.setFormat("json")
step_trainer_trusted_node1700060560222.writeFrame(SQLQuery_node1700060499403)
job.commit()
