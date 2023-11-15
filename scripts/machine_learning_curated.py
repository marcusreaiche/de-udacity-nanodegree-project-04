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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1700062143016 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1700062143016",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1700062105537 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_node1700062105537",
)

# Script generated for node SQL Query
SqlQuery29 = """
select 
    s.*,
    -- select only x, y, z columns from accelerometer_trusted
    -- do not select user to avoid privacy issues
    a.x,
    a.y,
    a.z
from step_trainer_trusted as s
join accelerometer_trusted as a
on s.sensorreadingtime = a.timestamp
"""
SQLQuery_node1700062197707 = sparkSqlQuery(
    glueContext,
    query=SqlQuery29,
    mapping={
        "step_trainer_trusted": step_trainer_trusted_node1700062105537,
        "accelerometer_trusted": accelerometer_trusted_node1700062143016,
    },
    transformation_ctx="SQLQuery_node1700062197707",
)

# Script generated for node machine_learning_curated
machine_learning_curated_node1700062303954 = glueContext.getSink(
    path="s3://stedi-human-analytics-lakehouse/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="machine_learning_curated_node1700062303954",
)
machine_learning_curated_node1700062303954.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
machine_learning_curated_node1700062303954.setFormat("json")
machine_learning_curated_node1700062303954.writeFrame(SQLQuery_node1700062197707)
job.commit()
