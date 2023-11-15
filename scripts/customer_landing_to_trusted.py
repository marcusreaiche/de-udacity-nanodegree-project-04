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

# Script generated for node customer_landing
customer_landing_node1699989193106 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_landing",
    transformation_ctx="customer_landing_node1699989193106",
)

# Script generated for node filter_customers
SqlQuery720 = """
select * from customer_landing
where sharewithresearchasofdate is not null

"""
filter_customers_node1699989245373 = sparkSqlQuery(
    glueContext,
    query=SqlQuery720,
    mapping={"customer_landing": customer_landing_node1699989193106},
    transformation_ctx="filter_customers_node1699989245373",
)

# Script generated for node customer_trusted
customer_trusted_node1699989371724 = glueContext.getSink(
    path="s3://stedi-human-analytics-lakehouse/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_trusted_node1699989371724",
)
customer_trusted_node1699989371724.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
customer_trusted_node1699989371724.setFormat("json")
customer_trusted_node1699989371724.writeFrame(filter_customers_node1699989245373)
job.commit()
