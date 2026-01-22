import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from awsgluedq.transforms import EvaluateDataQuality

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

DEFAULT_DATA_QUALITY_RULESET = """Rules = [ColumnCount > 0]"""

customer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="customer_landing"
)

query = """
SELECT *
FROM customer_landing
WHERE sharewithresearchasofdate IS NOT NULL
"""

customer_trusted = sparkSqlQuery(
    glueContext,
    query=query,
    mapping={"customer_landing": customer_landing},
    transformation_ctx="customer_trusted"
)

EvaluateDataQuality().process_rows(
    frame=customer_trusted,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "dq_customer_trusted",
        "enableDataQualityResultsPublishing": True
    }
)

sink = glueContext.getSink(
    path="s3://stedi-bucket-assessment/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True
)
sink.setCatalogInfo(catalogDatabase="stedi_db", catalogTableName="customer_trusted")
sink.setFormat("glueparquet", compression="snappy")
sink.writeFrame(customer_trusted)

job.commit()
