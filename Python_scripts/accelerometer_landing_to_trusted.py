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

accelerometer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="accelerometer_landing"
)

customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="customer_trusted"
)

query = """
SELECT a.timeStamp, a.user, a.x, a.y, a.z
FROM accelerometer_landing a
INNER JOIN customer_trusted c
ON a.user = c.email
"""

accelerometer_trusted = sparkSqlQuery(
    glueContext,
    query=query,
    mapping={
        "accelerometer_landing": accelerometer_landing,
        "customer_trusted": customer_trusted
    },
    transformation_ctx="accelerometer_trusted"
)

EvaluateDataQuality().process_rows(
    frame=accelerometer_trusted,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "dq_accelerometer_trusted",
        "enableDataQualityResultsPublishing": True
    }
)

sink = glueContext.getSink(
    path="s3://stedi-bucket-assessment/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True
)
sink.setCatalogInfo(catalogDatabase="stedi_db", catalogTableName="accelerometer_trusted")
sink.setFormat("glueparquet", compression="snappy")
sink.writeFrame(accelerometer_trusted)

job.commit()
