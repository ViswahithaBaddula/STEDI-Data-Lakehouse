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

step_trainer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="step_trainer_landing"
)

customer_curated = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="customer_curated"
)

query = """
SELECT s.sensorReadingTime, s.serialNumber, s.distanceFromObject
FROM step_trainer_landing s
INNER JOIN customer_curated c
ON s.serialNumber = c.serialnumber
"""

step_trainer_trusted = sparkSqlQuery(
    glueContext,
    query=query,
    mapping={
        "step_trainer_landing": step_trainer_landing,
        "customer_curated": customer_curated
    },
    transformation_ctx="step_trainer_trusted"
)

EvaluateDataQuality().process_rows(
    frame=step_trainer_trusted,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "dq_step_trainer_trusted",
        "enableDataQualityResultsPublishing": True
    }
)

sink = glueContext.getSink(
    path="s3://stedi-bucket-assessment/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True
)
sink.setCatalogInfo(catalogDatabase="stedi_db", catalogTableName="step_trainer_trusted")
sink.setFormat("glueparquet", compression="snappy")
sink.writeFrame(step_trainer_trusted)

job.commit()
