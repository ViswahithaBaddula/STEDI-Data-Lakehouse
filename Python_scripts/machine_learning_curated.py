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

step_trainer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="step_trainer_trusted"
)

accelerometer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="accelerometer_trusted"
)

query = """
SELECT
    s.sensorReadingTime,
    s.serialNumber,
    s.distanceFromObject,
    a.x,
    a.y,
    a.z
FROM step_trainer_trusted s
INNER JOIN accelerometer_trusted a
ON s.sensorReadingTime = a.timeStamp
"""

machine_learning_curated = sparkSqlQuery(
    glueContext,
    query=query,
    mapping={
        "step_trainer_trusted": step_trainer_trusted,
        "accelerometer_trusted": accelerometer_trusted
    },
    transformation_ctx="machine_learning_curated"
)

EvaluateDataQuality().process_rows(
    frame=machine_learning_curated,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "dq_machine_learning_curated",
        "enableDataQualityResultsPublishing": True
    }
)

sink = glueContext.getSink(
    path="s3://stedi-bucket-assessment/machine_learning_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True
)
sink.setCatalogInfo(catalogDatabase="stedi_db", catalogTableName="machine_learning_curated")
sink.setFormat("glueparquet", compression="snappy")
sink.writeFrame(machine_learning_curated)

job.commit()
