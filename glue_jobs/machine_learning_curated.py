import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1750210474941 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1750210474941")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1750210476953 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1750210476953")

# Script generated for node SQL Query
SqlQuery38 = '''
SELECT DISTINCT 
    step.serialnumber,
    step.sensorreadingtime,
    step.distancefromobject,
    accel.user,
    accel.timestamp,
    accel.x,
    accel.y,
    accel.z
FROM 
    step_trainer_trusted step
JOIN 
    accelerometer_trusted accel
ON 
    step.sensorreadingtime = accel.timestamp;
'''
SQLQuery_node1750210603734 = sparkSqlQuery(glueContext, query = SqlQuery38, mapping = {"step_trainer_trusted":StepTrainerTrusted_node1750210474941, "accelerometer_trusted":AccelerometerTrusted_node1750210476953}, transformation_ctx = "SQLQuery_node1750210603734")

# Script generated for node Machine Learning Curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1750210603734, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750209297638", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurated_node1750210711052 = glueContext.getSink(path="s3://my-spark-data-lake/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1750210711052")
MachineLearningCurated_node1750210711052.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1750210711052.setFormat("json")
MachineLearningCurated_node1750210711052.writeFrame(SQLQuery_node1750210603734)
job.commit()