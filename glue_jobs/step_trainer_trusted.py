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

# Script generated for node Step Trainer landing
StepTrainerlanding_node1750208086006 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerlanding_node1750208086006")

# Script generated for node Customer Curated
CustomerCurated_node1750208085274 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1750208085274")

# Script generated for node StepTrainerTrustedJoin
SqlQuery65 = '''
SELECT DISTINCT step.serialnumber, 
                step.sensorreadingtime, 
                step.distancefromobject
FROM step_trainer_landing AS step
JOIN customer_curated AS cust
  ON step.serialnumber = cust.serialnumber;
'''
StepTrainerTrustedJoin_node1750208357706 = sparkSqlQuery(glueContext, query = SqlQuery65, mapping = {"customer_curated":CustomerCurated_node1750208085274, "step_trainer_landing":StepTrainerlanding_node1750208086006}, transformation_ctx = "StepTrainerTrustedJoin_node1750208357706")

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(frame=StepTrainerTrustedJoin_node1750208357706, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750208079285", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1750208518453 = glueContext.getSink(path="s3://my-spark-data-lake/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1750208518453")
StepTrainerTrusted_node1750208518453.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1750208518453.setFormat("json")
StepTrainerTrusted_node1750208518453.writeFrame(StepTrainerTrustedJoin_node1750208357706)
job.commit()