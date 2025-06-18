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

# Script generated for node Customer Trusted
CustomerTrusted_node1750110630671 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1750110630671")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1750110628899 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1750110628899")

# Script generated for node Join
Join_node1750110665483 = Join.apply(frame1=AccelerometerTrusted_node1750110628899, frame2=CustomerTrusted_node1750110630671, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1750110665483")

# Script generated for node Drop Fields
SqlQuery40 = '''
select distinct customerName,email, phone, birthday, serialnumber, registrationdate, lastupdatedate, sharewithresearchasofdate, sharewithpublicasofdate, sharewithfriendsasofdate 
from myDataSource;


'''
DropFields_node1750170963496 = sparkSqlQuery(glueContext, query = SqlQuery40, mapping = {"myDataSource":Join_node1750110665483}, transformation_ctx = "DropFields_node1750170963496")

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(frame=DropFields_node1750170963496, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750170938933", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1750171124918 = glueContext.getSink(path="s3://my-spark-data-lake/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1750171124918")
CustomerCurated_node1750171124918.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCurated_node1750171124918.setFormat("json")
CustomerCurated_node1750171124918.writeFrame(DropFields_node1750170963496)
job.commit()