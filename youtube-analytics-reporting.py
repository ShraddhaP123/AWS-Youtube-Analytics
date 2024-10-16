import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1728935089386 = glueContext.create_dynamic_frame.from_catalog(database="db_youtube_cleaned", table_name="raw_statistics", transformation_ctx="AWSGlueDataCatalog_node1728935089386")

# Script generated for node Amazon S3
AmazonS3_node1728935071099 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://de-youtube-analytics-cleansed-useast1-dev/youtube/raw_statistics_reference_data/"], "recurse": True}, transformation_ctx="AmazonS3_node1728935071099")

# Script generated for node Join
Join_node1728935104238 = Join.apply(frame1=AWSGlueDataCatalog_node1728935089386, frame2=AmazonS3_node1728935071099, keys1=["category_id"], keys2=["id"], transformation_ctx="Join_node1728935104238")

# Script generated for node Amazon S3
AmazonS3_node1728935210348 = glueContext.getSink(path="s3://de-youtube-analytics-reporting-useast1-dev", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["region", "category_id"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1728935210348")
AmazonS3_node1728935210348.setCatalogInfo(catalogDatabase="db_youtube_analytics",catalogTableName="final_analytics")
AmazonS3_node1728935210348.setFormat("glueparquet", compression="snappy")
AmazonS3_node1728935210348.writeFrame(Join_node1728935104238)
job.commit()