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

# Script generated for node Daily flights raw from S3
DailyflightsrawfromS3_node1788453478977 = glueContext.create_dynamic_frame.from_catalog(database="airlines_db", table_name="flights_raw", transformation_ctx="DailyflightsrawfromS3_node1788453478977")

# Script generated for node Airport codes dim
Airportcodesdim_node1788453204653 = glueContext.create_dynamic_frame.from_catalog(database="airlines_db", table_name="dev_airlines_dim_airport_codes", redshift_tmp_dir="s3://s3redshiftbucketgds",additional_options={"aws_iam_role": "arn:aws:iam::204183190032:role/service-role/AmazonRedshift-CommandsAccessRole-20260825T100035"}, transformation_ctx="Airportcodesdim_node1788453204653")

# Script generated for node JoinForDepartureDetails
JoinForDepartureDetails_node1788453566311 = Join.apply(frame1=DailyflightsrawfromS3_node1788453478977, frame2=Airportcodesdim_node1788453204653, keys1=["originairportid"], keys2=["airport_id"], transformation_ctx="JoinForDepartureDetails_node1788453566311")

# Script generated for node SchemaChangeForDepartureDetails
SchemaChangeForDepartureDetails_node1788453777212 = ApplyMapping.apply(frame=JoinForDepartureDetails_node1788453566311, mappings=[("carrier", "string", "carrier", "string"), ("destairportid", "long", "destairportid", "long"), ("depdelay", "long", "dep_delay", "long"), ("arrdelay", "long", "arr_delay", "long"), ("city", "string", "dep_city", "string"), ("name", "string", "dep_airport", "string"), ("state", "string", "dep_state", "string")], transformation_ctx="SchemaChangeForDepartureDetails_node1788453777212")

# Script generated for node JoinForArrivalDetails
JoinForArrivalDetails_node1788453986408 = Join.apply(frame1=SchemaChangeForDepartureDetails_node1788453777212, frame2=Airportcodesdim_node1788453204653, keys1=["destairportid"], keys2=["airport_id"], transformation_ctx="JoinForArrivalDetails_node1788453986408")

# Script generated for node SchemaChangesForArrivalDetails
SchemaChangesForArrivalDetails_node1788454067375 = ApplyMapping.apply(frame=JoinForArrivalDetails_node1788453986408, mappings=[("carrier", "string", "carrier", "string"), ("dep_state", "string", "dep_state", "string"), ("state", "string", "arr_state", "string"), ("arr_delay", "long", "arr_delay", "long"), ("city", "string", "arr_city", "string"), ("name", "string", "arr_airport", "string"), ("dep_city", "string", "dep_city", "string"), ("dep_delay", "long", "dep_delay", "long"), ("dep_airport", "string", "dep_airport", "string")], transformation_ctx="SchemaChangesForArrivalDetails_node1788454067375")

# Script generated for node WriteInTargetRedshiftTable
WriteInTargetRedshiftTable_node1788454187060 = glueContext.write_dynamic_frame.from_catalog(frame=SchemaChangesForArrivalDetails_node1788454067375, database="airlines_db", table_name="dev_airlines_daily_flights_processed", redshift_tmp_dir="s3://s3redshiftbucketgds",additional_options={"aws_iam_role": "arn:aws:iam::204183190032:role/service-role/AmazonRedshift-CommandsAccessRole-20260825T100035"}, transformation_ctx="WriteInTargetRedshiftTable_node1788454187060")

job.commit()