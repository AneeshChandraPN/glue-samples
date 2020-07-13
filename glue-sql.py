import sys
import boto3
import json

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


'''
Read the SQL from a file in S3. 
The SQL can refer to the exact text - [[ START DATE ]] and [[ END DATE ]] to accept date parameters through the job.
For example - 
select col1, col2 from table1 where col3 >= '[[ START DATE ]]' and col4 <= '[[ END DATE ]]'
'''
def read_sql(s3_path, start_date="", end_date=""):
	s3client = boto3.client("s3", region_name="ap-southeast-1")

	f_bucketname = s3_path.split("/")[2]
	f_filepath = '/'.join(s3_path.split("/")[3:])

	f_fileObj = s3client.get_object(Bucket=f_bucketname, Key=f_filepath) 
	f_fileData = f_fileObj['Body'].read()
	f_sql = f_fileData.decode('utf-8')	
	r_sql = f_sql.replace("[[ START DATE ]]", start_date).replace("[[ END DATE ]]", end_date)	
	return r_sql


'''
TO DO:
Implement logic to support versioning of data. A simple approach would be backup existing data,
and overwrite into the target folder with the new results of the SQL
'''
def enable_versioning(target_path, transform):
	print("TO DO: Backup data into a seperate prefix")


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv,
							['JOB_NAME',
							'transform',
							's3_sql_path',
							's3_target_path',
							'param_start_date',
							'param_end_date',
							'enable_versioning'])


## These 3 parameters are mandatory
if (('transform' not in args) and ('s3_sql_path' not in args) and ('s3_target_path' not in args)):
	print('Mandatory parameters for SQL file Path in s3, and target path to store results missing')
	exit(1)


l_transform = args['transform']
l_sql_path = args['s3_sql_path']
l_target_prefix= args['s3_target_path']
l_start_date = ''
l_end_date = ''


## Optional parameters for start date and end date
if (('param_start_date' in args) and ('param_end_date' in args)):
	l_start_date = args['param_start_date']
	l_end_date = args['param_end_date']


## Fetch the SQL from the S3 sql path
l_sql = read_sql(l_sql_path , l_start_date, l_end_date)


## Initialize the Glue Spark Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


## Execute and Collect the SQL results into a data frame
df = spark.sql(l_sql)


## Prepare the S3 Target path to write the results into
l_s3_folder = l_transform.replace("_", "-")
l_target_prefix = l_target_prefix if l_target_prefix[len(l_target_prefix)-1:] == "/" else l_target_prefix+"/"
l_s3_path = l_target_prefix + l_s3_folder + "/"


## Write the results into s3 with Parquet format. The write mode OVERWRITE will replace the existing data.
df.toDF().write.mode("overwrite").format("parquet").save(l_s3_path)

print('Completed extract for {}'.format(l_transform))

job.commit()
