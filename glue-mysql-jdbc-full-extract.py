import sys
import MySQLdb
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

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           'secret_id',
                           'tables',
                           's3_prefix'])

if (('secret_id' not in args) and ('tables' not in args) and ('s3_prefix' not in args)):
    print('Mandatory parameters for secret_id, tables and s3_prefix not provided')
    exit(1)

l_target_prefix = args['s3_prefix']
l_secret_id = args['secret_id']
l_tables = args['tables']    

l_table_list = l_tables.split(",")


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Getting DB credentials from Secrets Manager
client = boto3.client("secretsmanager", region_name="ap-southeast-1")

l_get_secret_value_response = client.get_secret_value(
        SecretId=l_secret_id
)

l_secret = l_get_secret_value_response['SecretString']
l_secret = json.loads(l_secret)

l_server_name = l_secret.get('db_server')
l_database = l_secret.get('db_database')
l_user_name = l_secret.get('db_username')
l_pwd = l_secret.get('db_password')

l_connection = MySQLdb.connect(
	host = l_server_name,
	user = l_user_name,
	passwd = l_pwd)  


for l_table in l_table_list:

	l_url_params = [
		'zeroDateTimeBehavior=convertToNull',
		'autoReconnect=true',
		'characterEncoding=UTF-8',
		'characterSetResults=UTF-8'
	]

	df_src = spark.read \
				.format("jdbc") \
				.option("url", "jdbc:mysql://{}/{}?{}".format(l_server_name, l_database, ('&').join(l_url_params))) \
				.option("driver", "com.mysql.jdbc.Driver") \
				.option("dbtable", l_table) \
				.option("user", l_user_name) \
				.option("password", l_pwd) \
				.load()
	
	
    l_s3_folder = l_table.replace("_", "-")
    l_target_prefix = l_target_prefix if l_target_prefix[len(l_target_prefix)-1:] == "/" else l_target_prefix+"/"
    l_s3_path = l_target_prefix + l_s3_folder + "/"
    
    df_src.toDF().write.mode("overwrite").format("parquet").save(l_s3_path)
    
    print('Completed extract for {}'.format(l_table))


job.commit()
