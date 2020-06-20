import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           'database',
                           'tables',
                           's3_prefix'])

if (('database' not in args) and ('tables' not in args) and ('s3_prefix' not in args)):
    print('Mandatory parameters for database, tables and s3_prefix not provided')
    exit(1)

l_target_prefix = args['s3_prefix']
l_database = args['database']
l_tables = args['tables']    

l_table_list = l_tables.split(",")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

for l_table in l_table_list:
    datasource = glueContext.create_dynamic_frame.from_catalog(
        database = l_database, 
        table_name = l_table, 
        transformation_ctx = "datasource")
    
    l_s3_folder = l_table.replace("_", "-")
    l_target_prefix = l_target_prefix if l_target_prefix[len(l_target_prefix)-1:] == "/" else l_target_prefix+"/"
    l_s3_path = l_target_prefix + l_s3_folder + "/"
    
    datasource.toDF().write.mode("overwrite").format("parquet").save(l_s3_path)
    
    print('Completed extract for {}'.format(l_table))
    
job.commit()