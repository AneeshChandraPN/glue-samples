# glue-samples

## Glue job details

The Glue job to extract the data from a source table requires 3 arguments
--database 		Name of the source database in the Glue data catalog
--tables		Comma seperated list of tables in the database
--s3-prefix		Target S3 path to store the data, e.g. s3://your-datalake/extract/source-mysql/database-x/

The job "glue-mysql-full-extract.py" should be created with the Glue connection to the source database to be used to extract the data.


## Executing the job 

The glue-job-execution.py is a sample of invoking the Glue job with boto3, and waiting for the job to complete.

