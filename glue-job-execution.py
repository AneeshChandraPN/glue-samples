import boto3
import time

def initialize_job(job_name, script_arguments=None, worker_type="Standard", num_of_workers=5):
    glue_client = boto3.client('glue')

    try:
        job_run = glue_client.start_job_run(
            JobName=job_name,
            Arguments=script_arguments,
            WorkerType=worker_type,
            NumberOfWorkers=num_of_workers
        )

        return job_completion(job_name, job_run['JobRunId'])

    except Exception as general_error:
    	print(str(general_error))


def job_completion(job_name, run_id):
    glue_client = boto3.client('glue')

    job_run_state = 'SUBMITTED'
    failed = False
    stopped = False
    completed = False

    while True:
        if failed or stopped or completed:
            print("Exiting Job {} Run State: {}".format(run_id, job_run_state))
            return {'JobRunState': job_run_state, 'JobRunId': run_id}
        else:
            print("Polling for AWS Glue Job {} current run state".format(job_name))
            job_status = glue_client.get_job_run(
                JobName=job_name,
                RunId=run_id,
                PredecessorsIncluded=True
            )
            job_run_state = (job_status['JobRun']['JobRunState']).upper()
            print("Job Run state is {}".format(job_run_state))
            failed = job_run_state == 'FAILED'
            stopped = job_run_state == 'STOPPED'
            completed = job_run_state == 'SUCCEEDED'
            time.sleep(6)


if __name__ == '__main__':
	JobName = '<glue-job-name>'
	Arguments = {
		'--database':   '<source datbase name>',
		'--tables':  '<comma-seperated-list-of-tables-no-spaces',
		'--s3_prefix':  's3://<bucket>/<prefix-for-target>'
	}

	initialize_job(JobName, Arguments)
