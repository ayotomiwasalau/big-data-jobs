import boto3 
import os 
import configparser

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


session = boto3.Session(
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
)
client = session.client('emr')

# session = boto3.Session(profile_name='manager')
# client = session.client('emr')

S3_BUCKET = 'data-emr-bucket-store'
S3_KEY = 'deploy-on-console/python/spark_job_emr.py'
S3_URI = 's3://{bucket}/{key}'.format(bucket=S3_BUCKET, key=S3_KEY)

# upload file to an S3 bucket
s3 = session.resource('s3')
s3.meta.client.upload_file("spark_job_emr.py", S3_BUCKET, S3_KEY)

response = client.run_job_flow(
    Name="EMR cluster - python deployment",
    ReleaseLabel="emr-7.8.0",
    Instances={
        'InstanceGroups': [
            {
                'Name': 'Master node',
                'Market': 'ON_DEMAND',  # Specify On-Demand for the master node
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1
            },
            {
                'Name': 'Core nodes',
                'Market': 'ON_DEMAND',  # Specify On-Demand for the core nodes
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
    },
    Applications=[
        {
            'Name': 'Spark'
        }
    ],
    Steps=[
    {
        'Name': 'Spark Program',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'client',
                's3://data-emr-bucket-store/deploy-on-console/spark_job_emr.py'
            ]
        }
    }
    ],
    VisibleToAllUsers=True,
    JobFlowRole='EMR_EC2_DefaultRole',
    ServiceRole='EMR_DefaultRole'
)

print(f"Cluster created with the following Job Flow ID: {response['JobFlowId']}")

job_flow_id = response['JobFlowId']
print("Job flow ID:", job_flow_id)

step_response = client.add_job_flow_steps(
    JobFlowId=job_flow_id, 
    Steps=[
    {
        'Name': 'Spark Program',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                's3://data-emr-bucket-store/deploy-on-console/spark_job_emr.py'
            ]
        }
    }
    ]
)

step_ids = step_response['StepIds']

print("Step IDs:", step_ids)