# create an emr instance
aws emr create-cluster \
    --name "EMR Cluster - CLI" \
    --release-label "emr-7.8.0" \
    --use-default-roles \
    --applications Name=Hadoop Name=Hive Name=JupyterEnterpriseGateway Name=Livy Name=Spark \
    --ec2-attributes SubnetId="subnet-eef49ca3" \
    --instance-type m5.xlarge \
    --instance-count 2 \
    --scale-down-behavior "TERMINATE_AT_TASK_COMPLETION" \
    --auto-termination-policy IdleTimeout=3600 \
    --region "us-east-1" \
    --profile manager

#create cluster  with job flow
aws emr create-cluster \
 --name "notebookCluster" \
 --log-uri "s3://aws-logs-189128986856-us-east-1/elasticmapreduce" \
 --release-label "emr-7.8.0" \
 --service-role "arn:aws:iam::189128986856:role/EMR_DefaultRole" \
 --unhealthy-node-replacement \
 --ec2-attributes '{"InstanceProfile":"EMR_EC2_DefaultRole","EmrManagedMasterSecurityGroup":"sg-04b24b0b1e921adf7","EmrManagedSlaveSecurityGroup":"sg-0cd31e5349681a2c2","AdditionalMasterSecurityGroups":[],"AdditionalSlaveSecurityGroups":[],"SubnetIds":["subnet-eef49ca3"]}' \
 --applications Name=Hadoop Name=Hive Name=JupyterEnterpriseGateway Name=Livy Name=Spark \
 --instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","Name":"Primary","InstanceType":"m5.xlarge","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp2","SizeInGB":32},"VolumesPerInstance":2}]}},{"InstanceCount":1,"InstanceGroupType":"CORE","Name":"Core","InstanceType":"m5.xlarge","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"VolumeType":"gp2","SizeInGB":32},"VolumesPerInstance":2}]}}]' \
 --steps '[{"Name":"jobcluster","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Args":["spark-submit","--deploy-mode","cluster","s3://data-emr-bucket-store/deploy-on-console/spark_job_emr.py"],"Type":"CUSTOM_JAR"}]' \
 --scale-down-behavior "TERMINATE_AT_TASK_COMPLETION" \
 --auto-termination-policy '{"IdleTimeout":3600}' \
 --region "us-east-1"


# add steps to the cluster
aws emr add-steps \
    --cluster-id j-2MEQ475JXRSGC \
    --steps Type=CUSTOM_JAR,Name="Spark Program",ActionOnFailure=CONTINUE,Jar="command-runner.jar",Args=["spark-submit","--deploy-mode","client","s3://data-emr-bucket-store/deploy-on-console/spark_job_emr.py"] \
    --region "us-east-1" \
    --profile manager

aws emr add-steps \
    --cluster-id j-32ZWBLZK82C9W \
    --steps Type=CUSTOM_JAR,Name="Spark Program",ActionOnFailure=CONTINUE,Jar="command-runner.jar",Args=["spark-submit","--deploy-mode","cluster","s3://data-emr-bucket-store/deploy-on-console/spark_job_emr.py"] \
    --region "us-east-1" \
    --profile manager

aws emr terminate-clusters \
    --cluster-id j-32ZWBLZK82C9W \
    --region us-east-1 \
    --profile manager