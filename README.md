# yelp-noaa-data-engineering


### Data Acquisition
1. NOAA weather data - you would need the API key. Refer to this site for getting the token. Link [here](https://www.ncdc.noaa.gov/cdo-web/webservices/v2)
2. Yelp dataset - sample datasets can be found [here](https://www.yelp.com/dataset). Here is the link to acquire the data from yelp if you would want to do it for your organization [link](https://www.yelp.com/knowledge)

### Infrastructure
The project is using AWS. You would need to setup:
1. AWS credential - Create a IAM users with AdministratorAccess. Obtain the access key ID and access secret key.
2. Redshift - setup a redshift cluster. Recommend to use dc2.large and 2 nodes to save on dev cost
3. EMR - it will be spin up and down by Airflow
4. Airflow - you can spin up the Airflow in [AWS](https://aws.amazon.com/managed-workflows-for-apache-airflow/) or in [GCP](https://cloud.google.com/composer/).
  a. Note1: the version used is v1.10.2. It will have some functionality difference and library import differences compare to Airflow 2.0+
  b. Note2: the CLI command may have difference between v1+ and v2+. Please refer to Airflow official documentation.

  

### Pre-requisites
1. Setup the required infrastructure
1. use the DDL.sql to create all the table schema in redshift
2. Setup all the connection in Airflow (NOAA token, aws_default, emr_default, redshift).Refer to AirflowCLI to add the connection, or you can add them using the Airflow UI. Instruction [here](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html)
  a. CLI command as below:
          
          airflow connections -a --conn_id 'noaa_token' --conn_type ='' --conn_password '<your_noaa_api_token>'
          airflow connections -a --conn_id 'redshift' --conn_type ='Postgres' --conn_host '<your_redshift_url>' --conn_schema '<your_redshift_database_name>' --conn_port '5439' --conn_login '<your_redshift_username>' --conn_password '<your_redshift_password>'
          airflow connections -d --conn_id 'aws_default'
          airflow connections -a --conn_id 'aws_default' --conn_type ='Amazon Web Services'  --conn_login '<your_aws_credential_id>' --conn_password '<your_aws_secret_key>' --conn_extra '{"region_name": "us-west-2"}'
          airflow connections -d --conn_id 'emr_default'
          airflow connections -a --conn_id 'emr_default' --conn_type ='Elastic MapReduce' --conn_extra '{ "Name": "emr_etl_job",
          "LogUri":"s3://your_emr_aws/s3_path/",
          "Instances": 
          { "Ec2KeyName": "emr",
          "Ec2SubnetId": "<your_subnet_id",
          "MasterInstanceType": "m4.xlarge",
          "SlaveInstanceType": "m4.xlarge",
          "InstanceCount": 3,
          "KeepJobFlowAliveWhenNoSteps": true,
          "TerminationProtected": false},
          "Applications": [
            {"Name": "Spark"},
            {"Name": "Hadoop"}],
          "JobFlowRole": "EMR_EC2_DefaultRole",
          "ServiceRole": "EMR_DefaultRole",
          "ReleaseLabel": "emr-5.33.0",
          "VisibleToAllUsers": true}'

### Instruction
1. Run the Airflow DAG 1_yelp_data_dag.py, 2_NOAA_data_dag.py,3_stg_fdn_dimensions.py,4_stg_to_fdn_fact.py
