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


### Instruction
1. Run the Airflow DAG 1_yelp_data_dag.py, 2_NOAA_data_dag.py
