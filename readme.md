# Pintrest Data Pipeline

## Scenario:

Pinterest crunches billions of data points every day to decide how to provide more value to their users. This is an attempt to demonstrate how to create a (simplified) similar system using Amazon Web Services.

## Scenario Architecture

![Demo architecture](img/Pintrest_pipeline_arch.jpg)

This code base is concerned with creating inputs and processing outputs for Pintrest-style data. 

## Scenario Demonstrations

There's two main demonstrations:

* Batch Processing - where sample Pinterest data is sent via a configured Amazon API Gateway to a Kafka producer, is written to an S3 bucket, which is then consumed by Databricks, and finally cleansed and processed for valuable insights. The batch processing schedule is handled by Amazon Managed Workflows for Apache Airflow.
* Streaming data processing - where sample Pinterest data is sent to an Amazon Kinesis Data Stream which is then consumed by Databricks and finally cleansed and written to a Databricks Delta table

(Please note that creating, and configuring a Kafka/MSK instance is *out of scope* for this project)

 At the end of this readme, there will be an explanation of how some of the AWS services
 were configured.


# File Structure

For the batch processing demonstration:

* `user_posting_emulation.py` -  This script pulls sample Pinterest data, raw data, from a database, and then sends the data to a configured Amazon API gateway.  The gate endpoints have been configured so that the data is sent to a Kafka producer. This file can be run locally.

* `databricks_s3_to_databricks.ipynb` -  This notebook is run on the Databricks platform and is scheduled to run using Amazon managed workflows for Apache Airflow.  Pinterest data is pulled from an Amazon S3 bucket, the data is cleansed and then finally the data is processed for insights.  Each step is commented in the file itself.

* `124a514b9149_dag.py` - This is a DAG ("Directed Acyclic Graph").  This is a definition for a workflow to be run on Amazon Managed Workflows for Apache Airflow. It will attempt to run the notebook mentioned above on a daily schedule.

For the streaming demonstration:

* `user_posting_emulation_streaming.py` - This script pulls sample Pinterest data, raw data, from a database, and then sends the data to a configured Amazon API gateway (different endpoints to the batch demo).  The gate endpoints have been configured so that the data is sent to an Amazon Kinesis data stream instance. This file can be run locally.

* `databricks_kinisis_to_databricks.ipynb` -  This notebook is run on the Databricks platform.  It reads data from Kinesis streams, it cleanses the data and then writes the cleansed data to tables in Databricks.  This can be set to be run indefinitely so that the streaming data is constantly processed and written.


Other files/dirs

* `img/` - image files used 
* `environment.yml` - the dependancies for locally run files. Can be used with conda to install an environemnt.

## Local Installation

To grab the code:

```
git clone https://github.com/spence91-aicore/pinterest-data-pipeline595.git
```

### Local Dependancies

For the files that can be run locally, the project dependencies can be found in the `environment.yml` file.

If you have Conda installed, you can install an environment using the following command.

```
conda env create -f .\environment.yml 
```

Please note - other package versions **may** work fine, but the versions outlined is what **has been tested**.

If you want to run the notebooks then you'll also need to have ipykernel installed as well.

```
python -m pip install ipykernel==6.29
```

### Local Config

The two files concerned with emulating user posts: `user_posting_emulation`, and `user_posting_emulation_streaming` require a database connection to pull sample data. This detail can be put in a file in the root directory of the project, called `db_creds.yml`

e.g

```
cd pintrest_data_pipeline
touch db_creds.yml
```

Example `db_creds.yml` file, replace all `$....` with actual details.

```
RDS_HOST: $PINTERESTDB
RDS_PASSWORD: $PINTERESTDBPW
RDS_USER: $DBUSER
RDS_DATABASE: $DBNAME
RDS_PORT: $DBPORT
```

# AWS Configurations


*TBC*