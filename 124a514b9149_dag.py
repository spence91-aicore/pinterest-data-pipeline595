from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
import datetime

#Define params for Submit Run Operator
notebook_task = {
    'notebook_path': '/Workspace/Users/spence91@gmail.com/databricks/databricks_s3_to_databricks',
}


#Define params for Run Now Operator
notebook_params = {
    "Variable":5
}


default_args = {
    'owner': ' 124a514b9149',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=2)
}


with DAG('124a514b9149_dag',
    # should be a datetime format
    start_date=datetime.datetime(2024,8,25), # make sure this is a datetime!!
    # check out possible intervals, should be a string
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
    ) as dag:


    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_pintrest_analysis_run',
        # the connection we set-up previously
        databricks_conn_id='databricks_default',
        existing_cluster_id='1108-162752-8okw8dgg',
        notebook_task=notebook_task
    )
    opr_submit_run