from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
@dag(
    dag_id='dbt_main_pipeline',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['global_banking', 'dbt', 'taskflow'],
)


def dbt_main_pipeline():
    @task
    def run_dbt():
        """
        This task runs the dbt run command.
        """
        import subprocess

        activate_env = "/Users/camifandino/Documents/projects/python_envs/global-tech-p3/bin/activate"
        dbt_project_dir = "/Users/camifandino/Documents/projects/global-banking/dbt/dbt_global_banking"
        command = f"source {activate_env} && dbt run --project-dir {dbt_project_dir}"
        subprocess.run(command, shell=True, check=True, executable='/bin/bash')
    
    # @task
    # def run_dbt():
    #     activate_env = "env_global_banking"
    #     dbt_project_dir = "/Users/camifandino/Documents/projects/global-banking/dbt/dbt_global_banking"
    #     run_dbt_task = BashOperator(
    #         task_id='run_dbt',
    #         bash_command=f"{activate_env} && dbt run --project-dir {dbt_project_dir}",
    #     )
    #     run_dbt_task.execute(dict())


    @task
    def delete_no_consent_users():
        """
        This tasks runs an Update statement that deletes identfiable data
        for users that witdrawn their consent.
        """
        # Read SQL query from a file
        with open('/Users/camifandino/Documents/projects/global-banking/dwh/etl/jobs/DELETE_JOB.sql', 'r') as file:
            sql_query = file.read()
        
        delete_job = BigQueryExecuteQueryOperator(
            task_id='run_bigquery_delete_query',
            sql=sql_query,
            use_legacy_sql=False,
            bigquery_conn_id='gcp_global_banking'
        )
        delete_job.execute(dict())

    # Define task dependencies
    dbt_task = run_dbt()
    delete_task = delete_no_consent_users()

    dbt_task >> delete_task

# Instantiate the DAG
dag_instance = dbt_main_pipeline()