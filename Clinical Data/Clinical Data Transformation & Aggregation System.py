
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator

# Define your DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='gcs_to_bigquery_dag',
    default_args=default_args,
    description='A simple DAG to load data from GCS to BigQuery',
    schedule_interval=None,  # Can be set to a cron schedule if needed
    start_date=days_ago(1),
    catchup=False,
    tags=['example'],
) as dag:

    start = EmptyOperator(task_id='start')

    # Task to load CSV from GCS to BigQuery
    load_csv_to_bq = GCSToBigQueryOperator(
        task_id='load_csv_to_bq',
        bucket='clinical1',
        source_objects=['Clinical Data_Discovery_Cohort.csv'],
        destination_project_dataset_table='spry-tesla-426720-e7.gcs_bq_dataset_load.gcs_bq_table',
        skip_leading_rows=1,  # Skip the header row if it exists
        source_format='CSV',
        create_disposition='CREATE_IF_NEEDED',  # Creates the table if it does not exist
        write_disposition='WRITE_TRUNCATE',  # Overwrites the table if it exists
        autodetect=True,  # Automatically infer schema
    )
    # Task 2: Clean the data (remove rows with null values)
    clean_data = BigQueryOperator(
        task_id='clean_data',
        sql="""
        CREATE OR REPLACE TABLE `spry-tesla-426720-e7.gcs_bq_dataset_load.cleaned_table` AS
        SELECT
            *
        FROM
            `spry-tesla-426720-e7.gcs_bq_dataset_load.gcs_bq_table`
        WHERE
            `Dead or Alive` IS NOT NULL
            AND `Date of Death` IS NOT NULL
            AND `Date of Death` != '.'
            AND `Date of Last Follow Up` IS NOT NULL
            AND `sex` IS NOT NULL
            AND `race` IS NOT NULL
            AND `Stage` IS NOT NULL
            AND `Event` IS NOT NULL
            AND `Time` IS NOT NULL;
        """,
        use_legacy_sql=False,
        dag=dag,
    )


    end = EmptyOperator(task_id='end')

    # Define task dependencies
    start >> load_csv_to_bq >> clean_data >> end
