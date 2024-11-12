from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

from backend.buoy_data.localized_wave import LocalizedWaveProcessor
from backend.break_consistency_classifier.wave_consistency_labeler import (
    WaveConsistencyLabeler,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# Daily DAG for LocalizedWaveProcessor
with DAG(
    "daily_wave_data",
    default_args=default_args,
    description="Daily wave data collection from NDBC stations",
    schedule_interval="0 2 * * *",  # Daily at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["wave_data"],
) as daily_dag:

    def process_wave_data():
        """Wrapper function to handle async operation"""
        processor = LocalizedWaveProcessor()
        processor.process_data()  # Using the synchronous process_data method

    # Check if the localized_wave table exists in the wave_data database
    check_db = PostgresOperator(
        task_id="check_database",
        postgres_conn_id="wave_db",
        sql="""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = 'localized_wave_data'
            );
        """,
    )

    # Process wave data
    process_wave_data = PythonOperator(
        task_id="process_wave_data",
        python_callable=process_wave_data,
        retries=3,
        retry_delay=timedelta(minutes=5),
    )

    check_db >> process_wave_data

# Monthly DAG for WaveConsistencyLabeler
with DAG(
    "monthly_wave_consistency",
    default_args=default_args,
    description="Monthly wave consistency calculation",
    schedule_interval="0 3 1 * *",  # Monthly on 1st at 3 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["wave_consistency"],
) as monthly_dag:

    def calculate_consistency():
        """Wrapper function for consistency calculation"""
        labeler = WaveConsistencyLabeler()
        labeler.run()

    # Check if source data is available
    check_source_data = PostgresOperator(
        task_id="check_source_data",
        postgres_conn_id="wave_db",
        sql="""
            SELECT COUNT(*)
            FROM localized_wave_data
            WHERE datetime >= NOW() - INTERVAL '30 days';
        """,
    )

    # Calculate consistency
    calculate_consistency = PythonOperator(
        task_id="calculate_consistency",
        python_callable=calculate_consistency,
    )

    check_source_data >> calculate_consistency
