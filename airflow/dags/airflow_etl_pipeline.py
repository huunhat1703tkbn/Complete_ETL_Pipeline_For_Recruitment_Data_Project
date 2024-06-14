from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import os



def check_new_data(**kwargs):
    ti = kwargs['ti']
    try:
        # Thực thi spark-submit để kiểm tra dữ liệu mới
        command = r"""
            cd C:\Users\admin\PycharmProjects\Recruiment data ETL Pipeline Project\ETL_Job;
            spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 check_new_data.py;
        """
        os.system(command)
        
        # Lưu giá trị mới vào XCom
        ti.xcom_push(key='new_data', value=True)  # Assume successful execution means new_data is True
            
    except Exception as e:
        # Log any exceptions or errors
        print(f"Error in check_new_data: {str(e)}")
        ti.xcom_push(key='new_data', value=False)  # Set new_data to False if there's an error
        raise

def run_etl(**kwargs):
    try:
        # Thực thi ETL pipeline
        command = r"""
            cd C:\Users\admin\PycharmProjects\Recruiment data ETL Pipeline Project\ETL_Job;
            spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 etl_pipeline.py;
        """
        os.system(command)
    except Exception as e:
        # Log any exceptions or errors
        print(f"Error in run_etl: {str(e)}")
        raise

def no_new_data_found(**kwargs):
    print("No new data found")

# Khởi tạo DAG
dag = DAG(
    'my_dag',
    description='DAG to trigger pySpark job',
    schedule_interval='0 8 * * *',
    start_date=days_ago(1)
)

task1 = PythonOperator(
    task_id='check_new_data',
    python_callable=check_new_data,
    provide_context=True,
    dag=dag,
)

task2 = PythonOperator(
    task_id='run_etl',
    python_callable=run_etl,
    provide_context=True,
    dag=dag,
)

task3 = PythonOperator(
    task_id='no_new_data_found',
    python_callable=no_new_data_found,
    provide_context=True,
    dag=dag,
)

def choose_task(**kwargs):
    ti = kwargs['ti']
    # Lấy giá trị từ XCom
    new_data = ti.xcom_pull(task_ids='check_new_data', key='new_data')
    if new_data:
        return 'run_etl'
    else:
        return 'no_new_data_found'

choose_task = PythonOperator(
    task_id='choose_task',
    python_callable=choose_task,
    provide_context=True,
    dag=dag,
)

task1 >> choose_task >> [task2, task3]
