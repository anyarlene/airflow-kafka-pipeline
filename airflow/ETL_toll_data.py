# import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash import BashOperator

# This makes scheduling easy
from airflow.utils.dates import days_ago


#defining DAG arguments

default_args = {
    'owner': 'dummy_name',
    'start_date': days_ago(0),
    'email': ['dummy@email.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# define the DAG

dag = DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval='@daily',
)

# define the tasks

# define the task to unzip the data

# Extract the .tgz file
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xzvf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment/',
    dag=dag
)

# Extract data from csv file # vehicle-data.csv

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command="awk -F ',' '{print $1,$2,$3,$4}' /home/project/airflow/dags/finalassignment/vehicle-data.csv > /home/project/airflow/dags/finalassignment/csv_data.csv",
    dag=dag
)


# Extract data from tsv file #tollplaza-data.tsv

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command="awk -F '\\t' '{print $5,$6,$7}' /home/project/airflow/dags/finalassignment/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/tsv_data.csv",
    dag=dag
)

# extract data from txt file #payment-data.txt 

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command="awk -F '\\t' '{print $6,$7}' /home/project/airflow/dags/finalassignment/payment-data.txt > /home/project/airflow/dags/finalassignment/fixed_width_data.csv",
    dag=dag
)

# consolidate data

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste -d , /home/project/airflow/dags/finalassignment/csv_data.csv /home/project/airflow/dags/finalassignment/tsv_data.csv /home/project/airflow/dags/finalassignment/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/extracted_data.csv',
    dag=dag
)


# transform and load data

transform_data = BashOperator(
    task_id='transform_data',
    bash_command="awk -F ',' '{ $4=toupper($4); print }' OFS=',' /home/project/airflow/dags/finalassignment/extracted_data.csv > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv",
    dag=dag
)


unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data



