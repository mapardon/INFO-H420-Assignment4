from datetime import datetime
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import tarfile

default_args = {
    'owner': 'group_23',
    'retries': 0
}

dag = DAG('process_web_log',
          schedule_interval='@daily',
          default_args=default_args,
          start_date=datetime(2023, 1, 5))


def fun_scan_for_log():
    if not os.path.exists("dags/data/the_logs/log.txt"):
        raise ValueError('File not found.')
    return True


def fun_extract_data():
    no_duplicates = list()
    with open("dags/data/the_logs/log.txt", 'r', encoding='utf-8') as fin:
        with open("dags/data/the_logs/extracted_data.txt", 'w', encoding='utf-8') as fout:
            for ln in fin:
                ipaddr = ln.split(" - - ")[0]
                if ipaddr not in no_duplicates:
                    no_duplicates.append(ipaddr)
                    fout.write(ipaddr + '\n')


def fun_transform_data():
    with open("dags/data/the_logs/extracted_data.txt", 'r', encoding='utf-8') as fin:
        with open("dags/data/the_logs/transformed_data.txt", 'w', encoding='utf-8') as fout:
            for ln in fin:
                ipaddr = ln.split(" - - ")[0]
                if ipaddr != "198.46.149.143":
                    fout.write(ipaddr)


def fun_load_data():
    name_of_file = "weblog.tar"
    with tarfile.open(name_of_file, "w") as f:
        f.add("dags/data/the_logs/transformed_data.txt")


scan_for_log = PythonOperator(
    task_id='scan_for_log',
    python_callable=fun_scan_for_log,
    dag=dag
)

extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=fun_extract_data,
    dag=dag)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=fun_transform_data,
    dag=dag)

load_data = PythonOperator(
    task_id='load_data',
    python_callable=fun_load_data,
    dag=dag)

scan_for_log >> extract_data >> transform_data >> load_data