import time
import random

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

args = {
    "owner": "pratik"
}

def may_fail(ti):
    num = random.random()
    if num > 0.5:
        raise ValueError("task FAILED")
    ti.xcom_push(key="result", value="fail")
    print("I'm Done")

def always_pass(ti):
    ti.xcom_push(key="result", value="passed")
    print("I'm Done")

def task8(ti):
    statuses = ti.xcom_pull(key="result", task_ids=["task-1", "task-2", "task-3", "task-4", "task-5", "task-6", "task-7"])
    for idx, val in enumerate(statuses):
        print(f"TASK ID:: {idx}, STATUS:: {val}")


with DAG(
    dag_id="xcom_DAG",
    default_args=args,
    start_date=days_ago(2),
    tags=['example', 'xcom']) as dag:

    task1 = PythonOperator(task_id="task-1", python_callable=always_pass)
    task2 = PythonOperator(task_id="task-2", python_callable=always_pass)
    task3 = PythonOperator(task_id="task-3", python_callable=always_pass)
    task4 = PythonOperator(task_id="task-4", python_callable=always_pass)
    task5 = PythonOperator(task_id="task-5", python_callable=always_pass)
    task6 = PythonOperator(task_id="task-6", python_callable=may_fail)
    task7 = PythonOperator(task_id="task-7", python_callable=always_pass)
    task8 = PythonOperator(task_id="task-8", python_callable=task8)

    task1 >> [task2, task3]
    task3 >> [task4, task5]
    task2 >> task6
    task6 >> task7

    [task4, task5, task7] >> task8