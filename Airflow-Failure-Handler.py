from airflow.sdk import dag, task
from datetime import timedelta


# This DAG demonstrates how to handle task failures in Airflow using retries and exponential backoff.


default_args = {
    'retries': 3, # Number of retries before failing the task
    'retry_delay': timedelta(minutes=5), # Delay between retries
    'retry_exponential_backoff': True, # Use exponential backoff for retries
    'max_retry_delay': timedelta(hours=1) # Maximum delay between retries   
    }

@dag(
    dag_id='airflow_failure_handler',   
    default_args=default_args,  
    schedule='@daily',   
    catchup=False
)

def process_workflow():
    @task
    def task1():
        # Simulate a task that may fail
        import random
        if random.choice([True, False]):
            raise Exception("Task 1 failed!")
        return "Task 1 completed successfully."

    @task
    def task2():
        # Simulate a task that may fail
        import random
        if random.choice([True, False]):
            raise Exception("Task 2 failed!")
        return "Task 2 completed successfully."

    @task
    def task3():
        # Simulate a task that may fail
        import random
        if random.choice([True, False]):
            raise Exception("Task 3 failed!")
        return "Task 3 completed successfully."

    # Define the workflow
    t1 = task1()
    t2 = task2()
    t3 = task3()

    t1 >> t2 >> t3

process_workflow()

