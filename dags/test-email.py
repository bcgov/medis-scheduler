from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from datetime import datetime
from airflow.operators.empty import EmptyOperator

def send_success_status_email(context):
    task_instance = context['task_instance']
    task_status = task_instance.current_state()

    subject = f"Airflow Task {task_instance.task_id} {task_status}"
    body = f"The task {task_instance.task_id} finished with status: {task_status}.\n\n" \
           f"Task execution date: {context['execution_date']}\n" \
           f"Log URL: {task_instance.log_url}\n\n"

    to_email = "abc@example.com"  # Specify the recipient email address

    send_email(to=to_email, subject=subject, html_content=body)

def send_failure_status_email(context):
    task_instance = context['task_instance']
    task_status = task_instance.current_state()

    subject = f"Airflow Task {task_instance.task_id} {task_status}"
    body = f"The task {task_instance.task_id} finished with status: {task_status}.\n\n" \
           f"Task execution date: {context['execution_date']}\n" \
           f"Log URL: {task_instance.log_url}\n\n"

    to_email = "tatiana.pluzhnikova@cgi.com"  # Specify the recipient email address

    send_email(to=to_email, subject=subject, html_content=body)


# Create a DAG and define your tasks
dag = DAG(
    'email_example',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None
)

    start_task = EmptyOperator(
        task_id="executetask",
    )
task_to_watch = executetask

success_email_task = PythonOperator(
    task_id='success_email_task',
    python_callable=send_success_status_email,
    provide_context=True,
    dag=dag
)

failure_email_task = PythonOperator(
    task_id='failure_email_task',
    python_callable=send_failure_status_email,
    provide_context=True,
    dag=dag
)

# Set the on_success_callback and on_failure_callback
success_email_task.set_upstream(task_to_watch)
failure_email_task.set_upstream(task_to_watch)