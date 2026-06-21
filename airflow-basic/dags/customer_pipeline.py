from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

DATA_FILE = "/opt/airflow/data/customers.csv"
OUTPUT_DIR = "airflow-basic/output"


def extract_data(ti):
    df = pd.read_csv(DATA_FILE)

    print("Data extracted successfully")
    print(df)

    customers = df.to_dict(orient="records")

    ti.xcom_push(
        key="customers",
        value=customers
    )


def load_database(ti):
    customers = ti.xcom_pull(
        task_ids="extract_customers",
        key="customers"
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with open(f"{OUTPUT_DIR}/customers_loaded.txt", "w") as f:
        for customer in customers:
            f.write(
                f"Loaded Customer {customer['customer_id']}\n"
            )

    print("Database load complete")


def send_welcome_email(ti):
    customers = ti.xcom_pull(
        task_ids="extract_customers",
        key="customers"
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with open(f"{OUTPUT_DIR}/emails_sent.txt", "w") as f:
        for customer in customers:
            f.write(
                f"Welcome email sent to {customer['email']}\n"
            )

    print("Emails sent successfully")


with DAG(
    dag_id="customer_onboarding",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    extract_customers = PythonOperator(
        task_id="extract_customers",
        python_callable=extract_data
    )

    load_customers = PythonOperator(
        task_id="load_customers",
        python_callable=load_database
    )

    send_emails = PythonOperator(
        task_id="send_emails",
        python_callable=send_welcome_email
    )

    extract_customers >> load_customers >> send_emails