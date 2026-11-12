"""
Currency Exchange ETL pipeline (Astro CLI + Airflow + MongoDB)

Extract   -> pulls daily EUR exchange rates from the Frankfurter API (v2)
Transform -> reshapes each rate into the required document format
Load      -> upserts the documents into MongoDB (currency_db.exchange_rates)

Data moves between tasks through NAMED XComs (explicit keys), not return values.
"""

import os
from datetime import datetime, timedelta, timezone

import pendulum
import requests
from pymongo import MongoClient

# Airflow 3 (Astro Runtime 3.x) imports, with a fallback for Airflow 2.x
try:
    from airflow.sdk import DAG
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # Airflow 2.x
    from airflow import DAG
    from airflow.operators.python import PythonOperator


# ----------------------------- Configuration -----------------------------
API_URL = "https://api.frankfurter.dev/v2/rates"
BASE_CURRENCY = "EUR"
TARGET_CURRENCIES = ["USD", "GBP", "JPY", "CHF", "CAD"]

# Set in .env; "mongo" is the MongoDB service name on the shared Docker network
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin123@mongo:27017/?authSource=admin")
MONGO_DB = os.getenv("MONGO_DB", "currency_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "exchange_rates")

# Named XCom keys
XCOM_RAW_RATES = "raw_exchange_rates"
XCOM_TRANSFORMED_RATES = "transformed_exchange_rates"


# ------------------------------- Task 1: Extract -------------------------------
def extract_exchange_rates(**context):
    """Call the Frankfurter API and push the raw JSON list as a named XCom."""
    params = {"base": BASE_CURRENCY, "quotes": ",".join(TARGET_CURRENCIES)}
    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()
    raw_rates = response.json()
    # Example item: {"date": "2026-11-11", "base": "EUR", "quote": "USD", "rate": 1.1189}

    if not raw_rates:
        raise ValueError("Frankfurter API returned no exchange rates")

    context["ti"].xcom_push(key=XCOM_RAW_RATES, value=raw_rates)
    print(f"Extracted {len(raw_rates)} rates for base {BASE_CURRENCY}: {raw_rates}")


# ------------------------------ Task 2: Transform ------------------------------
def transform_exchange_rates(**context):
    """Pull raw rates via named XCom, reshape them, push transformed docs as a named XCom."""
    ti = context["ti"]
    raw_rates = ti.xcom_pull(task_ids="extract_exchange_rates", key=XCOM_RAW_RATES)
    if not raw_rates:
        raise ValueError("No raw rates found in XCom")

    retrieved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    transformed = []
    for item in raw_rates:
        transformed.append(
            {
                "base_currency": item["base"],
                "target_currency": item["quote"],
                "exchange_rate": float(item["rate"]),
                "rate_date": item["date"],
                "source": "Frankfurter API",
                "retrieved_at": retrieved_at,
            }
        )

    ti.xcom_push(key=XCOM_TRANSFORMED_RATES, value=transformed)
    print(f"Transformed {len(transformed)} records:")
    for doc in transformed:
        print(doc)


# -------------------------------- Task 3: Load --------------------------------
def load_to_mongodb(**context):
    """Pull transformed docs via named XCom and upsert them into MongoDB."""
    ti = context["ti"]
    records = ti.xcom_pull(task_ids="transform_exchange_rates", key=XCOM_TRANSFORMED_RATES)
    if not records:
        raise ValueError("No transformed records found in XCom")

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    try:
        collection = client[MONGO_DB][MONGO_COLLECTION]

        # One document per currency pair per day, so re-runs do not create duplicates
        collection.create_index(
            [("base_currency", 1), ("target_currency", 1), ("rate_date", 1)],
            unique=True,
            name="pair_date_unique",
        )

        inserted, updated = 0, 0
        for doc in records:
            key = {
                "base_currency": doc["base_currency"],
                "target_currency": doc["target_currency"],
                "rate_date": doc["rate_date"],
            }
            result = collection.update_one(key, {"$set": doc}, upsert=True)
            if result.upserted_id is not None:
                inserted += 1
            else:
                updated += 1

        print(f"MongoDB load complete -> inserted: {inserted}, updated: {updated}")
        print(f"Total documents in {MONGO_DB}.{MONGO_COLLECTION}: {collection.count_documents({})}")
    finally:
        client.close()


# ---------------------------------- DAG ----------------------------------
default_args = {
    "owner": "shaf",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="currency_exchange_etl",
    description="Daily EUR exchange rates from Frankfurter API into MongoDB",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["etl", "currency", "mongodb"],
) as dag:

    extract = PythonOperator(
        task_id="extract_exchange_rates",
        python_callable=extract_exchange_rates,
    )

    transform = PythonOperator(
        task_id="transform_exchange_rates",
        python_callable=transform_exchange_rates,
    )

    load = PythonOperator(
        task_id="load_to_mongodb",
        python_callable=load_to_mongodb,
    )

    extract >> transform >> load
