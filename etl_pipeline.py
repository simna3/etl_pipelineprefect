from prefect import flow, task
from prefect.deployments import Deployment
from prefect.filesystems import GitHub
from prefect.server.schemas.schedules import CronSchedule
import pandas as pd
import random
import time

@task
def extract_data():
    """Simulates data extraction from an API or database."""
    print("Extracting data...")
    data = {
        "id": list(range(1, 6)),
        "value": [random.randint(1, 100) for _ in range(5)]
    }
    df = pd.DataFrame(data)
    time.sleep(2)  # Simulate delay
    return df

@task
def transform_data(df: pd.DataFrame):
    """Processes extracted data."""
    print("Transforming data...")
    df["value"] = df["value"] * 2  # Example transformation
    time.sleep(2)
    return df

@task
def load_data(df: pd.DataFrame):
    """Loads transformed data into storage."""
    print("Loading data...")
    df.to_csv("transformed_data.csv", index=False)
    print("Data saved to transformed_data.csv")
    time.sleep(2)

@flow
def etl_pipeline():
    """Main ETL pipeline flow."""
    data = extract_data()
    transformed_data = transform_data(data)
    load_data(transformed_data)

# Deployment setup
deployment = Deployment.build_from_flow(
    flow=etl_pipeline,
    name="ETL_Pipeline_Deployment",
    schedule=CronSchedule(cron="0 * * * *"),  # Runs every hour
    work_queue_name="default",
    storage=GitHub(
        repository="your-github-repo",
        reference="main",
        access_token="your_github_token"
    )
)

if __name__ == "__main__":
    etl_pipeline()
