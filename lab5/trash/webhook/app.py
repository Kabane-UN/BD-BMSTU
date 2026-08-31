from fastapi import FastAPI, Request
import requests

app = FastAPI()

AIRFLOW_URL = "http://airflow-api:8080/api/v2/dags/dbt_pipeline/dagRuns"

@app.post("/minio-event")
async def minio_event(request: Request):

    payload = await request.json()

    print(payload)

    response = requests.post(
        AIRFLOW_URL,
        auth=("airflow", "airflow"),
        json={}
    )

    print(response.status_code)
    print(response.text)

    return {
        "status": response.status_code
    }