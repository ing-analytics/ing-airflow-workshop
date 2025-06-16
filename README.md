# Airflow Exercises

Practical exercises for the hands-on part of the "Introduction to Apache Airflow" workshop run by ING Analytics. 

## Setup

**PRE-REQUISITES:**

- [Docker Compose](https://docs.docker.com/compose/install/)


### Download Docker Images

```sh
docker pull mcr.microsoft.com/azure-sql-edge:1.0.7
docker pull minio/minio:RELEASE.2025-05-24T17-08-30Z
docker pull redis:7.2-bookworm
docker pull postgres:13
docker pull apache/airflow:2.10.5
```

### Run Airflow

Use the following command to spin up an Airflow deployment with Docker Compose:

```sh
docker compose -f airflow.docker-compose.yaml up -d
```

Wait for a few minutes for the containers to start, then access the Airflow console on <http://localhost:8080/home> with the username `airflow` and password `airflow`.


### Run S3 service

Run the following command to spin up the S3 and SQL services with Docker Compose:

```sh
docker compose -f compose.yaml up -d
```

You should then be able to go to the MinIO (S3) console on <http://localhost:9001/> with username `admin` and password `adminpassword`.

### Networking

All the containers share the `airflow-workshop` [network](https://docs.docker.com/compose/how-tos/networking/), and they can communicate with each other by using their service name. eg, the airflow workers can access the S3 buckets in the MinIO service from the endpoint `http://minio:9000`.
