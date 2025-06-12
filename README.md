# Airflow Exercises

Practical exercises for the hands-on part of the "Introduction to Apache Airflow" workshop run by ING Analytics. 

## *PRE-REQUISITES*

- Requires [Python >= 3.10](https://www.python.org/downloads/)

### Install Python Packages

```sh
brew install unixodbc
brew install awscli

```py
pip install -r requirements.txt
```

If you encounter `fatal error: 'sqlfront.h' file not found` during installation of `apache-airflow-providers-microsoft-mssql`, run the following commands:

```sh
brew uninstall --force freetds
brew install freetds
brew link --force freetds
```

### Download Docker Images

```sh
docker pull andrewgaul/s3proxy:sha-e5c20b6
docker pull mcr.microsoft.com/azure-sql-edge:1.0.7
```

### Run S3 & SQL Server

Run the following command to spin up the S3 and SQL services with Docker Compose:

```docker
docker compose up -d
```

## Test s3proxy connection

```sh
export AWS_ACCESS_KEY_ID=hello-access-key
export AWS_SECRET_ACCESS_KEY=hello-secret-key
aws s3api list-objects --bucket workshop --endpoint http://localhost:8083
```

OR

```py
python test_s3proxy.py
```

## Run Airflow

```sh
python3 -m virtualenv .venv
source ./.venv/bin/activate
pip install -r requirements.txt
export AIRFLOW_HOME=$PWD
airflow standalone
```
