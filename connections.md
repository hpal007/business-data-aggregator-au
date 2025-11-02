# Airflow Connections Setup Guide

## PostgreSQL Connection

To set up the required connections in Airflow, use one of the following methods:

### Method 1: Using Airflow CLI in bash
```bash
# First, access the Airflow CLI
docker compose run airflow-cli bash

# Then run the connection command
airflow connections add 'target_postgres' \
    --conn-type 'postgres' \
    --conn-host 'target_postgres' \
    --conn-login 'spark_user' \
    --conn-password 'spark_pass' \
    --conn-port '5432' \
    --conn-schema 'target_db'
```

### Method 2: Direct Connection Command
```bash
docker compose run airflow-cli airflow connections add 'target_postgres' \
    --conn-type 'postgres' \
    --conn-host 'target_postgres' \
    --conn-login 'spark_user' \
    --conn-password 'spark_pass' \
    --conn-port '5432' \
    --conn-schema 'target_db'
```

## Spark Connection Setup
```bash
docker compose run airflow-cli airflow connections add 'spark_default' \
    --conn-type 'spark' \
    --conn-host 'spark://spark-master' \
    --conn-port '7077'
```

## Important Requirements ⚠️
1. Ensure all services are configured to use the same Docker network
2. Verify that airflow-worker, spark-master, and spark-worker are using the same Python version (Python 3.10.12)

