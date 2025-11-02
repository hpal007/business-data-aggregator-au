## airflow_spark_docker — quickstart

This repository contains a local Airflow + Spark development environment wired together with Docker Compose.

Files of interest
- `docker-compose.yaml` — orchestrates services: `postgres`,`target_postgres`, `redis`, `spark-master`, `spark-worker`, and multiple Airflow components.
- `Dockerfile.airflow_worker` — custom image used by the `airflow-worker` service (installs Java & PySpark).
- `dags/` — example DAGs demonstrating download/unzip and Spark-based XML processing.
- `config/airflow.cfg` — Airflow configuration mounted into containers.
- `requirements.txt` — Python packages used by the project (for local development and reference).

Processed data storage
----------------------

This stack includes a second Postgres instance named `target_postgres` which is intended as the destination for processed data produced by Spark jobs and Airflow tasks. Key details:

- Service name (container): `target_postgres`
- Host port (mapped to container's 5432): `5433` (access on the host at localhost:5433)
- Database name: `target_db`
- User / password: `spark_user` / `spark_pass`

Airflow initialization (`airflow-init`) also creates an Airflow connection named `target_postgres` that points to this database (conn host `target_postgres`, conn port `5432`, schema `target_db`) so DAGs and Spark jobs can write/read processed data there.

Quickstart (local)
1. Copy the example env file and edit if you want custom UIDs/paths:

```bash
cp .env.sample .env
# edit .env if needed (AIRFLOW_UID, AIRFLOW_PROJ_DIR, etc.)
```

2. Start the stack (rebuild images if you changed Dockerfiles):

```bash
docker-compose up --build
```

3. The Airflow webserver will be available at http://localhost:8080 (credentials are created by the `airflow-init` step using env vars in `.env` if set).

