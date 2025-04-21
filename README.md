# mobius

## Overview
- [Quick Start](#quick-start)
- [Test](#test)
- [Upgrade](#upgrade)

## Quick Start

### Initializing Environment

```bash
mkdir -p ./dags ./logs ./plugins ./config ./docker/ ./include
```

add `.env` file and change `x` to fit your project.

```bash
nano .env

AIRFLOW_UID=airflow

_AIRFLOW_WWW_USER_USERNAME=x
_AIRFLOW_WWW_USER_PASSWORD=x
_AIRFLOW_WWW_USER_FIRSTNAME=x
_AIRFLOW_WWW_USER_LASTNAME=x
_AIRFLOW_WWW_USER_EMAIL=x
_AIRFLOW_WWW_USER_ROLE=x
```

deployed by docker

```
docker compose up -d
```

## Test

```bash
docker exec -it airflow_webserver bash
cd dags
pytest | pytest -k xx.py
```

## Upgrade

change apache/airflow:x.x.x to some version you want to upgrade.

```bash
docker compose up -d --build
```