FROM apache/airflow:2.9.1

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         gcc \
         heimdal-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN addgroup leaf

USER airflow

ADD requirements.txt .

RUN pip install --no-cache-dir "apache-airflow==2.9.1" -r requirements.txt
