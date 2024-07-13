DROP DATABASE IF EXISTS airflow_db;

CREATE DATABASE airflow_db;

GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_admin;

ALTER DATABASE airflow_db OWNER TO airflow_admin;