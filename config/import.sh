#!/bin/bash
echo "==============start importing connections=================="
airflow connections import connections.json

echo "==============start importing variables=================="
airflow variables import variables.json