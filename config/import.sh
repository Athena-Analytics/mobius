#!/bin/bash
echo "==============start import connections=================="
airflow connections import connections.json

echo "==============start import variables=================="
airflow variables import variables.json