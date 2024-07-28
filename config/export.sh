#!/bin/bash
echo "==============start exporting connections=================="
airflow connections export connections.json

echo "==============start exporting variables=================="
airflow variables export variables.json