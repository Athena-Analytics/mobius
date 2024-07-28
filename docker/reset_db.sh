#!/bin/bash

# Set PostgreSQL connection variables
DB_HOST="localhost"
DB_NAME="postgres"
DB_USER="postgres"

# Path to your DDL file
DDL_FILE="reset_db.sql"

# Construct the psql command
PSQL_CMD="psql -h $DB_HOST -d $DB_NAME -U $DB_USER -W -f $DDL_FILE"

# Execute the DDL using psql
echo "Executing DDL from $DDL_FILE..."
eval "$PSQL_CMD"

# Check for errors
if [ $? -eq 0 ]; then
  echo "DDL executed successfully."
else
  echo "Error executing DDL."
fi