#!/bin/bash
# Script to create Airflow connections

# Wait for Airflow to be ready
sleep 10

# Create Postgres source connection
airflow connections add 'postgres_banking' \
    --conn-type 'postgres' \
    --conn-host 'postgres-banking' \
    --conn-schema 'bankingdb' \
    --conn-login 'bankinguser' \
    --conn-password 'bankingpass' \
    --conn-port '5432'

echo "Airflow connections created successfully!"