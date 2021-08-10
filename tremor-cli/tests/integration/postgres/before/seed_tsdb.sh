#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE measurements;
    \c measurements;
    CREATE TABLE sensors(
    id SERIAL PRIMARY KEY,
    type VARCHAR(50),
    location VARCHAR(50)
    );
    CREATE TABLE sensor_data (
    time TIMESTAMPTZ NOT NULL,
    sensor_id INTEGER,
    temperature REAL,
    humidity REAL,
    FOREIGN KEY (sensor_id) REFERENCES sensors (id)
    );
    INSERT INTO sensors (type, location) VALUES
    ('a','floor'),
    ('a', 'ceiling'),
    ('b','floor'),
    ('b', 'ceiling');
    SELECT create_hypertable('sensor_data', 'time');

EOSQL
