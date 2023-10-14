# Spark ETL Pipeline for Transactional Data

## Project Description

This project implements an ETL (Extract, Transform, Load) pipeline using Apache Spark to aggregate transactional data and store the results in a PostgreSQL database. The goal is to efficiently process large amounts of transaction data and provide aggregated information for interactive queries.

## Overview

The system collects transactional data and stores them as CSV files. A sample input can be found in `data/transaction.csv`. The ETL pipeline processes this data, aggregates transactions by user ID, and stores the results in a PostgreSQL database.

## Implementation

* **Dockerization**: The application is Dockerized using a Dockerfile and a `docker-compose` service. The base image is the official Python 3.9 image. Java 8 or Java 11 is required to run Spark.

* **PostgreSQL Integration**: The Docker-compose file includes a container for PostgreSQL. The ETL pipeline writes its output to this PostgreSQL database.

* **Entry Point and Arguments**: The entry point of the application is `main.py`. It serves as the Python command-line tool that accepts source file path, database name, and table name as arguments.

* **Target Schema**: Aggregated customer data is stored in the PostgreSQL database in a table named `customers` with specific columns: `customer_id`, `favourite_product`, and `longest_streak`.

* **Integration Test**: An integration test runs the ETL pipeline using the provided sample input file `data/transaction.csv`. It writes the results to PostgreSQL and validates specific values for a given `customer_id`.

## Usage

To run the ETL pipeline, use the following command:

```shell
# build the image
docker-compose build 

# run the container
docker-compose up  

# check the container ID  
docker ps

# run the ETL pipeline
docker-compose exec etl_app poetry run python main.py --source_file_path /opt/data/transaction.csv --database postgres --table customers

# To run the integration test
docker-compose exec integration_test pytest integration_test.py
```