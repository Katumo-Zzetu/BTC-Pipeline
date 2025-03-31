# BTC Price ETL Pipeline

## Overview
This repository contains an automated ETL pipeline that extracts daily Bitcoin (BTC) price data from the Polygon.io API, performs basic transformations, and loads the data into a PostgreSQL database. The workflow is orchestrated using Apache Airflow.

## Components
- **btc_prices.py**: Handles the extraction of BTC price data, transformation, and loading into PostgreSQL
- **btc_dag.py**: Airflow DAG definition that schedules and orchestrates the ETL pipeline on a daily basis

## Features
- Daily extraction of BTC open and close prices
- Data transformation using pandas
- Secure database connection using environment variables
- Automated workflow orchestration with Apache Airflow
- Error handling and retry mechanisms

## Technologies Used
- Python
- Apache Airflow
- PostgreSQL
- Pandas
- SQLAlchemy
- Azure VM for hosting

## Setup
1. Clone this repository
2. Set up required environment variables in a `.env` file:
   - dbname
   - user
   - password
   - host
   - port
3. Install required dependencies
4. Copy files to the appropriate locations on your server
5. Initialize and start Airflow

## Usage
The DAG is configured to run daily, extracting the previous day's BTC price data and storing it in the PostgreSQL database.

