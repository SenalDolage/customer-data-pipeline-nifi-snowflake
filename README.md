
This project is about a real-time data ingestion pipeline for handling customer data using **Apache NiFi**, **AWS S3**, and **Snowflake**, with a focus on implementing Slowly Changing Dimensions (SCD).

## Project Overview

The pipeline simulates a setup for loading customer data from a generated source into Snowflake. Key objectives include:

- Automating file ingestion using NiFi
- Moving data to a Snowflake staging table via Snowpipe
- Applying **SCD Type 1** logic to update customer records
- (todo) Extending to **SCD Type 2** to maintain change history

## Architecture

[![architecture-drawio.png](https://i.postimg.cc/SNQLXnJ3/architecture-drawio.png)](https://postimg.cc/DmD4HyQP)

## Components

### 🔹 Data Generation
- Uses Python with `faker` to generate synthetic customer data.
- Output is a `.csv` file saved into a watched directory.

### 🔹 Apache NiFi
- Monitors the dataset folder for new files using:
  - `ListFile`
  - `FetchFile`
  - `PutS3Object`
- Uploads files to an **AWS S3 bucket**.

### 🔹 Snowpipe + Snowflake
- **Snowpipe** auto-ingests uploaded `.csv` files from S3.
- Data is loaded into a **staging table** (`Customer_raw`).
- A **MERGE query** moves the data into the main `Customer` table using **SCD Type 1** logic.
- (Planned) An `INSERT` into `Customer_history` table will implement **SCD Type 2**.

