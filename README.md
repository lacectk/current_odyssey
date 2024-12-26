# Dagster Data Pipeline for Wave Data Analytics

This project implements a Dagster pipeline to process and manage data related to wave analytics using PostgreSQL as the backend database. The pipeline fetches, processes, and stores buoy wave data, providing high-quality and structured datasets for further analysis.

## Project Features
- `Data Ingestion` : Fetches raw wave data from the NDBC API.
- `Data Storage` : Stores processed wave data in PostgreSQL tables with partitioning for efficient querying.
- `Dagster Integration` : Built using Dagster for asset management, scheduling, and metadata tracking.
- `Environment Variables`: Supports secure configuration using `.env` files.
- `Email Notifications`: Sends alerts for pipeline errors or issues.


## Tech Stack
- `Python` (3.11)
- `Dagster`: For orchestrating the data pipeline.
- `SQLAlchemy`: For database interactions.
- `Pandas`: For data processing and transformations.
- `PostgreSQL`: As the backend database.
- `Pydantic`: For structured configuration management.
- `dotenv`: For environment variable management.

### Project Structure
TODO

##  Pipeline Details
Asset: raw_buoy_data \
Description: Fetches, processes, and stores wave data from NDBC stations. \
Metadata: WVHT, DPD, APD, MWD \
Source: NDBC API \
Cron Schedule: 0 2 * * * (runs daily at 2 AM). \
Retry Policy: Retries 3 times \
Output: Pandas DataFrame stored in PostgreSQL. \
Resource: PostgresIOManager \
Handles reading and writing data to PostgreSQL. Ensures data partitioning for efficient queries and supports schema validation.