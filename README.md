# weather-data-collection
=======
# Weather Data Collection with Airflow

## Project Overview
This project uses **Apache Airflow** to collect weather data for Minsk every hour. The data is fetched from the OpenWeatherMap API and saved as `.parquet` files for efficient storage and analysis.

## Features
- Fetches weather data (temperature, wind, etc.) for Minsk.
- Saves data in `.parquet` format for efficient storage and processing.
- Runs every hour at the start of the hour (`0 * * * *`).
- Prevents fetching historical data by setting `catchup=False`.

## Prerequisites
1. **Docker** and **Docker Compose** installed.
2. An OpenWeatherMap API key stored in a `.env` file.

## Project Structure
```
project/
├── dags/
│   └── weather_data_collection.py  # Airflow DAG definition
├── .env                            # Environment variables (API key)
├── docker-compose.yaml             # Docker Compose configuration
└── parquet_files/                  # Directory for storing .parquet files
```

## Setup Instructions
1. Clone the repository and navigate to the project directory:
   ```bash
   git clone <repository-url>
   cd project
   ```

2. Create a `.env` file in the project root with the following content:
   ```
   OPENWEATHERMAP_API_KEY=your_api_key_here
   ```

3. Start the Airflow services using Docker Compose:
   ```bash
   docker-compose up -d
   ```

4. Access the Airflow web interface at [http://localhost:8080](http://localhost:8080) and enable the `weather_data_collection` DAG.

## How It Works
- The DAG fetches weather data from the OpenWeatherMap API using the `fetch_weather_data` Python function.
- The `execution_date` ensures that the correct datetime is used for each run.
- Data is saved in `.parquet` files under the `parquet_files/` directory.

## Notes
- Ensure the `.env` file is correctly configured with your OpenWeatherMap API key.
- The `parquet_files/` directory is mounted to the Airflow container via `docker-compose.yaml`.

## License
This project is licensed under the MIT License.
