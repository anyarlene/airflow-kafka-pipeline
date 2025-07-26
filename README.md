# Airflow-Kafka-Pipeline

A simple demonstration project that uses Apache Airflow to orchestrate data pipelines and Kafka to stream data for real-time processing.

## Components

- **Airflow DAGs**: For orchestrating ETL tasks and managing pipeline scheduling.
- **Kafka**: Used for streaming and simulating real-time data generation and processing.
- **Python Scripts**:
  - `ETL_toll_data.py`: Performs ETL on toll data.
  - `get_toll_data.py`: Fetches toll data for processing.
  - `toll_traffic_generator.py`: Simulates toll traffic data and produces messages to Kafka.
  - `streaming_data_reader.py`: Consumes and processes streaming data from Kafka.

## Getting Started

1. **Install requirements**  
   - Python 3.9  
   - Apache Airflow  
   - Apache Kafka 2.12-2.8.0

2. **Set up Kafka**  
   Extract and run Kafka using the provided binaries in `kafka_2.12-2.8.0`.

3. **Set up Airflow**  
   Configure your Airflow home and initialize the database.

4. **Run the ETL Pipeline**  
- Start Airflow webserver and scheduler.
- Trigger the DAGs as needed.

5. **Streaming Simulation**  
- Use `toll_traffic_generator.py` to send simulated data to Kafka.
- Use `streaming_data_reader.py` to consume the data.

## Author

Any-Arlene Niyubahwe

---

**Note:**  
This project is for educational/demo purposes and may require adjustments for use in production environments.
