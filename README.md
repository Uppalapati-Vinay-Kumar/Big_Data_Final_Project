# Big Data Engineering and Science: From Raw Data to Insights

## Overview
This project demonstrates a complete data engineering and data science pipeline, taking raw CSV files and transforming them into insightful visualizations and machine learning models. It's an end-to-end workflow perfect for those interested in data engineering, ETL pipelines, visualizations and machine learning.

## Features

1. **Data Streaming with Kafka**:
   - Streaming data from CSV files to a Kafka stream.
   - Using a Kafka consumer script to receive and save the streamed data into a new CSV file.

2. **Bronze-Silver-Gold Data Architecture**:
   - **Bronze Layer**: Ingest raw data from CSV File into a bronze table.
   - **Silver Layer**: Clean, transform, and process bronze layer data into a silver table.
   - **Gold Layer**: Aggregate and filter data to generate insights in the gold table.

3. **Data Visualization**:
   - Create impactful visualizations using `matplotlib` from data in the golden layer tables.

4. **Machine Learning**:
   - Leverage cleaned silver layer data to train classification and regression models.

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/Uppalapati-Vinay-Kumar/Big_Data_Final_Project.git
   cd Big_Data_Final_Project
   ```

2. Install dependencies:
   ```bash
   poetry update
   ```

3. Set up Kafka (if not already installed):
   - [Download Kafka](https://kafka.apache.org/downloads).
   - Follow the [Kafka Quickstart Guide](https://kafka.apache.org/quickstart) to set up your broker and topic.

## Usage

1. **Run the Kafka Producer**:
   ```bash
   poetry run python csv_to_kafka_producer.py
   ```

2. **Run the Kafka Consumer**:
   ```bash
   poetry run python kafka_to_csv_consumer.py
   ```

3.  **Process the Data**:
   - Bronze, Silver, and Gold layer scripts can be run sequentially to clean, transform, and aggregate the data.

4. **Generate Visualizations**:
   ```bash
   poetry run python visualize_gold_data_1.py
   poetry run python visualize_gold_data_2.py
   poetry run python visualize_gold_data_3.py

   ```

5. **Train Machine Learning classification Model**:
   ```bash
   poetry run python track_genre_prediction_model.py
   ```

6. **Train Machine Learning regression Model**:
   ```bash
   poetry run python track_popularity_prediction_model.py
   ```

## Technologies Used

- **Kafka**: For data streaming.
- **Pandas**: Data manipulation and cleaning.
- **Matplotlib**: Data visualization.
- **Scikit-learn**: Machine learning models.
- **Python**: Core programming language.
- **Cassandra (DataStax Astra)**: Big Data Platform for scalable data storage and management.

## Contributing
Contributions are welcome! Please fork the repository and submit a pull request.

Enjoy the world of Big Data Engineering and Science, transforming raw data into actionable insights through data pipelines and machine learning.