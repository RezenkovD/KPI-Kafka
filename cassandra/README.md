# Cassandra for Energy Monitoring: Wind Farm System 🌬️

This project is an implementation of a monitoring system for wind power plants in the Zaporizhzhia Oblast, completed as part of Laboratory Work №2 on NoSQL databases. The main goal is to demonstrate the principles of designing distributed data schemas in **Apache Cassandra** for handling time-series data from IoT devices.

The system models the collection of real-time data from 30 wind turbines, meteorological data, and generates aggregated analytical summaries.

---

## ✨ Technologies Used

* **Database**: Apache Cassandra
* **Programming Language**: Python 3
* **Containerization**: Docker
* **Python Driver**: `cassandra-driver`

---

## 🚀 Getting Started

Follow these steps to set up the environment and run the project locally.

### 1. Run Cassandra with Docker

First, pull the official Cassandra image and start a container.

```bash
# Pull the latest Cassandra image
docker pull cassandra
```

```bash
# Run the container in detached mode, mapping the port
docker run --name cassandra-lab -p 9042:9042 -d cassandra
```

Important Note: Cassandra can take 1-2 minutes to initialize. Before connecting, check that the service is ready:

```bash
# Check the node status. Repeat until the status is "UN" (Up/Normal)
docker exec -it cassandra-lab nodetool status
```


2. Set Up Python Environment

Create a virtual environment and install the required library.

```bash
# Create and activate a virtual environment (optional but recommended)
python -m venv venv
source venv/bin/activate
```

```bash
# Install the Cassandra driver
pip install cassandra-driver
```


3. Run the Script

Execute the main Python script to create the schema, generate data, and perform analysis.

```bash
python main.py
```

```
🗂️ Database Schema Design

The data model is built on the principle of denormalization, where each table is optimized for a specific query type3. The keyspace for this project is named zaporizhzhia_wind_farms.


Table 1: turbine_readings (Operational Data)

Purpose: Stores a continuous stream of sensor readings from each wind turbine4.


Partition Key: (turbine_id) - Groups all data for a single turbine in one partition for fast historical lookups5.


Clustering Key: (timestamp DESC) - Sorts data within the partition by time, from newest to oldest. This is critical for quickly retrieving the latest readings6.



Table 2: meteo_station_data (Weather Conditions)

Purpose: Stores weather data from regional meteorological stations7.


Partition Key: (station_id, date) - A composite key used to prevent partitions from becoming too large over time (a "hot partition") by breaking up data per day8.


Clustering Key: (timestamp ASC) - Orders readings chronologically within each day.

Table 3: daily_generation_summary (Daily Aggregates)

Purpose: Stores pre-calculated daily energy generation totals for each turbine, avoiding costly on-the-fly calculations9.


Partition Key: (turbine_id)
Clustering Key: (date DESC) - Allows for quick retrieval of recent daily summaries.

Table 4: wind_farm_analytics (Park-level Statistics)

Purpose: Stores aggregated statistics for an entire wind farm, enabling higher-level analytics10.


Partition Key: (farm_id) - Groups all daily stats for a single farm together.
Clustering Key: (date DESC)

📊 Sample Output

After running the main.py script, you should see the following output, confirming that the data was generated and analyzed successfully:
```

```
✅ Успішно підключено до Cassandra. Keyspace: zaporizhzhia_wind_farms
✅ Таблиці успішно створено (або вже існують).

⏳ Починаємо генерацію та вставку даних...
✅ Вставлено 2880 записів про показання турбін.

📊 Починаємо аналіз даних...
1. Загальна кількість записів у 'turbine_readings': 2880

2. Останні 3 показання для турбіни f50b38e4-f006-4a19-b6af-62f89e42968b:
   - Час: 2025-10-12 13:13:31.996000, Потужність: 4.77 МВт, Швидкість вітру: 17.73 м/с
   - Час: 2025-10-12 12:58:31.996000, Потужність: 2.18 МВт, Швидкість вітру: 14.13 м/с
   - Час: 2025-10-12 12:43:31.996000, Потужність: 3.38 МВт, Швидкість вітру: 17.67 м/с

3. Добова генерація для перших 5 турбін:
   - Турбіна: ed9b3a7e-dcbd-465b-8b0e-d2d4839495bf, Дата: 2025-10-11, Згенеровано: 68.92 МВт-год
   - Турбіна: 190b224e-01cd-460e-aab7-4ab4b291b999, Дата: 2025-10-11, Згенеровано: 89.26 МВт-год
   - Турбіна: 0bfc4c85-7739-47ef-8b41-c092c8d43a3e, Дата: 2025-10-11, Згенеровано: 53.51 МВт-год
   - Турбіна: 27aa5603-6552-4cfd-a552-86ccd8d38db8, Дата: 2025-10-11, Згенеровано: 79.75 МВт-год
   - Турбіна: 580308c7-06e8-41f7-8f26-12cbe9081eb8, Дата: 2025-10-11, Згенеровано: 63.95 МВт-год

📈 Аналіз завершено.

🔌 З'єднання з Cassandra закрито.
```

