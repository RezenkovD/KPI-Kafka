#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Лабораторна робота №3: Оптимізація схем даних в Apache Cassandra
Варіант 2: Вітрові електростанції Запорізької області
"""

import json
import random
import time
import uuid
from datetime import datetime, timedelta, date
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra.policies import DCAwareRoundRobinPolicy

# --- CONFIGURATION ---
CASSANDRA_HOSTS = ['127.0.0.1']
KEYSPACE = 'wind_energy'
REPLICATION_FACTOR = 1
NUM_RECORDS_TO_GENERATE = 1_000_000  # Зменшено для швидшого тестування
NUM_DEVICES = 30
BATCH_SIZE = 100
BENCHMARK_ITERATIONS = 100


# --- DATA GENERATION ---
def get_random_device_id(i):
    return f"WIND_ZP_{i:03d}"


def generate_wind_record(device_index):
    return {
        "device_id": get_random_device_id(device_index),
        "timestamp": datetime.now().isoformat() + "Z",
        "power_output": round(random.uniform(0.0, 3000.0), 2),
        "efficiency": round(random.uniform(25.0, 45.0), 2),
        "wind_speed": round(random.uniform(3.0, 25.0), 2),
        "rotor_rpm": round(random.uniform(5.0, 30.0), 2),
    }


# --- CASSANDRA OPERATIONS ---

def setup_cassandra(session):
    print(f"🔧 Створюємо keyspace '{KEYSPACE}'...")
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH REPLICATION = {{ 'class': 'SimpleStrategy', 'replication_factor': {REPLICATION_FACTOR} }};
    """)
    session.set_keyspace(KEYSPACE)

    print("🔧 Створюємо таблиці для трьох схем...")
    session.execute("""
                    CREATE TABLE IF NOT EXISTS telemetry_simple
                    (
                        device_id
                        TEXT,
                        timestamp
                        TIMESTAMP,
                        power_output
                        DOUBLE,
                        efficiency
                        DOUBLE,
                        wind_speed
                        DOUBLE,
                        rotor_rpm
                        DOUBLE,
                        PRIMARY
                        KEY
                    (
                        device_id,
                        timestamp
                    )
                        ) WITH CLUSTERING ORDER BY (timestamp DESC);
                    """)
    session.execute("""
                    CREATE TABLE IF NOT EXISTS telemetry_hourly
                    (
                        device_id
                        TEXT,
                        bucket_hour
                        TIMESTAMP,
                        timestamp
                        TIMESTAMP,
                        power_output
                        DOUBLE,
                        efficiency
                        DOUBLE,
                        wind_speed
                        DOUBLE,
                        rotor_rpm
                        DOUBLE,
                        PRIMARY
                        KEY (
                    (
                        device_id,
                        bucket_hour
                    ), timestamp)
                        ) WITH CLUSTERING ORDER BY (timestamp DESC);
                    """)
    session.execute("""
                    CREATE TABLE IF NOT EXISTS telemetry_daily_raw
                    (
                        device_id
                        TEXT,
                        bucket_date
                        DATE,
                        timestamp
                        TIMESTAMP,
                        power_output
                        DOUBLE,
                        efficiency
                        DOUBLE,
                        wind_speed
                        DOUBLE,
                        rotor_rpm
                        DOUBLE,
                        PRIMARY
                        KEY (
                    (
                        device_id,
                        bucket_date
                    ), timestamp)
                        ) WITH CLUSTERING ORDER BY (timestamp DESC);
                    """)
    print("✅ Схеми успішно створено!")


def load_data(session):
    print(f"🚀 Починаємо генерацію та завантаження {NUM_RECORDS_TO_GENERATE} записів...")
    insert_simple = session.prepare(
        "INSERT INTO telemetry_simple (device_id, timestamp, power_output, efficiency, wind_speed, rotor_rpm) VALUES (?, ?, ?, ?, ?, ?)")
    insert_hourly = session.prepare(
        "INSERT INTO telemetry_hourly (device_id, bucket_hour, timestamp, power_output, efficiency, wind_speed, rotor_rpm) VALUES (?, ?, ?, ?, ?, ?, ?)")
    insert_daily = session.prepare(
        "INSERT INTO telemetry_daily_raw (device_id, bucket_date, timestamp, power_output, efficiency, wind_speed, rotor_rpm) VALUES (?, ?, ?, ?, ?, ?, ?)")

    records_inserted = 0
    start_time = time.time()
    batch_simple = BatchStatement()
    batch_hourly = BatchStatement()
    batch_daily = BatchStatement()

    for i in range(NUM_RECORDS_TO_GENERATE):
        rec = generate_wind_record((i % NUM_DEVICES) + 1)
        ts_datetime = datetime.fromisoformat(rec['timestamp'][:-1])
        bucket_hour = ts_datetime.replace(minute=0, second=0, microsecond=0)
        bucket_date = ts_datetime.date()

        batch_simple.add(insert_simple,
                         (rec['device_id'], ts_datetime, rec['power_output'], rec['efficiency'], rec['wind_speed'],
                          rec['rotor_rpm']))
        batch_hourly.add(insert_hourly,
                         (rec['device_id'], bucket_hour, ts_datetime, rec['power_output'], rec['efficiency'],
                          rec['wind_speed'], rec['rotor_rpm']))
        batch_daily.add(insert_daily,
                        (rec['device_id'], bucket_date, ts_datetime, rec['power_output'], rec['efficiency'],
                         rec['wind_speed'], rec['rotor_rpm']))

        if (i + 1) % BATCH_SIZE == 0:
            session.execute(batch_simple)
            session.execute(batch_hourly)
            session.execute(batch_daily)
            batch_simple.clear();
            batch_hourly.clear();
            batch_daily.clear()
            records_inserted += BATCH_SIZE
            print(f"  ... Завантажено {records_inserted}/{NUM_RECORDS_TO_GENERATE} записів", end='\r')

    if len(batch_simple) > 0:
        session.execute(batch_simple);
        session.execute(batch_hourly);
        session.execute(batch_daily)
        records_inserted += len(batch_simple)

    end_time = time.time()
    print(f"\n✅ Успішно завантажено {records_inserted} записів за {end_time - start_time:.2f} секунд.")


def run_benchmark(session, query, params, description):
    timings = []
    print(f"⏱️  Тестуємо: {description}...")
    prepared_query = session.prepare(query)
    for _ in range(BENCHMARK_ITERATIONS):
        start = time.perf_counter()
        session.execute(prepared_query, params)
        end = time.perf_counter()
        timings.append((end - start) * 1000)

    avg_latency = sum(timings) / len(timings)
    p95 = sorted(timings)[int(len(timings) * 0.95)]
    p99 = sorted(timings)[int(len(timings) * 0.99)]
    print(f"    -> Avg: {avg_latency:.2f} ms | p95: {p95:.2f} ms | p99: {p99:.2f} ms")


def perform_testing(session):
    print("\n" + "=" * 50 + "\n📊 ПОЧИНАЄМО ТЕСТУВАННЯ ПРОДУКТИВНОСТІ\n" + "=" * 50)

    device_id = get_random_device_id(random.randint(1, NUM_DEVICES))
    now = datetime.now()
    six_hours_ago = now - timedelta(hours=6)

    print("\n--- СХЕМА 1: Simple Wide Row ---")
    run_benchmark(session, "SELECT * FROM telemetry_simple WHERE device_id = ? LIMIT 100", (device_id,),
                  "Останні 100 записів")
    run_benchmark(session, "SELECT * FROM telemetry_simple WHERE device_id = ? AND timestamp >= ? AND timestamp <= ?",
                  (device_id, six_hours_ago, now), "Діапазон 6 годин")

    print("\n--- СХЕМА 2: Hourly Bucketing ---")
    bucket_hour = now.replace(minute=0, second=0, microsecond=0)
    run_benchmark(session, "SELECT * FROM telemetry_hourly WHERE device_id = ? AND bucket_hour = ? LIMIT 100",
                  (device_id, bucket_hour), "Останні 100 записів")

    print(f"⏱️  Тестуємо: Діапазон 6 годин (координація 6 запитів)...")
    timings_hourly_range = []
    prepared_hourly_range_query = session.prepare(
        "SELECT * FROM telemetry_hourly WHERE device_id = ? AND bucket_hour = ?")
    for _ in range(BENCHMARK_ITERATIONS):
        start = time.perf_counter()
        for h in range(7):
            bucket = (now - timedelta(hours=h)).replace(minute=0, second=0, microsecond=0)
            session.execute(prepared_hourly_range_query, (device_id, bucket))
        end = time.perf_counter()
        timings_hourly_range.append((end - start) * 1000)
    avg_latency = sum(timings_hourly_range) / len(timings_hourly_range)
    p95 = sorted(timings_hourly_range)[int(len(timings_hourly_range) * 0.95)]
    p99 = sorted(timings_hourly_range)[int(len(timings_hourly_range) * 0.99)]
    print(f"    -> Avg: {avg_latency:.2f} ms | p95: {p95:.2f} ms | p99: {p99:.2f} ms")

    print("\n--- СХЕМА 3: Daily Bucketing ---")
    bucket_date = now.date()
    run_benchmark(session, "SELECT * FROM telemetry_daily_raw WHERE device_id = ? AND bucket_date = ? LIMIT 100",
                  (device_id, bucket_date), "Останні 100 записів")
    run_benchmark(session,
                  "SELECT * FROM telemetry_daily_raw WHERE device_id = ? AND bucket_date = ? AND timestamp >= ? AND timestamp <= ?",
                  (device_id, bucket_date, six_hours_ago, now), "Діапазон 6 годин")

    print("\n--- ТЕСТУВАННЯ Materialized View ---")
    # !!! ВИПРАВЛЕННЯ ТУТ !!!
    session.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS high_wind_speed_view AS
        SELECT * FROM telemetry_hourly
        WHERE device_id IS NOT NULL AND bucket_hour IS NOT NULL AND timestamp IS NOT NULL AND wind_speed IS NOT NULL
        PRIMARY KEY ((device_id, bucket_hour), wind_speed, timestamp);
    """)
    print("...чекаємо, поки Materialized View побудується...")
    time.sleep(5)

    run_benchmark(session,
                  "SELECT * FROM telemetry_hourly WHERE device_id = ? AND bucket_hour = ? AND wind_speed > 20.0 ALLOW FILTERING",
                  (device_id, bucket_hour), "Фільтр > 20 м/с (ALLOW FILTERING)")
    run_benchmark(session,
                  "SELECT * FROM high_wind_speed_view WHERE device_id = ? AND bucket_hour = ? AND wind_speed > 20.0",
                  (device_id, bucket_hour), "Фільтр > 20 м/с (Materialized View)")

    print("\n" + "=" * 50 + "\n✅ ТЕСТУВАННЯ ЗАВЕРШЕНО\n" + "=" * 50)


def main():
    try:
        cluster = Cluster(CASSANDRA_HOSTS)
        session = cluster.connect()
        print("✅ Підключення до Cassandra успішне!")
    except Exception as e:
        print(f"❌ Помилка підключення до Cassandra: {e}")
        return

    setup_cassandra(session)
    print("🗑️ Очищуємо таблиці перед новим завантаженням...")
    session.execute("TRUNCATE telemetry_simple;")
    session.execute("TRUNCATE telemetry_hourly;")
    session.execute("TRUNCATE telemetry_daily_raw;")
    # Також видаляємо MV, якщо він існує, щоб уникнути проблем
    session.execute("DROP MATERIALIZED VIEW IF EXISTS high_wind_speed_view;")

    load_data(session)
    perform_testing(session)

    cluster.shutdown()
    print("🔌 З'єднання з Cassandra закрито.")

if __name__ == "__main__":
    main()