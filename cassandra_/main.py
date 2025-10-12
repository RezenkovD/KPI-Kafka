import uuid
import random
from datetime import datetime, timedelta
from cassandra_.cluster import Cluster
from cassandra_.query import BatchStatement

# --- Налаштування підключення ---
KEYSPACE = "zaporizhzhia_wind_farms"


def connect_to_cassandra():
    """Підключається до кластера Cassandra та повертає об'єкт сесії."""
    try:
        cluster = Cluster(['127.0.0.1'], port=9042)
        session = cluster.connect()
        # Створюємо keyspace, якщо він не існує
        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
            WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '1' }}
        """)
        session.set_keyspace(KEYSPACE)
        print(f"✅ Успішно підключено до Cassandra. Keyspace: {KEYSPACE}")
        return cluster, session
    except Exception as e:
        print(f"❌ Помилка підключення до Cassandra: {e}")
        return None, None


def create_tables(session):
    """Створює таблиці, якщо вони не існують."""
    try:
        # Таблиця 1: Оперативні дані
        session.execute("""
                        CREATE TABLE IF NOT EXISTS turbine_readings
                        (
                            turbine_id
                            UUID,
                            timestamp
                            TIMESTAMP,
                            wind_speed
                            DECIMAL,
                            rotor_speed
                            DECIMAL,
                            power_output
                            DECIMAL,
                            vibration_level
                            DECIMAL,
                            PRIMARY
                            KEY
                        (
                            turbine_id,
                            timestamp
                        )
                            ) WITH CLUSTERING ORDER BY (timestamp DESC);
                        """)
        # Таблиця 2: Метеоумови
        session.execute("""
                        CREATE TABLE IF NOT EXISTS meteo_station_data
                        (
                            station_id
                            UUID,
                            date
                            DATE,
                            timestamp
                            TIMESTAMP,
                            air_temperature
                            DECIMAL,
                            wind_direction
                            INT,
                            PRIMARY
                            KEY (
                        (
                            station_id,
                            date
                        ), timestamp)
                            );
                        """)
        # Таблиця 3: Добові підсумки
        session.execute("""
                        CREATE TABLE IF NOT EXISTS daily_generation_summary
                        (
                            turbine_id
                            UUID,
                            date
                            DATE,
                            total_power_generated
                            DECIMAL,
                            avg_wind_speed
                            DECIMAL,
                            PRIMARY
                            KEY
                        (
                            turbine_id,
                            date
                        )
                            ) WITH CLUSTERING ORDER BY (date DESC);
                        """)
        # Таблиця 4: Загальна статистика по парках
        session.execute("""
                        CREATE TABLE IF NOT EXISTS wind_farm_analytics
                        (
                            farm_id
                            UUID,
                            date
                            DATE,
                            total_farm_power
                            DECIMAL,
                            peak_power_time
                            TIMESTAMP,
                            PRIMARY
                            KEY
                        (
                            farm_id,
                            date
                        )
                            ) WITH CLUSTERING ORDER BY (date DESC);
                        """)
        print("✅ Таблиці успішно створено (або вже існують).")
    except Exception as e:
        print(f"❌ Помилка створення таблиць: {e}")


def generate_and_insert_data(session):
    """Генерує та вставляє тестові дані для 30 турбін."""
    NUM_TURBINES = 30
    NUM_METEO_STATIONS = 5
    NUM_FARMS = 3

    turbine_ids = [uuid.uuid4() for _ in range(NUM_TURBINES)]
    station_ids = [uuid.uuid4() for _ in range(NUM_METEO_STATIONS)]
    farm_ids = [uuid.uuid4() for _ in range(NUM_FARMS)]

    print("\n⏳ Починаємо генерацію та вставку даних...")

    # Підготовка запитів для пакетної вставки
    insert_reading_stmt = session.prepare(
        "INSERT INTO turbine_readings (turbine_id, timestamp, wind_speed, rotor_speed, power_output, vibration_level) VALUES (?, ?, ?, ?, ?, ?)")
    insert_meteo_stmt = session.prepare(
        "INSERT INTO meteo_station_data (station_id, date, timestamp, air_temperature, wind_direction) VALUES (?, ?, ?, ?, ?)")

    start_time = datetime.now() - timedelta(days=1)
    records_count = 0

    for i in range(24 * 4):  # Дані кожні 15 хвилин протягом доби
        current_time = start_time + timedelta(minutes=15 * i)
        current_date = current_time.date()

        # Використовуємо BatchStatement для групової вставки
        batch = BatchStatement()
        for turbine_id in turbine_ids:
            batch.add(insert_reading_stmt, (
                turbine_id,
                current_time,
                random.uniform(5.0, 25.0),  # швидкість вітру
                random.uniform(10.0, 20.0),  # оберти ротора
                random.uniform(1.5, 5.0),  # потужність, МВт
                random.uniform(0.01, 0.5)  # вібрація
            ))
            records_count += 1

        for station_id in station_ids:
            batch.add(insert_meteo_stmt, (
                station_id,
                current_date,
                current_time,
                random.uniform(10.0, 25.0),  # температура
                random.randint(0, 360)  # напрям вітру
            ))

        session.execute(batch)

    # Вставка агрегованих даних (симуляція)
    for turbine_id in turbine_ids:
        session.execute(
            "INSERT INTO daily_generation_summary (turbine_id, date, total_power_generated, avg_wind_speed) VALUES (%s, %s, %s, %s)",
            (turbine_id, start_time.date(), random.uniform(50, 100), random.uniform(10, 20))
        )
    for farm_id in farm_ids:
        session.execute(
            "INSERT INTO wind_farm_analytics (farm_id, date, total_farm_power, peak_power_time) VALUES (%s, %s, %s, %s)",
            (farm_id, start_time.date(), random.uniform(500, 1000),
             start_time + timedelta(hours=random.randint(10, 18)))
        )

    print(f"✅ Вставлено {records_count} записів про показання турбін.")
    return turbine_ids  # Повертаємо ID для аналізу


def analyze_data(session, turbine_ids):
    """Виконує базовий аналіз даних."""
    print("\n📊 Починаємо аналіз даних...")

    # 1. Загальна кількість записів
    total_rows = session.execute("SELECT COUNT(*) FROM turbine_readings").one()[0]
    print(f"1. Загальна кількість записів у 'turbine_readings': {total_rows}")

    # 2. Останні 3 показання для першої турбіни
    first_turbine_id = turbine_ids[0]
    rows = session.execute(f"SELECT * FROM turbine_readings WHERE turbine_id = {first_turbine_id} LIMIT 3")
    print(f"\n2. Останні 3 показання для турбіни {first_turbine_id}:")
    for row in rows:
        print(
            f"   - Час: {row.timestamp}, Потужність: {row.power_output:.2f} МВт, Швидкість вітру: {row.wind_speed:.2f} м/с")

    # 3. Добовий підсумок для перших 5 турбін
    rows = session.execute("SELECT * FROM daily_generation_summary LIMIT 5")
    print("\n3. Добова генерація для перших 5 турбін:")
    for row in rows:
        print(f"   - Турбіна: {row.turbine_id}, Дата: {row.date}, Згенеровано: {row.total_power_generated:.2f} МВт-год")

    print("\n📈 Аналіз завершено.")


def main():
    """Головна функція."""
    cluster, session = connect_to_cassandra()
    if not session:
        return

    create_tables(session)
    turbine_ids = generate_and_insert_data(session)
    analyze_data(session, turbine_ids)

    # Закриваємо з'єднання
    cluster.shutdown()
    print("\n🔌 З'єднання з Cassandra закрито.")


if __name__ == "__main__":
    main()