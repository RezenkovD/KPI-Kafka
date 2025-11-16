from cassandra.cluster import Cluster
from shared_setup import KEYSPACE, CASSANDRA_HOSTS, CASSANDRA_PORT


def setup_cassandra_schema():
    """Створює keyspace та всі необхідні таблиці"""
    try:
        cluster = Cluster(CASSANDRA_HOSTS, port=CASSANDRA_PORT)
        session = cluster.connect()
        print("✅ Підключення до Cassandra успішне!")
    except Exception as e:
        print(f"❌ Помилка підключення до Cassandra: {e}")
        return

    # Створюємо keyspace
    print(f"🔧 Створюємо keyspace '{KEYSPACE}'...")
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH REPLICATION = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }};
    """)
    session.set_keyspace(KEYSPACE)

    # Створюємо таблицю для агрегацій ramp rate
    print("🔧 Створюємо таблицю 'ramp_rate_aggregates'...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS ramp_rate_aggregates (
            device_id TEXT,
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            avg_power DOUBLE,
            ramp_rate DOUBLE,
            PRIMARY KEY (device_id, window_start)
        ) WITH CLUSTERING ORDER BY (window_start DESC);
    """)

    # Створюємо таблицю для статусу турбін
    print("🔧 Створюємо таблицю 'turbine_status'...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS turbine_status (
            device_id TEXT PRIMARY KEY,
            status TEXT,
            last_updated TIMESTAMP
        );
    """)

    # Створюємо таблицю для журналу Saga транзакцій
    print("🔧 Створюємо таблицю 'saga_log'...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS saga_log (
            saga_id TEXT,
            timestamp TIMESTAMP,
            device_id TEXT,
            status TEXT,
            step TEXT,
            details TEXT,
            PRIMARY KEY (saga_id, timestamp)
        ) WITH CLUSTERING ORDER BY (timestamp DESC);
    """)

    print("✅ Схема Cassandra успішно створена!")
    cluster.shutdown()


if __name__ == "__main__":
    setup_cassandra_schema()

