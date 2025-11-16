from cassandra.cluster import Cluster
from faust import App
import os

# Налаштування Kafka
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')

# Налаштування Cassandra
CASSANDRA_HOSTS = ['127.0.0.1']
CASSANDRA_PORT = 9042
KEYSPACE = 'lab4_wind_energy'

# Створюємо Faust App
app = App(
    'lab4-wind-energy',
    broker=KAFKA_BROKER,
    store='memory://',
    value_serializer='json',
)


def get_cassandra_session():
    """Створює та повертає сесію Cassandra"""
    try:
        cluster = Cluster(CASSANDRA_HOSTS, port=CASSANDRA_PORT)
        session = cluster.connect()
        return cluster, session
    except Exception as e:
        print(f"❌ Помилка підключення до Cassandra: {e}")
        raise


def ensure_keyspace(session):
    """Створює keyspace, якщо він не існує"""
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH REPLICATION = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }};
    """)
    session.set_keyspace(KEYSPACE)

