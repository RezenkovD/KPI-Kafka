import uuid
from datetime import datetime, timedelta
from faust import Topic, Stream, Table
from faust.windows import HoppingWindow
from shared_setup import app, get_cassandra_session, ensure_keyspace
from models import TurbineTelemetry, CurtailmentRequest, CancelCurtailment

# Топіки
telemetry_topic: Topic = app.topic('turbine_telemetry', value_type=TurbineTelemetry)
curtailment_requests_topic: Topic = app.topic('curtailment_requests', value_type=CurtailmentRequest)
cancel_curtailment_topic: Topic = app.topic('cancel_curtailment', value_type=CancelCurtailment)

# Таблиця для зберігання попередніх середніх значень потужності
previous_avg_power_table: Table = app.Table(
    'previous_avg_power',
    default=lambda: {'avg_power': None, 'window_end': None}
)

# Ініціалізуємо Cassandra сесію
_cassandra_cluster = None
_cassandra_session = None


def get_cassandra():
    """Отримує або створює Cassandra сесію"""
    global _cassandra_cluster, _cassandra_session
    if _cassandra_session is None:
        _cassandra_cluster, _cassandra_session = get_cassandra_session()
        ensure_keyspace(_cassandra_session)
    return _cassandra_session


# Підготовлені запити для Cassandra
def prepare_cassandra_statements(session):
    """Підготовлює prepared statements для швидшої роботи"""
    insert_ramp_rate = session.prepare("""
        INSERT INTO ramp_rate_aggregates 
        (device_id, window_start, window_end, avg_power, ramp_rate)
        VALUES (?, ?, ?, ?, ?)
    """)
    
    insert_saga_log = session.prepare("""
        INSERT INTO saga_log 
        (saga_id, timestamp, device_id, status, step, details)
        VALUES (?, ?, ?, ?, ?, ?)
    """)
    
    update_turbine_status = session.prepare("""
        INSERT INTO turbine_status 
        (device_id, status, last_updated)
        VALUES (?, ?, ?)
    """)
    
    get_turbine_status = session.prepare("""
        SELECT status FROM turbine_status WHERE device_id = ?
    """)
    
    return {
        'insert_ramp_rate': insert_ramp_rate,
        'insert_saga_log': insert_saga_log,
        'update_turbine_status': update_turbine_status,
        'get_turbine_status': get_turbine_status,
    }


# Ініціалізуємо prepared statements
cassandra_statements = None


@app.on_worker_init
async def init_cassandra():
    """Ініціалізує Cassandra при старті worker"""
    global cassandra_statements
    session = get_cassandra()
    cassandra_statements = prepare_cassandra_statements(session)
    print("✅ Cassandra statements підготовлено")


# Таблиця для зберігання подій у вікнах (10 хвилин, крок 2 хвилини)
# Використовуємо Table з Hopping Window
power_aggregates_table: Table = app.Table(
    'power_aggregates',
    default=lambda: {'events': [], 'last_processed': None},
    window=HoppingWindow(size=timedelta(minutes=10), step=timedelta(minutes=2))
)


@app.agent(telemetry_topic)
async def process_telemetry_with_hopping_windows(stream: Stream):
    """
    Агент 1: Обробка телеметрії з використанням Hopping Windows
    Розмір вікна: 10 хвилин, крок: 2 хвилини
    """
    async for event in stream:
        device_id = event.device_id
        
        # Додаємо подію до таблиці для подальшої обробки
        current = power_aggregates_table.get(device_id) or {'events': [], 'last_processed': None}
        current['events'].append({
            'power_output': event.power_output,
            'timestamp': datetime.utcnow()
        })
        power_aggregates_table[device_id] = current


# Періодична задача для обробки вікон (кожні 2 хвилини)
@app.timer(interval=120.0)  # 2 хвилини в секундах
async def process_windows():
    """Обробляє завершені вікна та розраховує ramp rate"""
    session = get_cassandra()
    current_time = datetime.utcnow()
    window_end = current_time
    window_start = window_end - timedelta(minutes=10)
    
    # Обробляємо всі device_id
    for device_id in list(power_aggregates_table.keys()):
        device_data = power_aggregates_table.get(device_id)
        if not device_data or not device_data.get('events'):
            continue
        
        # Фільтруємо події, що потрапили в поточне вікно
        events_in_window = [
            e for e in device_data['events']
            if window_start <= e['timestamp'] <= window_end
        ]
        
        if not events_in_window:
            continue
        
        # Обчислюємо середню потужність
        total_power = sum(e['power_output'] for e in events_in_window)
        avg_power = total_power / len(events_in_window)
        
        # Отримуємо попереднє середнє значення
        prev_data = previous_avg_power_table.get(device_id) or {'avg_power': None, 'window_end': None}
        prev_avg = prev_data.get('avg_power')
        
        # Розраховуємо ramp rate
        if prev_avg is not None and prev_data.get('window_end'):
            # Розраховуємо час між вікнами (2 хвилини - крок вікна)
            time_diff_minutes = 2.0
            power_diff_mw = (avg_power - prev_avg) / 1000.0  # Конвертація кВт -> МВт
            ramp_rate = power_diff_mw / time_diff_minutes
        else:
            ramp_rate = 0.0
        
        # Оновлюємо попереднє значення
        previous_avg_power_table[device_id] = {
            'avg_power': avg_power,
            'window_end': window_end
        }
        
        # Зберігаємо в Cassandra (тільки якщо є попереднє значення для розрахунку ramp rate)
        if prev_avg is not None:
            try:
                session.execute(
                    cassandra_statements['insert_ramp_rate'],
                    (device_id, window_start, window_end, avg_power, ramp_rate)
                )
                print(f"📊 Збережено ramp rate: {device_id} | "
                      f"Window: {window_start} - {window_end} | "
                      f"Avg Power: {avg_power:.2f} kW | "
                      f"Ramp Rate: {ramp_rate:.4f} MW/min")
            except Exception as e:
                print(f"❌ Помилка збереження ramp rate: {e}")
        
        # Очищаємо старі події (старіші за 15 хвилин)
        cutoff_time = current_time - timedelta(minutes=15)
        device_data['events'] = [
            e for e in device_data['events']
            if e['timestamp'] > cutoff_time
        ]
        power_aggregates_table[device_id] = device_data


@app.agent(curtailment_requests_topic)
async def process_curtailment_saga(stream: Stream):
    """
    Агент 2 (частина 1): Обробка curtailment requests з використанням Saga Pattern
    """
    session = get_cassandra()
    
    async for request in stream:
        saga_id = str(uuid.uuid4())
        timestamp = datetime.utcnow()
        
        try:
            # Крок 1: Записуємо в saga_log (status: STARTED)
            session.execute(
                cassandra_statements['insert_saga_log'],
                (saga_id, timestamp, request.device_id, 'STARTED', 'step_1', 
                 f"Saga started for device {request.device_id}, reason: {request.reason}")
            )
            print(f"📝 Saga STARTED: {saga_id} | Device: {request.device_id}")
            
            # Крок 2: Оновлюємо turbine_status (status: CURTAILED)
            session.execute(
                cassandra_statements['update_turbine_status'],
                (request.device_id, 'CURTAILED', timestamp)
            )
            print(f"🔄 Status updated: {request.device_id} -> CURTAILED")
            
            # Крок 3: Записуємо в saga_log (status: COMPLETED)
            session.execute(
                cassandra_statements['insert_saga_log'],
                (saga_id, timestamp + timedelta(seconds=1), request.device_id, 'COMPLETED', 'step_3',
                 f"Saga completed for device {request.device_id}")
            )
            print(f"✅ Saga COMPLETED: {saga_id} | Device: {request.device_id}")
            
        except Exception as e:
            print(f"❌ Помилка обробки curtailment saga: {e}")


@app.agent(cancel_curtailment_topic)
async def process_curtailment_compensation(stream: Stream):
    """
    Агент 2 (частина 2): Обробка компенсації (скасування curtailment)
    """
    session = get_cassandra()
    
    async for cancel_request in stream:
        saga_id = str(uuid.uuid4())
        timestamp = datetime.utcnow()
        
        try:
            # Крок 1: Записуємо в saga_log (status: COMPENSATION_STARTED)
            session.execute(
                cassandra_statements['insert_saga_log'],
                (saga_id, timestamp, cancel_request.device_id, 'COMPENSATION_STARTED', 'compensation_step_1',
                 f"Compensation started for device {cancel_request.device_id}, reason: {cancel_request.reason or 'N/A'}")
            )
            print(f"📝 Compensation STARTED: {saga_id} | Device: {cancel_request.device_id}")
            
            # Крок 2: Виконуємо компенсуючу дію - оновлюємо turbine_status (status: ACTIVE)
            session.execute(
                cassandra_statements['update_turbine_status'],
                (cancel_request.device_id, 'ACTIVE', timestamp)
            )
            print(f"🔄 Status updated: {cancel_request.device_id} -> ACTIVE")
            
            # Крок 3: Записуємо в saga_log (status: COMPENSATION_COMPLETED)
            session.execute(
                cassandra_statements['insert_saga_log'],
                (saga_id, timestamp + timedelta(seconds=1), cancel_request.device_id, 'COMPENSATION_COMPLETED', 
                 'compensation_step_3',
                 f"Compensation completed for device {cancel_request.device_id}")
            )
            print(f"✅ Compensation COMPLETED: {saga_id} | Device: {cancel_request.device_id}")
            
        except Exception as e:
            print(f"❌ Помилка обробки compensation: {e}")


if __name__ == "__main__":
    app.main()

