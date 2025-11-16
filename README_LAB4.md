# Лабораторна робота №4: Потокова обробка з Faust та Saga Pattern

## ВАРІАНТ 2: Вітрові електростанції Запорізької області

### Об'єкт дослідження

- **Кількість**: 150 вітрових турбін (по 2.5 МВт кожна)
- **Частота генерації даних**: Кожні 5 секунд
- **Throughput**: ~30 msg/sec
- **Дані телеметрії**:
  - `power_output` - вихідна потужність (кВт)
  - `wind_speed` - швидкість вітру (м/с)
  - `wind_direction` - напрямок вітру (градуси)
  - `blade_pitch` - кут лопатей (градуси)
  - `vibration` - рівень вібрації
  - `temperature_generator` - температура генератора (°C)
  - `temperature_gearbox` - температура редуктора (°C)

### Підваріант Б: Hopping Windows (10 хв / 2 хв крок) + Saga Pattern

## Опис проєкту

Цей проєкт реалізує систему потокової обробки телеметрії вітрових турбін з використанням:
- **Faust** для потокової обробки даних
- **Kafka** для обміну повідомленнями
- **Cassandra** для зберігання даних
- **Hopping Windows** для ковзних вікон (10 хвилин, крок 2 хвилини)
- **Saga Pattern** для транзакційної обробки curtailment операцій

## Структура проєкту

```
.
├── scripts/
│   ├── models.py                 # Faust Record моделі
│   ├── shared_setup.py           # Ініціалізація Faust App та Cassandra
│   ├── setup_cassandra.py        # Скрипт створення Cassandra схеми
│   ├── producer.py                # Faust Producer для генерації телеметрії
│   ├── stream_processor.py       # Faust Stream Processor з агентами
│   └── test_saga.py              # Тестовий скрипт для Saga Pattern
├── requirements.txt              # Залежності Python
└── README_LAB4.md                # Документація (цей файл)
```

## Встановлення

1. Встановіть залежності:
```bash
pip install -r requirements.txt
```

2. Переконайтеся, що запущені:
   - Kafka (localhost:9092)
   - Cassandra (localhost:9042)

## Налаштування Cassandra

Створіть keyspace та таблиці:
```bash
cd scripts
python setup_cassandra.py
```

Або з кореневої директорії:
```bash
python scripts/setup_cassandra.py
```

Це створить:
- Keyspace: `lab4_wind_energy`
- Таблиці:
  - `ramp_rate_aggregates` - для збереження агрегацій ramp rate
  - `turbine_status` - для відстеження статусу турбін
  - `saga_log` - для журналу Saga транзакцій

## Запуск

### 1. Запуск Producer

Генерує телеметрію для 150 турбін кожні 5 секунд та відправляє в топік `turbine_telemetry`:

```bash
cd scripts
faust -A producer worker -l info
```

Або з кореневої директорії:
```bash
faust -A scripts.producer worker -l info
```

### 2. Запуск Stream Processor

Обробляє події з двома агентами:
- **Агент 1**: Hopping Windows для розрахунку ramp rate
- **Агент 2**: Saga Pattern для обробки curtailment requests та compensation

```bash
cd scripts
faust -A stream_processor worker -l info
```

Або з кореневої директорії:
```bash
faust -A scripts.stream_processor worker -l info
```

### 3. Тестування Saga Pattern

Відправляє тестові повідомлення для перевірки Saga:

```bash
cd scripts
faust -A test_saga worker -l info
```

Або з кореневої директорії:
```bash
faust -A scripts.test_saga worker -l info
```

## Топіки Kafka

- `turbine_telemetry` - телеметрія турбін
- `curtailment_requests` - запити на обмеження потужності
- `cancel_curtailment` - запити на скасування обмеження

## Завдання реалізації

### 1. Ковзні вікна для розрахунку швидкості зростання (МВт/хв)

**Агент 1: Hopping Windows**

Реалізує ковзне вікно з наступними параметрами:
- **Розмір вікна**: 10 хвилин
- **Крок (hop)**: 2 хвилини
- **Функція**: Розраховує середню потужність та ramp rate (швидкість зростання потужності, МВт/хв)

**Алгоритм розрахунку ramp rate**:
```
ramp_rate = (avg_power_current - avg_power_previous) / time_diff_minutes
```
де:
- `avg_power_current` - середня потужність поточного вікна (кВт)
- `avg_power_previous` - середня потужність попереднього вікна (кВт)
- `time_diff_minutes` - різниця часу між вікнами (2 хвилини)
- Результат конвертується з кВт в МВт: `ramp_rate_MW/min = ramp_rate_kW / 1000 / 2`

Результати зберігаються в таблиці `ramp_rate_aggregates`.

### 2. Розрахунок ramp rate та прогноз на наступне вікно

Система розраховує:
- **Поточний ramp rate**: на основі різниці між поточним та попереднім вікном
- **Прогнозований ramp rate**: екстраполяція тренду для наступного вікна
- **Історія змін**: зберігання історії ramp rate для аналізу трендів

### 3. State з історією для виявлення wake effects між турбінами

**Wake Effects** - це явище, коли одна турбіна створює турбулентність, що впливає на продуктивність інших турбін, розташованих за вітром.

Система зберігає:
- **Історію потужності** для кожної турбіни
- **Просторову інформацію** (можна додати координати турбін)
- **Кореляцію між турбінами** для виявлення wake effects

Це дозволяє:
- Виявляти турбіни, що впливають одна на одну
- Оптимізувати розташування та налаштування турбін
- Прогнозувати втрати продуктивності через wake effects

### 4. 2-крокова Saga для координації curtailment

**Агент 2: Saga Pattern**

Реалізує розподілену транзакцію для обмеження потужності турбін (curtailment) з можливістю компенсації.

#### Curtailment Request Saga

**Топік**: `curtailment_requests`

**2-крокова транзакція**:

**Крок 1: Запис команди curtailment**
- Генерується унікальний `saga_id`
- Записується в `saga_log` зі статусом `STARTED`
- Зберігається інформація про причину обмеження (наприклад, "Grid overload")

**Крок 2: Оновлення статусу турбін**
- Оновлюється `turbine_status` для вказаної турбіни
- Статус змінюється на `CURTAILED`
- Записується `last_updated` timestamp
- Записується в `saga_log` зі статусом `COMPLETED`

**Приклад повідомлення**:
```json
{
  "device_id": "WIND_ZP_001",
  "reason": "Grid overload - reducing power output"
}
```

#### Compensation при відміні curtailment

**Топік**: `cancel_curtailment`

**Compensation Saga**:

**Крок 1: COMPENSATION_STARTED**
- Генерується `saga_id` для компенсації
- Записується в `saga_log` зі статусом `COMPENSATION_STARTED`
- Зберігається причина скасування

**Крок 2: Виконання компенсуючої дії**
- Оновлюється `turbine_status` для вказаної турбіни
- Статус змінюється на `ACTIVE` (повернення до нормальної роботи)
- Записується `last_updated` timestamp

**Крок 3: COMPENSATION_COMPLETED**
- Записується в `saga_log` зі статусом `COMPENSATION_COMPLETED`
- Транзакція компенсації завершена

**Приклад повідомлення**:
```json
{
  "device_id": "WIND_ZP_001",
  "reason": "Grid stabilized - resuming normal operation"
}
```

### 5. Saga log для audit операторів мережі

**Таблиця**: `saga_log`

**Призначення**: Повний журнал всіх операцій curtailment для аудиту та відстеження дій операторів мережі.

**Структура запису**:
- `saga_id` - унікальний ID транзакції
- `timestamp` - час виконання кроку
- `device_id` - ID турбіни
- `status` - статус кроку (STARTED, COMPLETED, COMPENSATION_STARTED, COMPENSATION_COMPLETED)
- `step` - номер/назва кроку (step_1, step_2, compensation_step_1, тощо)
- `details` - детальна інформація про операцію

**Переваги**:
- **Аудит**: Повна історія всіх операцій curtailment
- **Відстеження**: Можливість відстежити хто, коли та чому виконав операцію
- **Відновлення**: При збоях можна відновити стан системи
- **Аналітика**: Аналіз частоти та причин curtailment операцій

## Моделі даних

### TurbineTelemetry
- `device_id`: ID турбіни
- `timestamp`: Часова мітка
- `power_output`: Потужність (кВт)
- `wind_speed`: Швидкість вітру (м/с)
- `wind_direction`: Напрямок вітру (градуси)
- `blade_pitch`: Кут лопатей (градуси)
- `vibration`: Вібрація
- `temperature_generator`: Температура генератора (°C)
- `temperature_gearbox`: Температура редуктора (°C)

### CurtailmentRequest
- `device_id`: ID турбіни
- `reason`: Причина обмеження

### CancelCurtailment
- `device_id`: ID турбіни
- `reason`: Причина скасування (опціонально)

## Перевірка результатів

### Перевірка ramp_rate_aggregates

```python
from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('lab4_wind_energy')

rows = session.execute("SELECT * FROM ramp_rate_aggregates LIMIT 10")
for row in rows:
    print(f"Device: {row.device_id}, Avg Power: {row.avg_power:.2f} kW, Ramp Rate: {row.ramp_rate:.4f} MW/min")
```

### Перевірка turbine_status

```python
rows = session.execute("SELECT * FROM turbine_status")
for row in rows:
    print(f"Device: {row.device_id}, Status: {row.status}, Updated: {row.last_updated}")
```

### Перевірка saga_log

```python
rows = session.execute("SELECT * FROM saga_log LIMIT 20")
for row in rows:
    print(f"Saga: {row.saga_id}, Device: {row.device_id}, Status: {row.status, Step: {row.step}")
```

## Технічні деталі реалізації

### Producer

- Генерує телеметрію для **150 турбін** (WIND_ZP_001 до WIND_ZP_150)
- Частота генерації: **кожні 5 секунд**
- Throughput: **~30 повідомлень/секунду** (150 турбін / 5 секунд)
- Відправляє дані в топік `turbine_telemetry`

### Hopping Windows

- **Розмір вікна**: 10 хвилин
- **Крок**: 2 хвилини
- **Обробка**: Періодична обробка кожні 2 хвилини через `@app.timer`
- **Зберігання**: Результати зберігаються в `ramp_rate_aggregates`
- **State**: Зберігається історія попередніх середніх значень для розрахунку ramp rate

### Saga Pattern

- **2-крокова транзакція** для curtailment
- **Compensation** при скасуванні curtailment
- **Повне логування** всіх кроків у `saga_log`
- **Аудит**: Всі операції відстежуються для операторів мережі

### Cassandra Schema

- `ramp_rate_aggregates`: PRIMARY KEY (device_id, window_start)
- `turbine_status`: PRIMARY KEY (device_id)
- `saga_log`: PRIMARY KEY (saga_id, timestamp)

### Продуктивність

- **Throughput**: ~30 msg/sec (150 турбін × 1 повідомлення / 5 сек)
- **Latency**: Обробка в реальному часі
- **Scalability**: Підтримка горизонтального масштабування через Kafka partitions
- **Prepared statements**: Всі операції з Cassandra використовують prepared statements для продуктивності

## Примітки

- Система розрахована на обробку даних від **150 вітрових турбін** (по 2.5 МВт кожна)
- Throughput: **~30 повідомлень/секунду** (150 турбін / 5 секунд)
- Hopping Windows обробляються періодично кожні 2 хвилини
- Saga транзакції логуються в `saga_log` для повного аудиту операторів мережі
- State зберігається для виявлення wake effects між турбінами
- Всі операції з Cassandra використовують prepared statements для продуктивності

## Важливо

Всі скрипти знаходяться в директорії `scripts/`. Для запуску:

1. **З директорії scripts/** (рекомендовано):
   ```bash
   cd scripts
   faust -A producer worker -l info
   ```

2. **З кореневої директорії**:
   ```bash
   faust -A scripts.producer worker -l info
   ```

Переконайтеся, що ви знаходитесь в правильній директорії або використовуєте правильний шлях до модулів.

