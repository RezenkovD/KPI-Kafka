import asyncio
import random
from datetime import datetime
from faust import Topic
from shared_setup import app
from models import TurbineTelemetry

# Топік для телеметрії
telemetry_topic: Topic = app.topic('turbine_telemetry', value_type=TurbineTelemetry)

# Кількість турбін: 150 (згідно з Варіантом 2)
NUM_DEVICES = 150


def get_device_id(device_index: int) -> str:
    """Генерує ID турбіни"""
    return f"WIND_ZP_{device_index:03d}"


def generate_telemetry_record(device_index: int) -> TurbineTelemetry:
    """Генерує запис телеметрії для турбіни"""
    # Максимальна потужність 2.5 МВт = 2500 кВт
    max_power = 2500.0
    return TurbineTelemetry(
        device_id=get_device_id(device_index),
        timestamp=datetime.utcnow().isoformat() + "Z",
        power_output=round(random.uniform(0.0, max_power), 2),
        wind_speed=round(random.uniform(3.0, 25.0), 2),
        wind_direction=round(random.uniform(0.0, 360.0), 2),
        blade_pitch=round(random.uniform(-5.0, 20.0), 2),
        vibration=round(random.uniform(0.0, 10.0), 2),
        temperature_generator=round(random.uniform(40.0, 80.0), 2),
        temperature_gearbox=round(random.uniform(30.0, 70.0), 2),
    )


async def produce_telemetry():
    """Головна функція producer - генерує та відправляє телеметрію кожні 5 секунд"""
    device_counter = 0
    
    print("🚀 Запуск Faust Producer для генерації телеметрії...")
    print(f"📡 Відправка даних в топік 'turbine_telemetry' кожні 5 секунд...")
    print(f"🏭 Кількість турбін: {NUM_DEVICES} (WIND_ZP_001 до WIND_ZP_{NUM_DEVICES:03d})")
    print(f"⚡ Throughput: ~{NUM_DEVICES / 5:.0f} msg/sec")
    
    while True:
        # Генеруємо запис для наступної турбіни (циклічно)
        device_index = (device_counter % NUM_DEVICES) + 1
        record = generate_telemetry_record(device_index)
        
        # Відправляємо в топік
        await telemetry_topic.send(value=record)
        
        print(f"📤 Відправлено телеметрію: {record.device_id} | "
              f"Power: {record.power_output:.2f} kW | "
              f"Wind: {record.wind_speed:.2f} m/s")
        
        device_counter += 1
        
        # Чекаємо 5 секунд перед наступною генерацією
        await asyncio.sleep(5)


@app.on_worker_init
async def on_started():
    """Запускається при старті Faust App"""
    print("✅ Faust App запущено, починаємо генерацію телеметрії...")
    asyncio.create_task(produce_telemetry())


if __name__ == "__main__":
    app.main()

