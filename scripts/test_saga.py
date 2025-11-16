import asyncio
from shared_setup import app
from models import CurtailmentRequest, CancelCurtailment

curtailment_requests_topic = app.topic('curtailment_requests', value_type=CurtailmentRequest)
cancel_curtailment_topic = app.topic('cancel_curtailment', value_type=CancelCurtailment)


@app.task
async def test_saga():
    """Головна функція для тестування Saga"""
    print("🧪 Тестування Saga Pattern...")
    await asyncio.sleep(5)  # Чекаємо, поки app повністю запуститься
    
    # Тест 1: Відправляємо curtailment request
    print("\n1. Відправка curtailment request для WIND_ZP_001...")
    request = CurtailmentRequest(device_id="WIND_ZP_001", reason="Grid overload")
    await curtailment_requests_topic.send(value=request)
    print(f"📤 Відправлено curtailment request: {request.device_id} | Reason: {request.reason}")
    await asyncio.sleep(3)
    
    # Тест 2: Відправляємо cancel curtailment
    print("\n2. Відправка cancel curtailment для WIND_ZP_001...")
    cancel = CancelCurtailment(device_id="WIND_ZP_001", reason="Grid stabilized")
    await cancel_curtailment_topic.send(value=cancel)
    print(f"📤 Відправлено cancel curtailment: {cancel.device_id} | Reason: {cancel.reason or 'N/A'}")
    await asyncio.sleep(3)
    
    print("\n✅ Тестування завершено!")


if __name__ == "__main__":
    app.main()

