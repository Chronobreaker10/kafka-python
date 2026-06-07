import asyncio
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer

from watering_app.config import SENSOR_TOPIC_NAME
from watering_app.models import SensorReading


def decision_maker(temperature: int, humidity: int) -> bool:
    return humidity <= 60 and temperature >= 15


async def process_sensors_data():
    common_consumer = AIOKafkaConsumer(
        SENSOR_TOPIC_NAME,
        bootstrap_servers='localhost:9092',
        group_id="controller-group"
    )
    current_temperature = 0
    current_humidity = 0
    prev_sensor_timestamp = datetime.now(timezone.utc)
    await common_consumer.start()
    try:
        # common_consumer.subscribe(pattern=r'^events.*') подпишемся на все топики, начинающиеся c events
        async for message in common_consumer:
            sensor_data = SensorReading.model_validate_json(message.value.decode("utf-8"))
            if sensor_data.type == "temperature" and sensor_data.timestamp > prev_sensor_timestamp:
                current_temperature = sensor_data.value
                prev_sensor_timestamp = sensor_data.timestamp
            elif sensor_data.type == "humidity" and sensor_data.timestamp > prev_sensor_timestamp:
                current_humidity = sensor_data.value
                prev_sensor_timestamp = sensor_data.timestamp
            else:
                raise ValueError("Неизвестный тип датчика, допустимые значения: temperature, humidity")
            print(f"Текущая температура и влажность: {current_temperature}°C, {current_humidity}%")
            if decision_maker(current_temperature, current_humidity):
                print("Включаем полив")
            else:
                print('Поливать не нужно')
    finally:
        await common_consumer.stop()


if __name__ == '__main__':
    asyncio.run(process_sensors_data())