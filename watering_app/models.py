from datetime import datetime, timezone
from typing import Literal, Annotated
from pydantic import BaseModel, Field

class SensorReading(BaseModel):
    id: Annotated[int, Field(ge=0)]
    type: Annotated[Literal['temperature', 'humidity'], Field(title="Тип датчика")]
    value: Annotated[int, Field(title='Значение')]
    timestamp: Annotated[datetime, Field(default_factory=lambda: datetime.now(timezone.utc), title="Время замера")]

class TemperatureReading(SensorReading):
    type: Literal['temperature'] = 'temperature'
    value: Annotated[int, Field(ge=-60, le=60, title='Температура, °C')]

class HumidityReading(SensorReading):
    type: Literal['temperature'] = 'humidity'
    value: Annotated[int, Field(ge=0, le=100, title='Влажность, %')]
