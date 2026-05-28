from datetime import datetime

from pydantic import BaseModel, Field, StrictInt
from typing import Annotated


class Event(BaseModel):
    id: Annotated[int, Field(title="Идентификатор события", ge=1)]
    name: Annotated[str, Field(title="Название события", min_length=1, max_length=100)]
    timestamp: Annotated[datetime, Field(title="Время события")]

class Data(BaseModel):
    id: StrictInt
    value: float | None = Field(gt=0, le=100)
