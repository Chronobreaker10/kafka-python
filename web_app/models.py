from pydantic import BaseModel


class User(BaseModel):
    id: int
    name: str
    email: str


class Item(BaseModel):
    id: int
    name: str
    quantity: int
    price: float


class Order(BaseModel):
    id: int
    user: User
    items: list[Item]


class Response(BaseModel):
    msg: str
    error: str | None = None


class EmailMessage(BaseModel):
    email: str
    user_name: str
    subject: str
    body: str
