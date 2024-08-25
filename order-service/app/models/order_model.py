
from sqlmodel import SQLModel, Field, Relationship
from datetime import datetime
from typing import List, Optional

# class OrderItem(SQLModel, table=True):
#     id: Optional[int] = Field(default=None, primary_key=True)
#     order_id: Optional[int] = Field(default=None, foreign_key="order.id")
#     product_id: int
#     quantity: int
#     price: float
   

class Order(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: int
    status: str = Field(default="Pending")  # e.g., Pending, Shipped, Delivered, Cancelled
    total_price: float
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    #items: List[OrderItem] = Relationship(back_populates="order")

# OrderItem.order = Relationship(back_populates="items")

#Create OrderUpdata class
class OrderUpdate(SQLModel):
    status: str | None
    total_price: float | None
    created_at: datetime | None
    updated_at: datetime | None

#create get all OrderResponse class
class OrderResponse(SQLModel):
    id: int
    user_id: int
    status: str
    total_price: float
    created_at: datetime
    updated_at: datetime

#OrderResponse By Id
class OrderResponseId(SQLModel):
    id: int
   
#Create OrderDelete 
class OrderItemDelete(SQLModel):
    id: int
    
