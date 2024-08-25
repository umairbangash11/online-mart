# main.py
from contextlib import asynccontextmanager
from typing import Annotated
from sqlmodel import Session, SQLModel
from fastapi import FastAPI, Depends, HTTPException
from typing import AsyncGenerator
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncio
import json

from app import settings
from app.db_engine import engine
from app.models.inventory_model import (InventoryItem, InventoryItemUpdate, InventoryItemId,
InventoryItemDelete,InventoryItemResponse)
from app.crud.inventory_crud import (add_new_inventory_item, 
delete_inventory_item_by_id,
get_all_inventory_items, 
get_inventory_item_by_id,
update_inventory_item_by_id)
from app.deps import get_session, get_kafka_producer
from app.consumers.add_stock_consumer import consume_messages


def create_db_and_tables() -> None:
    SQLModel.metadata.create_all(engine)


# The first part of the function, before the yield, will
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    print("Creating tabl...")

    task = asyncio.create_task(consume_messages(
        "inventory-add-stock-response", 'broker:19092'))
    create_db_and_tables()
    print("\n\n LIFESPAN created!! \n\n")
    yield


app = FastAPI(
    lifespan=lifespan,
    title="Hello World API with DB",
    version="0.0.1",
)


@app.get("/")
def read_root():
    return {"Hello": "Inventory Service"}


@app.post("/manage-inventory/", response_model=InventoryItem)
async def create_new_inventory_item(item: InventoryItem, session: Annotated[Session, Depends(get_session)], producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]):
    """ Create a new inventory item and send it to Kafka"""

    item_dict = {field: getattr(item, field) for field in item.dict()}
    item_json = json.dumps(item_dict).encode("utf-8")
    print("item_JSON:", item_json)
    # Produce message
    #await producer.send_and_wait("AddStock", item_json)
    await producer.send_and_wait(settings.KAFKA_INVENTORY_TOPIC, item_json)
    #new_item = add_new_inventory_item(item, session)
    return item


@app.get("/manage-inventory/all", response_model=list[InventoryItemResponse])
def all_inventory_items(session: Annotated[Session, Depends(get_session)]):
    """ Get all inventory items from the database"""
    return get_all_inventory_items(session)


@app.get("/manage-inventory/{item_id}", response_model=InventoryItemId)
def single_inventory_item(item_id: int, session: Annotated[Session, Depends(get_session)]):
    """ Get a single inventory item by ID"""
    try:
        return get_inventory_item_by_id(inventory_item_id=item_id, session=session)
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.delete("/manage-inventory/{item_id}", response_model=InventoryItemDelete)
async def delete_single_inventory_item(
    item_id: int,
    session: Annotated[Session, Depends(get_session)],
    producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]
):
    """ Delete a single product by ID and send a message to Kafka"""

    try:
        # Delete the product from the database
        deleted_inventory = delete_inventory_item_by_id(inventory_item_id=item_id, session=session)
        if not deleted_inventory:
            raise HTTPException(status_code=404, detail="Inventory not found")

        # Prepare the Kafka message
        inventory_item_dict = {"item_id": item_id, "action": "delete"}
        inventory_item_json = json.dumps(inventory_item_dict).encode("utf-8")
        print("inventory_JSON:", inventory_item_json)

        # Produce message to Kafka
        #await producer.send_and_wait(settings.KAFKA_PRODUCT_TOPIC, inventory_item_json)
        await producer.send_and_wait(settings.KAFKA_INVENTORY_TOPIC, inventory_item_json)
        #await producer.send_and_wait("AddStock", inventory_item_json)

        # Return a success response
        return {"message": "Inventory deleted successfully", "item_id": item_id}

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
   
@app.patch("/manage-inventory/{item_id}", response_model=InventoryItemUpdate)
async def update_single_product(
    inventory_item_id: int,
    inventory_item: InventoryItemUpdate,
    session: Annotated[Session, Depends(get_session)],
    producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]
):
    """ Update a single product by ID and send a message to Kafka"""
    try:
        # Update the product in the database
        updated_inventory_item = update_inventory_item_by_id(
            inventory_item_id=inventory_item_id,
            to_update_inventory_item_data=inventory_item,
            session=session
        ) 
        if not updated_inventory_item:
            raise HTTPException(status_code=404, detail="Inventory Item not found")

        # Prepare the Kafka message
        inventory_item_dict = {"inventory_item_id": inventory_item_id, "action": "update", "updated_data": inventory_item.dict()}
        inventory_item_json = json.dumps(inventory_item_dict).encode("utf-8")
        print("inventory_item_JSON:", inventory_item_json)

        # Produce message to Kafka
        await producer.send_and_wait(settings.KAFKA_INVENTORY_TOPIC, inventory_item_json)

        # Return the updated product
        return updated_inventory_item

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

