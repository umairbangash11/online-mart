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
from app.models.order_model import Order, OrderUpdate, OrderResponse, OrderResponseId, OrderItemDelete
from app.crud.order_crud import (add_new_order,
get_all_order,get_order_by_id,
delete_order_by_id,update_order_by_id, track_order_by_id)

from app.deps import get_session, get_kafka_producer
from app.consumers.order_consumer import consume_messages
#from app.consumers.inventroy_consumer import consume_inventory_messages
from app.hello_ai import chat_completion
# from app.crud.rating_crud import (
#     add_new_rating,
#     get_rating_by_id,
#     delete_rating_by_id,
#     get_all_ratings_for_product,
#     update_rating_by_id
# )
# from app.consumers.product_rating_consumer import consume_rating_messages

def create_db_and_tables() -> None:
    SQLModel.metadata.create_all(engine)


# The first part of the function, before the yield, will
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    print("Creating ... ... ?? !!! ")

    task = asyncio.create_task(consume_messages(
        settings.KAFKA_ORDER_TOPIC, 'broker:19092'))
    
     # Kafka consumer for product rating-related messages
    # task_rating = asyncio.create_task(consume_rating_messages(
    #     settings.KAFKA_PRODUCT_RATING_TOPIC, 'broker:19092'))
    
    # asyncio.create_task(consume_inventory_messages(
    #     #"AddStock",
    #     settings.KAFKA_INVENTORY_TOPIC,
    #     'broker:19092'
        
    # ))
    
    create_db_and_tables()
    yield


app = FastAPI(
    lifespan=lifespan,
    title="Hello World API with DB",
    version="0.0.1",
)



@app.get("/")
def read_root():
    return {"Hello": "Product Service"}


@app.post("/manage-order/", response_model=Order)
async def create_new_order(order: Order,
session: Annotated[Session, Depends(get_session)], 
producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]):
    """ Create a new product and send it to Kafka"""

    order_dict = {field: getattr(order, field) for field in order.dict()}
    order_json = json.dumps(order_dict).encode("utf-8")
    print("order_JSON:", order_json)
    # Produce message
    await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, order_json)
    #new_order = add_new_order(order, session)
    return order


@app.get("/manage-order/all", response_model=list[OrderResponse])
def call_all_order(session: Annotated[Session, Depends(get_session)]):
    """ Get all products from the database"""
    return get_all_order(session)


@app.get("/manage-order/{order_id}", response_model=OrderResponseId)
def get_single_order(order_id: int, session: Annotated[Session, Depends(get_session)]):
    """ Get a single product by ID"""

    try:
        return get_order_by_id(order_id=order_id, session=session)
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/manage-orders/{order_id}", response_model=OrderItemDelete)
async def delete_single_order(
    order_id: int,
    session: Annotated[Session, Depends(get_session)],
    producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]
):
    """ Delete a single product by ID and send a message to Kafka"""

    try:
        # Delete the product from the database
        deleted_order = delete_order_by_id(order_id=order_id, session=session)
        if not deleted_order:
            raise HTTPException(status_code=404, detail="Product not found")

        # Prepare the Kafka message
        order_dict = {"order_id": order_id, "action": "delete"}
        order_json = json.dumps(order_dict).encode("utf-8")
        print("order_JSON:", order_json)

        # Produce message to Kafka
        await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, order_json)

        # Return a success response
        return {"message": "Order deleted successfully", "order_id": order_id}

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.patch("/manage-orders/{order_id}", response_model=OrderUpdate)
async def update_single_order(
    order_id: int,
    order: OrderUpdate,
    session: Annotated[Session, Depends(get_session)],
    producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]
):
    """ Update a single product by ID and send a message to Kafka"""
    try:
        # Update the product in the database
        updated_order = update_order_by_id(
            order_id=order_id, 
            to_update_order_data=order, 
            session=session
        )
        if not updated_order:
            raise HTTPException(status_code=404, detail="Order not found")

        # Prepare the Kafka message
        order_dict = {"order_id": order_id, "action": "update", "updated_data": order.dict()}
        order_json = json.dumps(order_dict).encode("utf-8")
        print("order_JSON:", order_json)

        # Produce message to Kafka
        await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, order_json)

        # Return the updated product
        return updated_order

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@app.get("/orders/track/{order_id}", response_model=OrderResponseId)
async def track_order_route(
    order_id: int,
    session: Annotated[Session, Depends(get_session)],
    producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]  # This line must be correct
):
    """Track an order by ID and send a message to Kafka"""
    try:
        # Track the order in the database
        tracked_order = track_order_by_id(session, order_id)
        if not tracked_order:
            raise HTTPException(status_code=404, detail="Order not found")

        # Prepare the Kafka message
        order_dict = {
            "order_id": tracked_order.id,
            "status": tracked_order.status,
            "tracked_at": tracked_order.updated_at.isoformat(),
            "action": "track"
        }
        order_json = json.dumps(order_dict).encode("utf-8")
        print("order_JSON:", order_json)

        # Produce message to Kafka
        await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, order_json)

        # Return the tracked order
        return tracked_order

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/manage-order/{order_id}", response_model=OrderResponseId)
def get_single_order(order_id: int, session: Annotated[Session, Depends(get_session)]):
    return get_order_by_id(order_id=order_id, session=session)



























    
# @app.post("/manage_productratings/", response_model=ProductRating)
# async def create_new_rating(productRating: ProductRating, session: Annotated[Session, Depends(get_session)],
# producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]
# ):
#     """ Create a new product and send it to Kafka"""

#     productRating_dict = {field: getattr(productRating, field) for field in productRating.dict()}
#     productRating_json = json.dumps(productRating_dict).encode("utf-8")
#     print("product_JSON:", productRating_json)
#     # Produce message
#     await producer.send_and_wait(settings.KAFKA_PRODUCT_RATING_TOPIC, productRating_json)
#     #new_product = add_new_product(product, session)
#     return add_new_rating

# @app.get("/manage-ratings/all", response_model=list[ProductRating])
# def call_all_rating(session: Annotated[Session, Depends(get_session)]):
#     """ Get all products from the database"""
#     return get_all_ratings_for_product(session)


# @app.get("/manage-productratings/{product_id}", response_model=ProductRating)
# def get_single_rating_by_id(product_id: int, session: Annotated[Session, Depends(get_session)]):
#     """ Get a single product by ID"""

#     try:
#         return get_rating_by_id(product_id=product_id, session=session)
#     except HTTPException as e:
#         raise e
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))
    
# @app.delete("/ratings/{rating_id}", response_model=dict)
# async def delete_single_rating(
#     rating_id: int,
#     session: Annotated[Session, Depends(get_session)],
#     producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]
# ):
#     """ Delete a single product rating by ID and send a message to Kafka"""

#     try:
#         # Delete the product rating from the database
#         deleted_rating = delete_rating_by_id(rating_id=rating_id, session=session)
#         if not deleted_rating:
#             raise HTTPException(status_code=404, detail="ProductRating not found")

#         # Prepare the Kafka message
#         rating_dict = {"rating_id": rating_id, "action": "delete"}
#         rating_json = json.dumps(rating_dict).encode("utf-8")
#         print("rating_JSON:", rating_json)

#         # Produce message to Kafka
#         await producer.send_and_wait(settings.KAFKA_RATING_TOPIC, rating_json)

#         # Return a success response
#         return {"message": "ProductRating deleted successfully", "rating_id": rating_id}

#     except HTTPException as e:
#         raise e
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

# @app.patch("/ratings/{rating_id}", response_model=ProductRatingUpdate)
# async def update_rating(
#     rating_id: int,
#     rating: ProductRatingUpdate,
#     session: Annotated[Session, Depends(get_session)],
#     producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]
# ):
#     """ Update a single product rating by ID and send a message to Kafka"""
#     try:
#         # Update the product rating in the database
#         updated_rating = update_rating_by_id(
#             rating_id=rating_id, 
#             to_update_rating_data=rating, 
#             session=session
#         )
#         if not updated_rating:
#             raise HTTPException(status_code=404, detail="ProductRating not found")

#         # Prepare the Kafka message
#         rating_dict = {"rating_id": rating_id, "action": "update", "updated_data": rating.dict()}
#         rating_json = json.dumps(rating_dict).encode("utf-8")
#         print("rating_JSON:", rating_json)

#         # Produce message to Kafka
#         await producer.send_and_wait(settings.KAFKA_RATING_TOPIC, rating_json)

#         # Return the updated rating
#         return updated_rating

#     except HTTPException as e:
#         raise e
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))




# @app.get("/hello-ai")
# def get_ai_response(prompt:str):
#     return chat_completion(prompt)