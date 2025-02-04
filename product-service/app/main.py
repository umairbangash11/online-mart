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
from app.models.product_model import Product, ProductUpdate, ProductRating, ProductRatingUpdate
from app.crud.product_crud import add_new_product, get_all_products, get_product_by_id, delete_product_by_id, update_product_by_id
from app.deps import get_session, get_kafka_producer
from app.consumers.product_consumer import consume_messages
from app.consumers.inventroy_consumer import consume_inventory_messages
from app.hello_ai import chat_completion
from app.crud.rating_crud import (
    add_new_rating,
    get_rating_by_id,
    delete_rating_by_id,
    get_all_ratings_for_product,
    update_rating_by_id
)
# from app.consumers.product_rating_consumer import consume_rating_messages

def create_db_and_tables() -> None:
    SQLModel.metadata.create_all(engine)


# The first part of the function, before the yield, will
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    print("Creating ... ... ?? !!!! ")

    task = asyncio.create_task(consume_messages(
        settings.KAFKA_PRODUCT_TOPIC, 'broker:19092'))
    
     # Kafka consumer for product rating-related messages
    # task_rating = asyncio.create_task(consume_rating_messages(
    #     settings.KAFKA_PRODUCT_RATING_TOPIC, 'broker:19092'))
    
    asyncio.create_task(consume_inventory_messages(
        "AddStock",
        #settings.KAFKA_INVENTORY_TOPIC,
        'broker:19092'
        
    ))

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


@app.post("/manage-products/", response_model=Product)
async def create_new_product(product: Product,
session: Annotated[Session, Depends(get_session)], 
producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]):
    """ Create a new product and send it to Kafka"""

    product_dict = {field: getattr(product, field) for field in product.dict()}
    product_json = json.dumps(product_dict).encode("utf-8")
    print("product_JSON:", product_json)
    # Produce message
    await producer.send_and_wait(settings.KAFKA_PRODUCT_TOPIC, product_json)
    #new_product = add_new_product(product, session)
    return product


@app.get("/manage-products/all", response_model=list[Product])
def call_all_products(session: Annotated[Session, Depends(get_session)]):
    """ Get all products from the database"""
    return get_all_products(session)


@app.get("/manage-products/{product_id}", response_model=Product)
def get_single_product(product_id: int, session: Annotated[Session, Depends(get_session)]):
    """ Get a single product by ID"""

    try:
        return get_product_by_id(product_id=product_id, session=session)
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/manage-products/{product_id}", response_model=dict)
async def delete_single_product(
    product_id: int,
    session: Annotated[Session, Depends(get_session)],
    producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]
):
    """ Delete a single product by ID and send a message to Kafka"""

    try:
        # Delete the product from the database
        deleted_product = delete_product_by_id(product_id=product_id, session=session)
        if not deleted_product:
            raise HTTPException(status_code=404, detail="Product not found")

        # Prepare the Kafka message
        product_dict = {"product_id": product_id, "action": "delete"}
        product_json = json.dumps(product_dict).encode("utf-8")
        print("product_JSON:", product_json)

        # Produce message to Kafka
        await producer.send_and_wait(settings.KAFKA_PRODUCT_TOPIC, product_json)

        # Return a success response
        return {"message": "Product deleted successfully", "product_id": product_id}

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.patch("/manage-products/{product_id}", response_model=Product)
async def update_single_product(
    product_id: int,
    product: ProductUpdate,
    session: Annotated[Session, Depends(get_session)],
    producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]
):
    """ Update a single product by ID and send a message to Kafka"""
    try:
        # Update the product in the database
        updated_product = update_product_by_id(
            product_id=product_id, 
            to_update_product_data=product, 
            session=session
        )
        if not updated_product:
            raise HTTPException(status_code=404, detail="Product not found")

        # Prepare the Kafka message
        product_dict = {"product_id": product_id, "action": "update", "updated_data": product.dict()}
        product_json = json.dumps(product_dict).encode("utf-8")
        print("product_JSON:", product_json)

        # Produce message to Kafka
        await producer.send_and_wait(settings.KAFKA_PRODUCT_TOPIC, product_json)

        # Return the updated product
        return updated_product

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
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