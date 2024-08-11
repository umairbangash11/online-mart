from fastapi import FastAPI, Depends, HTTPException, AsyncGenerator
from sqlmodel import Session
from app.deps import get_session
from app.models.product_model import ProductRating, ProductRatingUpdate
from app.crud.rating_crud import add_new_rating
from app.crud.rating_crud import delete_rating_by_id
from app.crud.rating_crud import get_all_ratings_for_product
from app.crud.rating_crud import get_rating_by_id
from app.crud.rating_crud import update_rating_by_id
from typing import List, Annotated
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from app import settings
import json
from contextlib import asynccontextmanager
from sqlmodel import Session, SQLModel
from app.db_engine import engine
import asyncio
from app.consumers.product_rating_consumer import consume_rating_messages
from app.consumers.product_rating_consumer import consume_messages
from app.deps import get_kafka_producer



def create_db_and_tables() -> None:
    SQLModel.metadata.create_all(engine)

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    print("Starting up...")

    # Kafka consumer for product-related messages
    task_product = asyncio.create_task(consume_messages(
        settings.KAFKA_PRODUCT_TOPIC, 'broker:19092'))

    # Kafka consumer for product rating-related messages
    task_rating = asyncio.create_task(consume_rating_messages(
        settings.KAFKA_RATING_TOPIC, 'broker:19092'))

    # Create database tables
    create_db_and_tables()
    
    yield


app = FastAPI(
    lifespan=lifespan,
    title="Hello World API with DB",
    version="0.0.1",
)

@app.post("/manage_productratings/", response_model=ProductRating)
async def create_new_rating(productRating: ProductRating, session: Annotated[Session, Depends(get_session)],
producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]
):
    """ Create a new product and send it to Kafka"""

    rating_dict = {field: getattr(rating_dict, field) for field in productRating.dict()}
    rating_json = json.dumps(productRating).encode("utf-8")
    print("product_JSON:", rating_json)
    # Produce message
    await producer.send_and_wait(settings.KAFKA_PRODUCT_TOPIC, rating_json)
    #new_product = add_new_product(product, session)
    return add_new_rating

@app.get("/manage-ratings/all", response_model=list[ProductRating])
def call_all_rating(session: Annotated[Session, Depends(get_session)]):
    """ Get all products from the database"""
    return get_all_ratings_for_product(session)


@app.get("/manage-productratings/{product_id}", response_model=ProductRating)
def get_single_rating_by_id(product_id: int, session: Annotated[Session, Depends(get_session)]):
    """ Get a single product by ID"""

    try:
        return get_rating_by_id(product_id=product_id, session=session)
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.delete("/ratings/{rating_id}", response_model=dict)
async def delete_single_rating(
    rating_id: int,
    session: Annotated[Session, Depends(get_session)],
    producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]
):
    """ Delete a single product rating by ID and send a message to Kafka"""

    try:
        # Delete the product rating from the database
        deleted_rating = delete_rating_by_id(rating_id=rating_id, session=session)
        if not deleted_rating:
            raise HTTPException(status_code=404, detail="ProductRating not found")

        # Prepare the Kafka message
        rating_dict = {"rating_id": rating_id, "action": "delete"}
        rating_json = json.dumps(rating_dict).encode("utf-8")
        print("rating_JSON:", rating_json)

        # Produce message to Kafka
        await producer.send_and_wait(settings.KAFKA_RATING_TOPIC, rating_json)

        # Return a success response
        return {"message": "ProductRating deleted successfully", "rating_id": rating_id}

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.patch("/ratings/{rating_id}", response_model=ProductRatingUpdate)
async def update_rating(
    rating_id: int,
    rating: ProductRatingUpdate,
    session: Annotated[Session, Depends(get_session)],
    producer: Annotated[AIOKafkaProducer, Depends(get_kafka_producer)]
):
    """ Update a single product rating by ID and send a message to Kafka"""
    try:
        # Update the product rating in the database
        updated_rating = update_rating_by_id(
            rating_id=rating_id, 
            to_update_rating_data=rating, 
            session=session
        )
        if not updated_rating:
            raise HTTPException(status_code=404, detail="ProductRating not found")

        # Prepare the Kafka message
        rating_dict = {"rating_id": rating_id, "action": "update", "updated_data": rating.dict()}
        rating_json = json.dumps(rating_dict).encode("utf-8")
        print("rating_JSON:", rating_json)

        # Produce message to Kafka
        await producer.send_and_wait(settings.KAFKA_RATING_TOPIC, rating_json)

        # Return the updated rating
        return updated_rating

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
