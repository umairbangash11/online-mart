from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import json
from app.models.product_model import Product, ProductUpdate, ProductRating
from app.crud.rating_crud import add_new_rating, get_all_products, get_product_by_id, delete_product_by_id, update_product_by_id
from app.deps import get_session




async def consume_rating_messages(topic, bootstrap_servers):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id="my-rating-consumer-group",
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        async for message in consumer:
            rating_data = json.loads(message.value.decode())
            print(f"Received rating message: {rating_data}")

            with next(get_session()) as session:
                db_rating = add_new_rating(
                    ProductRating(**rating_data), session=session
                )
                print("Saved rating to database:", db_rating)

    finally:
        await consumer.stop()
