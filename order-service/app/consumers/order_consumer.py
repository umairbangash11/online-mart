from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import json
from app.models.order_model import Order, OrderUpdate
from app.crud.order_crud import (add_new_order)
from app.deps import get_session


async def consume_messages(topic, bootstrap_servers):
    # Create a consumer instance.
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id="my-order-consumer-group",
        # auto_offset_reset="earliest",
    )

    # Start the consumer.
    await consumer.start()
    try:
        # Continuously listen for messages.
        async for message in consumer:
            print("RAW!!")
            print(f"Received message on topic {message.topic}")

            order_data = json.loads(message.value.decode())
            print("TYPE", (type(order_data)))
            print(f"Order Data {order_data}")

            with next(get_session()) as session:
                print("SAVING DATA TO DATABSE")
                db_insert_order = add_new_order(
                    order_data=Order(**order_data), session=session)
                print("DB_INSERT_ORDER", db_insert_order)
            
            
            
                
                # Event EMIT In NEW TOPIC

            # Here you can add code to process each message.
            # Example: parse the message, store it in a database, etc.
    finally:
        # Ensure to close the consumer when done.
        await consumer.stop()



