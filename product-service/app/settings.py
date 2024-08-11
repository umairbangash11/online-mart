# from starlette.config import Config
# from starlette.datastructures import Secret

# try:
#     config = Config(".env")
# except FileNotFoundError:
#     config = Config()

# DATABASE_URL = config("DATABASE_URL", cast=Secret)
# KAFKA_PRODUCT_TOPIC = config("KAFKA_PRODUCT_TOPIC", cast=str)

# BOOTSTRAP_SERVER = config("BOOTSTRAP_SERVER", cast=str)

# KAFKA_CONSUMER_GROUP_ID_FOR_PRODUCT = config("KAFKA_CONSUMER_GROUP_ID_FOR_PRODUCT", cast=str)
# TEST_DATABASE_URL = config("TEST_DATABASE_URL", cast=Secret)

# OPENAI_API_KEY = config("OPENAI_API_KEY", cast=Secret)


from starlette.config import Config
from starlette.datastructures import Secret

try:
    config = Config(".env")
except FileNotFoundError:
    config = Config()

# Database URLs
DATABASE_URL = config("DATABASE_URL", cast=Secret)
TEST_DATABASE_URL = config("TEST_DATABASE_URL", cast=Secret)

# Kafka Topics
KAFKA_PRODUCT_TOPIC = config("KAFKA_PRODUCT_TOPIC", cast=str)
KAFKA_RATING_TOPIC = config("KAFKA_RATING_TOPIC", cast=str)  # New Kafka topic for ProductRatings

# Kafka Bootstrap Server
BOOTSTRAP_SERVER = config("BOOTSTRAP_SERVER", cast=str)

# Kafka Consumer Group IDs
KAFKA_CONSUMER_GROUP_ID_FOR_PRODUCT = config("KAFKA_CONSUMER_GROUP_ID_FOR_PRODUCT", cast=str)
KAFKA_CONSUMER_GROUP_ID_FOR_RATING = config("KAFKA_CONSUMER_GROUP_ID_FOR_RATING", cast=str)  # New consumer group for ratings

# OpenAI API Key
OPENAI_API_KEY = config("OPENAI_API_KEY", cast=Secret)
