from starlette.config import Config
from starlette.datastructures import Secret
from starlette.config import Config

# Load environment variables from a .env file
config = Config(".env")

# Retrieve the SECRET_KEY from the environment variables or the .env file
SECRET_KEY = config("SECRET_KEY", cast=str, default="A Secure Secret Key")

try:
    config = Config(".env")
except FileNotFoundError:
    config = Config()

DATABASE_URL = config("DATABASE_URL", cast=Secret)
KAFKA_USER_TOPIC = config("KAFKA_USER_TOPIC", cast=str)

# KAFKA_PRODUCT_TOPIC = config("KAFKA_PRODUCT_TOPIC", cast=str)

# KAFKA_PRODUCT_RATING_TOPIC = config("KAFKA_PRODUCT_RATING_TOPIC", cast=str)

BOOTSTRAP_SERVER = config("BOOTSTRAP_SERVER", cast=str)

KAFKA_CONSUMER_GROUP_ID_FOR_USER = config("KAFKA_CONSUMER_GROUP_ID_FOR_USER", cast=str)

# KAFKA_CONSUMER_GROUP_ID_FOR_PRODUCT = config("KAFKA_CONSUMER_GROUP_ID_FOR_PRODUCT", cast=str)

# KAFKA_CONSUMER_GROUP_ID_FOR_PRODUCT_RATING = config("KAFKA_CONSUMER_GROUP_ID_FOR_PRODUCT_RATING", cast=str)

TEST_DATABASE_URL = config("TEST_DATABASE_URL", cast=Secret)

OPENAI_API_KEY = config("OPENAI_API_KEY", cast=Secret)


