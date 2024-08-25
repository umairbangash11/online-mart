from fastapi import FastAPI, Depends, HTTPException
from datetime import timedelta
from fastapi.security import OAuth2PasswordRequestForm, OAuth2PasswordBearer
from app.auth.user_auth import create_access_token, decode_access_token
from app.crud.user_crud import create_user, get_user_by_username, get_all_users
from app.models.user_model import UserCreate, UserRead, User
from app.deps import get_session, get_kafka_producer
from sqlmodel import Session, SQLModel, select
from app.db_engine import engine
from aiokafka import AIOKafkaProducer
import json
from contextlib import asynccontextmanager
from typing import AsyncGenerator
import asyncio
from app import settings
from app.consumers.user_consumer import consume_messages


def create_db_and_tables() -> None:
    SQLModel.metadata.create_all(engine)


# The first part of the function, before the yield, will
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    print("Creating ... ... ?? !!!! ")

    task = asyncio.create_task(consume_messages(
        settings.KAFKA_USER_TOPIC, 'broker:19092'))
    


    create_db_and_tables()
    yield


app = FastAPI(
    lifespan=lifespan,
    title="Hello World API with DB",
    version="0.0.1",
)



@app.get("/")
def read_root():
    return {"Hello": "User Service"}

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

@app.post("/signup", response_model=UserRead)
async def signup(
    user_create: UserCreate, 
    session: Session = Depends(get_session), 
    producer: AIOKafkaProducer = Depends(get_kafka_producer)
):
    existing_user = get_user_by_username(user_create.username, session)
    if existing_user:
        raise HTTPException(status_code=400, detail="Username already registered")
    
    new_user = create_user(user_create=user_create, session=session)
    
    # Produce a message to Kafka after a successful signup
    user_dict = {"id": new_user.id, "username": new_user.username, "email": new_user.email, "event": "signup"}
    user_json = json.dumps(user_dict).encode("utf-8")
    await producer.send_and_wait("user_signup_topic", user_json)
    
    return new_user

@app.post("/token")
async def login(
    form_data: OAuth2PasswordRequestForm = Depends(), 
    session: Session = Depends(get_session),
    producer: AIOKafkaProducer = Depends(get_kafka_producer)
):
    user = get_user_by_username(form_data.username, session)
    if not user or user.password != form_data.password:
        raise HTTPException(status_code=400, detail="Incorrect username or password")

    access_token_expires = timedelta(minutes=30)
    access_token = create_access_token(subject=user.username, expires_delta=access_token_expires)

    # Produce a message to Kafka after a successful login
    login_event = {"username": user.username, "event": "login", "status": "successful"}
    login_json = json.dumps(login_event).encode("utf-8")
    await producer.send_and_wait("user_login_topic", login_json)
    
    return {"access_token": access_token, "token_type": "bearer"}

@app.get("/users/me", response_model=UserRead)
async def read_users_me(
    token: str = Depends(oauth2_scheme), 
    session: Session = Depends(get_session)
   
):
    payload = decode_access_token(token)
    username = payload.get("sub")
    if username is None:
        raise HTTPException(status_code=400, detail="Invalid token")

    user = get_user_by_username(username=username, session=session)
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    
    return user

@app.get("/users/all", response_model=list[UserRead])
async def get_all_users_endpoint(session: Session = Depends(get_session)):
    users = get_all_users(session)
    return users

