#cruds
from sqlmodel import Session, select
from fastapi import HTTPException
from app.models.user_model import User, UserCreate, UserUpdate

def create_user(user_create: UserCreate, session: Session) -> User:
    user = User(username=user_create.username, email=user_create.email, password=user_create.password)
    session.add(user)
    session.commit()
    session.refresh(user)
    return user

def get_user_by_username(username: str, session: Session) -> User | None:
    statement = select(User).where(User.username == username)
    user = session.exec(statement).first()
    return user

def get_user_by_email(email: str, session: Session) -> User | None:
    statement = select(User).where(User.email == email)
    user = session.exec(statement).first()
    return user

def update_user(user_id: int, user_update: UserUpdate, session: Session) -> User:
    user = session.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    update_data = user_update.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(user, key, value)
    session.add(user)
    session.commit()
    session.refresh(user)
    return user



def get_all_users(session: Session):
    users = session.exec(select(User)).all()
    if not users:
        raise HTTPException(status_code=404, detail="No users found")
    return users
