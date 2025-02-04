from jose import jwt, JWTError
from datetime import datetime, timedelta
from app.settings import SECRET_KEY

ALGORITHM = "HS256"


def create_access_token(subject: str, expires_delta: timedelta) -> str:
    expire = datetime.utcnow() + expires_delta
    to_encode = {"exp": expire, "sub": str(subject)}
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def decode_access_token(access_token: str):
    try:
        decoded_jwt = jwt.decode(access_token, SECRET_KEY, algorithms=[ALGORITHM])
        return decoded_jwt
    except JWTError:
        raise JWTError("Invalid token")
