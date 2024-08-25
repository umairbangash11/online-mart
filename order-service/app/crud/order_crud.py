from fastapi import HTTPException
from sqlmodel import Session, select
from app.models.order_model import Order, OrderUpdate

# Add a New Product to the Database
def add_new_order(order_data: Order, session: Session):
    print("Adding Product to Database")
    session.add(order_data)
    session.commit()
    session.refresh(order_data)
    return order_data

# Get All Products from the Database
def get_all_order(session: Session):
    all_products = session.exec(select(Order)).all()
    return all_products

# Get a Product by ID
def get_order_by_id(order_id: int, session: Session):
    order = session.exec(select(Order).where(Order.id == order_id)).one_or_none()
    if order is None:
        raise HTTPException(status_code=404, detail="Order not found")
    return order

# Delete Product by ID
def delete_order_by_id(order_id: int, session: Session):
    # Step 1: Get the Product by ID
    order = session.exec(select(Order).where(Order.id == order_id)).one_or_none()
    if order is None:
        raise HTTPException(status_code=404, detail="Order not found")
    # Step 2: Delete the Product
    session.delete(order)
    session.commit()
    return {"message": "Order Deleted Successfully"}

# Update Product by ID
def update_order_by_id(order_id: int, to_update_order_data:OrderUpdate, session: Session):
    # Step 1: Get the Product by ID
    order = session.exec(select(Order).where(Order.id == order_id)).one_or_none()
    if order is None:
        raise HTTPException(status_code=404, detail="Order not found")
    # Step 2: Update the Product
    hero_data = to_update_order_data.model_dump(exclude_unset=True)
    order.sqlmodel_update(hero_data)
    session.add(order)
    session.commit()
    return order

# Validate Order by ID

def validate_order_by_id(order_id: int, session: Session) -> Order | None:
    order = session.exec(select(Order).where(Order.id == order_id)).one_or_none()
    return order

# Track Order by ID
def track_order_by_id(order_id: int, session: Session):
    order = session.exec(select(Order).where(Order.id == order_id)).one_or_none()
    if order is None:
        raise HTTPException(status_code=404, detail="Order not found")
    # Here you can add any specific logic you want for tracking, e.g., 
    # logging, returning status, etc.
    return {"order_id": order.id, "status": order.status, "updated_at": order.updated_at}

def get_order_by_id(order_id: int, session: Session):  # `session` should be of type `Session`
    order = session.exec(select(Order).where(Order.id == order_id)).one_or_none()
    if order is None:
        raise HTTPException(status_code=404, detail="Order not found")
    return order
