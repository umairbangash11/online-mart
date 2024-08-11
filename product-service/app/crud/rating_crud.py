from fastapi import HTTPException
from sqlmodel import Session, select
from app.models.product_model import ProductRating, ProductRatingUpdate

# Add a New Product Rating to the Database
def add_new_rating(rating_data: ProductRating, session: Session):
    print("Adding Product Rating to Database")
    session.add(rating_data)
    session.commit()
    session.refresh(rating_data)
    return rating_data

# Get All Ratings for a Specific Product from the Database
def get_all_ratings_for_product(product_id: int, session: Session):
    all_ratings = session.exec(select(ProductRating).where(ProductRating.product_id == product_id)).all()
    return all_ratings

# Get a Rating by ID
def get_rating_by_id(rating_id: int, session: Session):
    rating = session.exec(select(ProductRating).where(ProductRating.id == rating_id)).one_or_none()
    if rating is None:
        raise HTTPException(status_code=404, detail="Rating not found")
    return rating

# Delete Rating by ID
def delete_rating_by_id(rating_id: int, session: Session):
    # Step 1: Get the Rating by ID
    rating = session.exec(select(ProductRating).where(ProductRating.id == rating_id)).one_or_none()
    if rating is None:
        raise HTTPException(status_code=404, detail="Rating not found")
    # Step 2: Delete the Rating
    session.delete(rating)
    session.commit()
    return {"message": "Rating Deleted Successfully"}

# Update Rating by ID
def update_rating_by_id(rating_id: int, to_update_rating_data: ProductRatingUpdate, session: Session):
    # Step 1: Get the Rating by ID
    rating = session.exec(select(ProductRating).where(ProductRating.id == rating_id)).one_or_none()
    if rating is None:
        raise HTTPException(status_code=404, detail="Rating not found")
    # Step 2: Update the Rating
    rating_data = to_update_rating_data.model_dump(exclude_unset=True)
    rating.sqlmodel_update(rating_data)
    session.add(rating)
    session.commit()
    return rating

# Validate Rating by ID
def validate_rating_by_id(rating_id: int, session: Session) -> ProductRating | None:
    rating = session.exec(select(ProductRating).where(ProductRating.id == rating_id)).one_or_none()
    return rating
