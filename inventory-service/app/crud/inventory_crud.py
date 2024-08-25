from fastapi import HTTPException
from sqlmodel import Session, select
from app.models.inventory_model import InventoryItem, InventoryItemUpdate, InventoryItemDelete

# Add a New Inventory Item to the Database
def add_new_inventory_item(inventory_item_data: InventoryItem, session: Session):
    print("Adding Inventory Item to Database")
    
    session.add(inventory_item_data)
    session.commit()
    session.refresh(inventory_item_data)
    return inventory_item_data

# Get All Inventory Items from the Database
def get_all_inventory_items(session: Session):
    all_inventory_items = session.exec(select(InventoryItem)).all()
    return all_inventory_items

# Get an Inventory Item by ID
def get_inventory_item_by_id(inventory_item_id: int, session: Session):
    inventory_item = session.exec(select(InventoryItem).where(InventoryItem.id == inventory_item_id)).one_or_none()
    if inventory_item is None:
        raise HTTPException(status_code=404, detail="Inventory Item not found")
    return inventory_item

# Delete Inventory Item by ID
def delete_inventory_item_by_id(inventory_item_id: int, session: Session):
    # Step 1: Get the Inventory Item by ID
    inventory_item = session.exec(select(InventoryItemDelete).where(InventoryItem.id == inventory_item_id)).one_or_none()
    if inventory_item is None:
        raise HTTPException(status_code=404, detail="Inventory Item not found")
    # Step 2: Delete the Inventory Item
    session.delete(inventory_item)
    session.commit()
    return {"message": "Inventory Item Deleted Successfully"}

# Update Inventory Item by ID
def update_inventory_item_by_id(inventory_item_id: int, to_update_inventory_item_data:InventoryItemUpdate, session: Session):
    # Step 1: Get the Inventory Item by ID
    inventory_item = session.exec(select(InventoryItem).where(InventoryItem.id == inventory_item_id)).one_or_none()
    if inventory_item is None:
        raise HTTPException(status_code=404, detail="Inventory Item not found")
    # Step 2: Update the Inventory Item
    hero_data = to_update_inventory_item_data.model_dump(exclude_unset=True)
    inventory_item.sqlmodel_update(hero_data)
    session.add(inventory_item)
    session.commit()
    return inventory_item