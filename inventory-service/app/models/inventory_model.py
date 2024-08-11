from sqlmodel import SQLModel, Field, Relationship

# Inventory Microservice Models
class InventoryItem(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    product_id: int
    variant_id: int | None = None
    quantity: int
    status: str 


# InventoryItemUpdate
class InventoryItemUpdate(SQLModel):
    quantity: int | None = None
    status: str | None = None

# inventoryItemId
class InventoryItemId(SQLModel):
    id: int

# InventoryItem
class InventoryItemResponse(SQLModel):
    product_id: int
    variant_id: int | None = None
    quantity: int
    status: str

#InventoryItemDelete
class InventoryItemDelete(SQLModel):
    id: int
