from pymongo import MongoClient
from threading import Thread, Lock
import datetime

# Connect to MongoDB instances
client1 = MongoClient('mongodb://localhost:27017')
client2 = MongoClient('mongodb://localhost:27018')
client3 = MongoClient('mongodb://localhost:27019')

# Access databases
db1 = client1.UserDatabase
db2 = client2.ProductDatabase
db3 = client3.OrderInformation

# Create collections and insert a sample document to define schema implicitly

# Node 1: Users collection
users = db1.users
users.insert_one({
    "user_id": 1,
    "username": "john_doe",
    "email": "john@example.com",
    "password": "hashed_password",
    "created_at": datetime.datetime.now()
})
print("Database has been created at localhost:27017")

sellers = db1.sellers
sellers.insert_one({
    "seller_id": "s1",
    "seller_name": "company_xyz",
    "email": "company_xyz@.gmail.com",
    "created_at": datetime.datetime.now()
})

# Node 2: Products collection with quantity
products = db2.products
products.insert_one({
    "product_id": 1,
    "seller_id" : "s1",
    "name": "Laptop",
    "description": "High-end gaming laptop",
    "price": 1200.00,
    "category": "Electronics",
    "created_at": datetime.datetime.now(),
    "quantity": 50
})
print("Database has been created at localhost:27018")

# Node 3: Orders, OrderItems, and Inventory collections
orders = db3.orders
orders.insert_one({
    "order_id": 1,
    "user_id": 1,
    "order_date": datetime.datetime.now(),
    "status": "Pending"
})

order_items = db3.order_items
order_items.insert_one({
    "order_item_id": 1,
    "order_id": 1,
    "product_id": 1,
    "quantity": 2,
    "price": 1200.00
})

inventory = db3.inventory
inventory.insert_one({
    "product_id": 1,
    "quantity": 50
})
print("Database has been created at localhost:27019")