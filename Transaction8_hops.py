from pymongo import MongoClient
from threading import Thread, Lock
import datetime
import time

client = MongoClient('mongodb://localhost:27017')
db1 = client.UserDatabase


# Transaction_1 - Registering the user into the database.
def add_seller(seller_id, seller_name, email):
    start_time = time.time()
    def first_hop():
        print("T8: Hop 1 started")
        try:
            existing_user = db1.sellers.find_one({"$or": [{"seller_id": seller_id}, {"email": email}]})
            if existing_user:
                print("T8 - The seller already exists")
                return
            user = {
                "seller_id": seller_id,
                "seller_name": seller_name,
                "email": email,
                "created_at": datetime.datetime.now()
            }
            db1.sellers.insert_one(user)
            print("T8 - seller added")
            print("T8 - Hop 1 is successful")
        except Exception as e:
            print(f"T1- Hop 1 aborted - {e}")
            return

    first_hop()
    end_time = time.time()
    return start_time, end_time

add_seller("S1", "company_xyz", "company_xyz@gmail.com")