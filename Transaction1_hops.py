from pymongo import MongoClient
from threading import Thread, Lock
import datetime
import time

client = MongoClient('mongodb://localhost:27017')
db1 = client.UserDatabase

#Transaction_1 - Registering the user into the database.
def transaction1(userId, userName, email, password):
    start_time = time.time()
    def first_hop():
        print("T1: Hop 1 started")
        try:
            existing_user = db1.users.find_one({"$or": [{"user_id": userId}, {"email": email}]})
            if existing_user:
                print("T1 - The user already exists")
                return
            user = {
            "user_id": userId,
            "username": userName,
            "email": email,
            "password": password,
            "created_at": datetime.datetime.now()
            }
            db1.users.insert_one(user)
            print("T1 - User added")
            print("T1 - Hop 1 ended")
        except Exception as e:
            print(f"T1- Hop 1 aborted - {e}")
            return
    first_hop()
    end_time = time.time()
    return start_time, end_time
    
