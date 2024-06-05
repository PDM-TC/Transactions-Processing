from pymongo import MongoClient
from threading import Thread, Lock
import datetime
import time

# Transaction 2 - Adding hops into the products table
client2 = MongoClient("mongodb://localhost:27018")
client3 = MongoClient("mongodb://localhost:27019")

db2 = client2.ProductDatabase
db3 = client3.OrderInformation


def transaction2(productId, productName, Description, Quantity, Price):
    start_time = time.time()
    def t2_hop1():
        print("T2: Hop 1 started")
        flag = False
        try:
            product = {
                "product_id": productId,
                "name": productName,
                "description": Description,
                "price": Price,
                "category": "Electronics",
                "created_at": datetime.datetime.now(),
                "quantity": Quantity,
            }
            result = db2.products.insert_one(product)
            if result:
                print("T2: Hop 1 ended")
                T2_hop1_Time = time.time()
                print(f"Time taken to execute T1 Hop 1: {T2_hop1_Time-start_time} seconds")
                while not flag:
                    flag = t2_hop2()
        except Exception as e:
            print(f"T2: Hop 1 aborted - {e}")
            return

    def t2_hop2():
        print("T2: Hop 2 started")
        T2_hop2_Time = time.time()
        flag = False
        product = {
                    "product_id": productId,
                    "quantity": Quantity,
                }
        try:
            result = db3.inventory.insert_one(product)
            if result:
                print("T2: Hop 2 ended")
                print(f"Time taken to execute T2 Hop 2: {T2_hop2_Time-start_time} seconds")
                flag = True
        except Exception as e:
            print(f"T2 hop2 aborts, re-run: {e}")
        return flag
    t2_hop1()
    end_time = time.time()
    return start_time, end_time
    

