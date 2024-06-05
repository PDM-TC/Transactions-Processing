from pymongo import MongoClient
from threading import Thread, Lock
import datetime
import time

# Transaction 3 - Increment the product stock
client2 = MongoClient("mongodb://localhost:27018")
client3 = MongoClient("mongodb://localhost:27019")

db2 = client2.ProductDatabase
db3 = client3.OrderInformation


def transaction3(productId, Quantity):
    start_time = time.time()

    def t3_hop1():
        print("T3: Hop 1 started")
        flag = False
        try:
            exists = db2.products.find_one({"product_id": productId})
            previous_val = exists['quantity']
            if not exists:
                print("Product not present")
                return
            else:
                result = db2.products.update_one(
                    {"product_id": productId}, {"$set": {"quantity": Quantity}}
                )
                if result:
                    print("T3: Hop 1 ended")
                    print(f"The previous value {previous_val} and the current value is {Quantity}")
                    T3_hop1_Time = time.time()
                    print(
                        f"Time taken to execute T3 Hop 1: {T3_hop1_Time-start_time} seconds"
                    )
                    while not flag:
                        flag = t3_hop2()
        except Exception as e:
            print(f"T3 hop 1 aborts - {e}")
            return

    def t3_hop2():
        print("T3: Hop 2 started")
        T3_hop2_Time = time.time()
        flag = False
        try:
            result = db3.inventory.update_one(
                {"product_id": productId}, {"$set": {"quantity": Quantity}}
            )
            if result:
                print("T3: Hop 2 ended")
                print(
                    f"Time taken to execute the first hop: {T3_hop2_Time-start_time} seconds"
                )
                flag = True
        except Exception as e:
            print(f"T3 Hop 3 aborts, re-run: {e}")
        return flag

    t3_hop1()
    end_time = time.time()
    return start_time, end_time


