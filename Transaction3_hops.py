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
        flag = False
        try:
            exists = db2.products.find_one({"product_id": productId})
            if not exists:
                print("Product not present")
                return
            else:
                result = db2.products.update_one(
                    {"product_id": productId}, {"$set": {"quantity": Quantity}}
                )
                if result:
                    print("Transaction 3 - Hop 1 is successfully committed")
                    T3_hop1_Time = time.time()
                    print(
                        f"Time taken to execute the first hop: {T3_hop1_Time-start_time} seconds"
                    )
                    while not flag:
                        flag = t3_hop2()
        except Exception as e:
            print(f"t3- First hop aborts - {e}")
            return

    def t3_hop2():
        T3_hop2_Time = time.time()
        flag = False
        try:
            result = db3.inventory.update_one(
                {"product_id": productId}, {"$set": {"quantity": Quantity}}
            )
            if result:
                print(
                    "Transaction 3 - Hop 2 is successfully committed and accessed node 3"
                )
                print(
                    f"Time taken to execute the first hop: {T3_hop2_Time-start_time} seconds"
                )
                flag = True
        except Exception as e:
            print(f"t3 hop2 did not commit, re-run: {e}")
        return flag

    t3_hop1()
    end_time = time.time()
    return start_time, end_time


start_time, end_time = transaction3(561, 45)
print(f"Total time taken by the transaction {end_time-start_time} seconds")
