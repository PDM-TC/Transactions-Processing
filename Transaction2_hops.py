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
        flag = False
        try:
            exists = db2.products.find_one({"product_id": productId})
            if exists:
                print("Product already exists")
                return
            else:
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
                    print("Transaction 2 - Hop 1 is successfully committed")
                    T2_hop1_Time = time.time()
                    print(f"Time taken to execute the first hop: {T2_hop1_Time-start_time} seconds")
                    while not flag:
                        flag = t2_hop2()
        except Exception as e:
            print(f"T2- First hop aborts - {e}")
            return

    def t2_hop2():
        T2_hop2_Time = time.time()
        flag = False
        product = {
                    "product_id": productId,
                    "quantity": Quantity,
                }
        try:
            result = db3.inventory.insert_one(product)
            if result:
                print("Transaction 2 - Hop 2 is successfully committed and accessed node 3")
                print(f"Time taken to execute the first hop: {T2_hop2_Time-start_time} seconds")
                flag = True
        except Exception as e:
            print(f"T2 hop2 did not commit, re-run: {e}")
        return flag
    t2_hop1()
    end_time = time.time()
    return start_time, end_time
    

start_time, end_time = transaction2( 5613 , "Laptop", "15.6", 30, 1300)
print(f"Total time taken by the transaction {end_time-start_time} seconds")