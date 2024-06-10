from pymongo import MongoClient
from threading import Thread
import time

class Transaction8(Thread):
    latencies = []

    def __init__(self, seller_id, name="Transaction-8"):
        Thread.__init__(self, name=name)
        self.seller_id = seller_id
        self.total_time = 0.0
        self.client1 = MongoClient('mongodb://localhost:27017')
        self.client2 = MongoClient("mongodb://localhost:27018")
        self.client3 = MongoClient("mongodb://localhost:27019")
        self.db1 = self.client1.UserDatabase
        self.db2 = self.client2.ProductDatabase
        self.db3 = self.client3.OrderInformation

    def t8_hop1(self):
        hop_start = time.perf_counter()
        print("T8 Hop_1 started")
        try:
            result = self.db1.sellers.delete_many({"seller_id": self.seller_id})
            if result.deleted_count:
                print(f"Seller {self.seller_id} deleted from sellers table")
                hop_end = time.perf_counter()
                h1_latency = hop_end - hop_start
                Transaction8.latencies.append(h1_latency)
                return True
            else:
                print("Seller doesn't exist")
        except Exception as e:
            print(f"Hop 1 - Error occurred: {e}")
        print("T8 Hop_1 is successful")
        return False

    def t8_hop2(self):
        hop_start = time.perf_counter()
        print("T8 Hop_2 started")
        try:
            products = self.db2.products.find({"seller_id": self.seller_id})
            product_ids = [product["product_id"] for product in products]
            result = self.db2.products.delete_many({"seller_id": self.seller_id})
            if result.deleted_count:
                print(f"{result.deleted_count} products deleted associated with the seller")
                hop_end = time.perf_counter()
                h2_latency = hop_end - hop_start
                Transaction8.latencies.append(h2_latency)
                return True, product_ids
            else:
                print(f"No product associated with the seller exists")
                return False, []
        except Exception as e:
            print(f"Hop 2 - Error occurred: {e}")
            return False, []
        print("T8 Hop_2 is successful")
        return True, []

    def t8_hop3(self, product_ids):
        hop_start = time.perf_counter()
        print("T8 Hop_3 started")
        try:
            result = self.db3.inventory.delete_many({"product_id": {"$in": product_ids}})
            if result.deleted_count:
                print(f"{result.deleted_count} products deleted from inventory associated with the seller")
                hop_end = time.perf_counter()
                h3_latency = hop_end - hop_start
                Transaction8.latencies.append(h3_latency)
                return True
            else:
                print(f"Inventory contains no products associated with the seller's products")
                return False
        except Exception as e:
            print(f"Hop 3 - Error occurred: {e}")
            return False
        print("T8 Hop_3 is successful")
        return True

    def run(self):
        start_time = time.time()
        if self.t8_hop1():
            hop2_successful, product_ids = self.t8_hop2()
            if hop2_successful:
                self.t8_hop3(product_ids)
        end_time = time.time()
        self.total_time = end_time - start_time
        print(f"Total time taken: {self.total_time} seconds")

# Example Usage
transaction8 = Transaction8("S1")
transaction8.start()
transaction8.join()
