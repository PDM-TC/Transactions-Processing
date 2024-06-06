from pymongo import MongoClient
import datetime
import time

# Connect to MongoDB instances
client1 = MongoClient('mongodb://localhost:27017')
client2 = MongoClient("mongodb://localhost:27018")
client3 = MongoClient("mongodb://localhost:27019")

# Access databases
db1 = client1.UserDatabase
db2 = client2.ProductDatabase
db3 = client3.OrderInformation

def delete_seller(seller_id):
    start_time = time.time()

    def hop1():
        print("T8 Hop_1 started")
        try:
            result = db1.sellers.delete_many({"seller_id": seller_id})
            if result.deleted_count:
                print(f"Seller {seller_id} deleted from sellers table")
                return True
            else:
                print("Seller doesn't exist")
        except Exception as e:
            print(f"Hop 1 - Error occurred: {e}")
        print("T8 Hop_1 is successful")
        return

    def hop2():
        print("T8 Hop_2 started")
        try:
            products = db2.products.find({"seller_id": seller_id})
            product_ids = [product["product_id"] for product in products]
            result = db2.products.delete_many({"seller_id": seller_id})
            if result.deleted_count:
                print(f"{result.deleted_count} products deleted associated with the seller")
            else:
                print(f"No product associated with the seller exists")
                return False, []
        except Exception as e:
            print(f"Hop 2 - Error occurred: {e}")
            return False, []
        print("T8 Hop_2 is successful")
        return True, product_ids

    def hop3(product_ids):
        print("T8 Hop_3 started")
        try:
            result = db3.inventory.delete_many({"product_id": {"$in": product_ids}})
            if result.deleted_count:
                print(f"{result.deleted_count} products deleted from inventory associated with the seller")
            else:
                print(f"Inventory contains no products associated with the seller's products")
                return False
        except Exception as e:
            print(f"Hop 3 - Error occurred: {e}")
            return False
        print("T8 Hop_3 is successful")
        return True

    # Perform the hops sequentially
    if hop1():
        hop2_successful, product_ids = hop2()
        if hop2_successful:
            hop3(product_ids)

    end_time = time.time()
    print(f"Total time taken: {end_time - start_time} seconds")

# Example Usage
delete_seller("S1")
