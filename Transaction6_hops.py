from pymongo import MongoClient
import datetime
import time

# Connect to MongoDB instance (Node 2)
client = MongoClient('mongodb://localhost:27018')
db2 = client.ProductDatabase

# Transaction 6 - Updating the product's price in the database.
def transaction6(product_id, new_price):
    start_time = time.time()
    
    def first_hop():
        try:
            product = db2.products.find_one({"product_id": product_id})
            if not product:
                print(f"T6 - Product with product_id {product_id} does not exist.")
                return
            previous_price = product['price']
            result = db2.products.update_one(
                {"product_id": product_id}, {"$set": {"price": new_price}}
            )
            if result.matched_count:
                print("T6 - First hop successful - Price updated")
                print(f"The previous price was {previous_price} and the new price is {new_price}")
        except Exception as e:
            print(f"T6 - First hop aborts: {e}")
            return
    
    first_hop()
    end_time = time.time()
    return start_time, end_time

# Example Usage
start_time, end_time = transaction6(1, 1500.00)
print(f"Transaction completed. Time taken: {round(end_time - start_time, 1)} seconds")
