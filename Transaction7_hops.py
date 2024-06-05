from pymongo import MongoClient
import datetime
import time

# Connect to MongoDB instances
client2 = MongoClient('mongodb://localhost:27018')
client3 = MongoClient('mongodb://localhost:27019')

# Access databases
db2 = client2.ProductDatabase
db3 = client3.OrderInformation

# Transaction 7 - Updating the product's information in the database.
def transaction7(product_id, new_quantity, new_price):
    start_time = time.time()

    def first_hop():
        flag = False
        try:
            product = db2.products.find_one({"product_id": product_id})
            if not product:
                print(f"T7 - Product with product_id {product_id} does not exist.")
                return flag
            previous_quantity = product['quantity']
            previous_price = product['price']
            result = db2.products.update_one(
                {"product_id": product_id}, {"$set": {"quantity": new_quantity, "price": new_price}}
            )
            if result.matched_count:
                print("T7 - First hop successful - Product info updated")
                print(f"The previous quantity was {previous_quantity} and the new quantity is {new_quantity}")
                print(f"The previous price was {previous_price} and the new price is {new_price}")
                t7_hop1_time = time.time()
                print(f"Time taken to execute the first hop: {t7_hop1_time - start_time} seconds")
                flag = True
        except Exception as e:
            print(f"T7 - First hop aborts: {e}")
        return flag

    def second_hop():
        t7_hop2_time = time.time()
        flag = False
        try:
            result = db3.inventory.update_one(
                {"product_id": product_id}, {"$set": {"quantity": new_quantity}}
            )
            if result.matched_count:
                print("T7 - Second hop successful - Inventory updated")
                print(f"Time taken to execute the second hop: {t7_hop2_time - start_time} seconds")
                flag = True
        except Exception as e:
            print(f"T7 - Second hop did not commit, re-run: {e}")
        return flag

    if first_hop():
        while not second_hop():
            time.sleep(1)  # Adding a slight delay before retrying the second hop

    end_time = time.time()
    print(f"Total time taken by the transaction: {end_time - start_time} seconds")
    return start_time, end_time

# Example Usage
start_time, end_time = transaction7(1, 100, 1500.00)
print(f"Transaction completed. Time taken: {round(end_time - start_time, 1)} seconds")
