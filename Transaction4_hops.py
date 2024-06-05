from pymongo import MongoClient
import datetime
import time

# Connect to MongoDB instances
client2 = MongoClient('mongodb://localhost:27018')
client3 = MongoClient('mongodb://localhost:27019')

# Access databases
db2 = client2.ProductDatabase
db3 = client3.OrderInformation

# Collections
products = db2.products
orders = db3.orders
order_items = db3.order_items
inventory = db3.inventory


def place_order(order_id, user_id, product_id, quantity):
    start_time = time.time()

    def hop1():
        flag = False
        try:
            # Check if the product quantity in inventory is sufficient
            product_in_inventory = inventory.find_one({"product_id": product_id})
            if not product_in_inventory or product_in_inventory["quantity"] < quantity:
                print(f"Order {order_id} cannot be placed due to insufficient quantity.")
                return flag
            else:
                # Decrement the quantity in inventory
                new_quantity = product_in_inventory["quantity"] - quantity
                inventory.update_one({"product_id": product_id}, {"$set": {"quantity": new_quantity}})

                # Add order to Orders collection
                orders.insert_one({
                    "order_id": order_id,
                    "user_id": user_id,
                    "order_date": datetime.datetime.now(),
                    "status": "Pending"
                })

                # Add order item to OrderItems collection
                order_items.insert_one({
                    "order_item_id": order_id,  # Assuming order_item_id is the same as order_id for simplicity
                    "order_id": order_id,
                    "product_id": product_id,
                    "quantity": quantity,
                    "price": products.find_one({"product_id": product_id})["price"]  # Get price from ProductDatabase
                })

                print(f"Order {order_id} has been placed successfully in hop 1.")
                hop1_time = time.time()
                print(f"Time taken to execute hop 1: {hop1_time - start_time} seconds")
                flag = True
        except Exception as e:
            print(f"Hop 1 aborts: {e}")
        return flag

    def hop2():
        hop2_time = time.time()
        flag = False
        try:
            # Decrement the product quantity in ProductDatabase
            product = products.find_one({"product_id": product_id})
            if product:
                new_quantity = product["quantity"] - quantity
                products.update_one({"product_id": product_id}, {"$set": {"quantity": new_quantity}})
                print(f"Product {product_id} quantity has been decremented by {quantity} in hop 2.")
                print(f"Time taken to execute hop 2: {hop2_time - start_time} seconds")
                flag = True
            else:
                print(f"Product {product_id} not found in ProductDatabase.")
        except Exception as e:
            print(f"Hop 2 did not commit, re-run: {e}")
        return flag

    if hop1():
        while not hop2():
            time.sleep(1)  # Adding a slight delay before retrying hop 2

    end_time = time.time()
    print(f"Total time taken by the transaction: {end_time - start_time} seconds")
    return start_time, end_time

# Example Usage
start_time, end_time = place_order(order_id=2, user_id=1, product_id=1, quantity=2)
print(f"Total time taken by the transaction: {end_time - start_time} seconds")
