import random
import time
from queue import Queue
from pymongo import MongoClient
from threading import Thread
import datetime

node1_queue = []
node2_queue = []
node3_queue = []
node1 = {}
node2 = {}
node3 = {}

# ADD A USER
########################################################################################################################################################
########################################################################################################################################################
########################################################################################################################################################
########################################################################################################################################################
class Transaction1(Thread):
    latencies = []
    def __init__(self, userId, userName, email, password):
        Thread.__init__(self)
        self.userId = userId
        self.userName = userName
        self.email = email
        self.password = password
        self.total_time = 0.0
        self.client = MongoClient("mongodb://localhost:27017")
        self.db1 = self.client.UserDatabase

    def t1_hop1(self):
        print("Transaction_1 : Hop_1 - Started.")
        hop_start = time.perf_counter()
        node1_queue.append('T1')
        try:
            existing_user = self.db1.users.find_one({"$or": [{"user_id": self.userId}, {"email": self.email}]})
            if existing_user:
                print("Transaction_1 : The user already exists.")
                print("Transaction_1 : Hop_1 - Successful - No need of further hops.")
                node1_queue.remove('T1')
                return
            user = {
                "user_id": self.userId,
                "username": self.userName,
                "email": self.email,
                "password": self.password,
                "created_at": datetime.datetime.now()
            }
            self.db1.users.insert_one(user)
            print("Transaction_1 : Hop_1 - Successful.")
            hop_end = time.perf_counter()
            node1_queue.remove('T1')
            h1_latency = hop_end - hop_start
            Transaction1.latencies.append(h1_latency)
            node1['Transaction_1'] = h1_latency
        except Exception as e:
            print(f"Transaction_1 : Hop_1 - Aborts - {e}")

    def run(self):
        start_time = time.perf_counter()
        print("Transaction_1 : Started")
        self.t1_hop1()
        end_time = time.perf_counter()
        print("Transaction_1 : Ended")
        total_time = end_time - start_time
        print(f"Transaction_1 : Total Time - {round(total_time,2)} seconds.")

#ADD PRODUCT
########################################################################################################################################################
########################################################################################################################################################
########################################################################################################################################################
########################################################################################################################################################

class Transaction2(Thread):
    latencies = []

    def __init__(self, productId, productName, Description, Quantity, Price, SellerId, SellerName):
        Thread.__init__(self)
        self.productId = productId
        self.productName = productName
        self.Description = Description
        self.Quantity = Quantity
        self.Price = Price
        self.total_time = 0.0
        self.client2 = MongoClient("mongodb://localhost:27018")
        self.client3 = MongoClient("mongodb://localhost:27019")
        self.db2 = self.client2.ProductDatabase
        self.db3 = self.client3.OrderInformation
        self.SellerId = SellerId
        self.SellerName = SellerName

    def t2_hop1(self):
        hop_start = time.perf_counter()
        node2_queue.append('T2')
        node3_queue.append('T2')
        print(f"Transaction_2 : Started - Node_1 : {node1_queue}")
        print(f"Transaction_2 : Started - Node_2 : {node2_queue}")
        print(f"Transaction_2 : Started - Node_3 : {node3_queue}")
        print("Transaction_2 : Hop_1 - Started")
        runFlag = False
        try:
            while not runFlag:
                if node2_queue[0] == 'T2':
                    print(node2_queue)
                    exists = self.db2.products.find_one({"product_id": self.productId})
                    if exists:
                        print("Transaction_2 : Product already exists")
                        print("Transaction_2 : Hop_1 - Successful - No need of further hops.")
                        node2_queue.remove('T2')
                        node3_queue.remove('T2')
                        return
                    else:
                        product = {
                            "product_id": self.productId,
                            "name": self.productName,
                            "description": self.Description,
                            "price": self.Price,
                            "category": "Electronics",
                            "created_at": datetime.datetime.now(),
                            "quantity": self.Quantity,
                            "SellerId": self.SellerId,
                            "SellerName": self.SellerName
                        }
                        result = self.db2.products.insert_one(product)
                        if result:
                            print("Transaction_2 : Hop_1 - Successful")
                            node2_queue.pop(0)
                            runFlag = True
                            hop_end = time.perf_counter()
                            h1_latency = hop_end - hop_start
                            Transaction2.latencies.append(h1_latency)
                            node2['Transaction_2'] = h1_latency
                            time.sleep(3)
                            start_time_h2 = time.perf_counter()
                            while not self.t2_hop2(start_time_h2):
                                time.sleep(1)
                else:
                    time.sleep(1)
        except Exception as e:
            print(f"Transaction_2 : Hop_1 - Aborts - {e}")
            hop_end = time.perf_counter()
            h1_latency = hop_end - hop_start
            Transaction2.latencies.append(h1_latency)
            node2['Transaction_2'] = h1_latency

    def t2_hop2(self, start_time_h2):
        runFlag = False
        while not runFlag:
            if node3_queue[0] == 'T2':
                print("Transaction_2 : Hop_2 - Started")
                try:
                    product = {
                        "product_id": self.productId,
                        "quantity": self.Quantity,
                    }
                    result = self.db3.inventory.insert_one(product)
                    if result:
                        print("Transaction_2 : Hop_2 - Successful")
                        T2_hop2_Time = time.perf_counter()
                        h2_latency = T2_hop2_Time - start_time_h2
                        Transaction2.latencies.append(h2_latency)
                        node3['Transaction_2'] = h2_latency
                        node3_queue.pop(0)
                        runFlag = True
                        return True
                except Exception as e:
                    T2_hop2_Time = time.perf_counter()
                    print(f"Transaction_2 : Hop_2 did not commit, re-run: {e}")
                    return False
            else:
                time.sleep(1)
            return False

    def run(self):
        start_time = time.perf_counter()
        print("Transaction_2 : Started")
        self.t2_hop1()
        end_time = time.perf_counter()
        print("Transaction_2 : Ended")
        print(f"Transaction_2 : Ended - Node_1 : {node1_queue}")
        print(f"Transaction_2 : Ended - Node_2 : {node2_queue}")
        print(f"Transaction_2 : Ended - Node_3 : {node3_queue}")
        total_time = end_time - start_time
        print(f"Transaction_2 : Total Time -  {round(total_time, 2)} seconds")
#INCREMENT PRODUCT QUANTITY
########################################################################################################################################################
########################################################################################################################################################
########################################################################################################################################################
########################################################################################################################################################
class Transaction3(Thread):
    latencies = []

    def __init__(self, productId, Quantity):
        Thread.__init__(self)
        self.productId = productId
        self.Quantity = Quantity
        self.total_time = 0.0
        self.client2 = MongoClient("mongodb://localhost:27018")
        self.client3 = MongoClient("mongodb://localhost:27019")
        self.db2 = self.client2.ProductDatabase
        self.db3 = self.client3.OrderInformation

    def t3_hop1(self):
        hop_start = time.perf_counter()
        print("Transaction_3 : Hop_1 - Started")
        node2_queue.append('T3')
        node3_queue.append('T3')
        print(f"Transaction_3 : Started - Node_1 : {node1_queue}")
        print(f"Transaction_3 : Started - Node_2 : {node2_queue}")
        print(f"Transaction_3 : Started - Node_3 : {node3_queue}")
        runFlag = False
        try:
            while not runFlag:
                if node2_queue[0] == 'T3':
                    exists = self.db2.products.find_one({"product_id": self.productId})
                    if not exists:
                        print("Transaction_3 : Product does not exist.")
                        print("Transaction_3 : Hop_1 - Successful - No need to execute Hop_2")
                        node1_queue.remove('T3')
                        node2_queue.remove('T3')
                        node3_queue.remove('T3')
                        return
                    previous_val = exists['quantity']
                    result = self.db2.products.update_one(
                        {"product_id": self.productId}, {"$set": {"quantity": self.Quantity}}
                    )
                    if result:
                        print(f"Transaction_3 : The previous value is {previous_val} and the current value is {self.Quantity}")
                        print("Transaction_3 : Hop_1 - Successful")
                        runFlag = True
                        hop_end = time.perf_counter()
                        node2_queue.pop(0)
                        h1_latency = hop_end - hop_start
                        Transaction3.latencies.append(h1_latency)
                        node2['Transaction_3'] = h1_latency
                        start_time_h2 = time.perf_counter()
                        while not self.t3_hop2(start_time_h2):
                            time.sleep(1)
                else:
                    time.sleep(1)
        except Exception as e:
            print(f"Transaction_3 : Hop_1 - Aborts - {e}")

    def t3_hop2(self, hop_start):
        runFlag = False
        while not runFlag:
            if node3_queue[0] == 'T3':
                print('Transaction_3 : Hop_2 - Started')
                try:
                    result = self.db3.inventory.update_one(
                        {"product_id": self.productId}, {"$set": {"quantity": self.Quantity}}
                    )
                    if result:
                        print("Transaction_3 : Hop_2 - Successful")
                        hop_end = time.perf_counter()
                        h2_latency = hop_end - hop_start
                        Transaction3.latencies.append(h2_latency)
                        node3['Transaction_3'] = h2_latency
                        node3_queue.pop(0)
                        runFlag = True
                        return True
                except Exception as e:
                    print(f"Transaction_3 : Hop_2 - Aborts, re-run: {e}")
                    return False
            else:
                time.sleep(1)
            return False

    def run(self):
        print("Transaction_3 : Started")
        start_time = time.perf_counter()
        self.t3_hop1()
        end_time = time.perf_counter()
        print("Transaction_3 : Ended")
        print(f"Transaction_3 : Ended - Node_1 : {node1_queue}")
        print(f"Transaction_3 : Ended - Node_2 : {node2_queue}")
        print(f"Transaction_3 : Ended - Node_3 : {node3_queue}")
        self.total_time = end_time - start_time
        print(f'Transaction_3 : Total Time - {round(self.total_time, 2)} seconds')

#PLACE ORDER    
########################################################################################################################################################
########################################################################################################################################################
########################################################################################################################################################
########################################################################################################################################################       
class Transaction4(Thread):
    latencies = []
    def __init__(self, order_id, user_id, product_id, quantity):
        Thread.__init__(self)
        self.order_id = order_id
        self.user_id = user_id
        self.product_id = product_id
        self.quantity = quantity
        self.total_time = 0.0
        self.client2 = MongoClient("mongodb://localhost:27018")
        self.client3 = MongoClient("mongodb://localhost:27019")
        self.db2 = self.client2.ProductDatabase
        self.db3 = self.client3.OrderInformation
        self.products = self.db2.products
        self.orders = self.db3.orders
        self.order_items = self.db3.order_items
        self.inventory = self.db3.inventory
        
    def t4_hop1(self):
        hop_start = time.perf_counter()
        node2_queue.append('T4')
        node3_queue.append('T4')
        print("Transaction_4 : Hop_1 - Started")
        print(f"Transaction_4 : Started - Node_1 : {node1_queue}")
        print(f"Transaction_4 : Started - Node_2 : {node2_queue}")
        print(f"Transaction_4 : Started - Node_3 : {node3_queue}")
        runFlag = False
        try:
            while not runFlag:
                if node3_queue[0] == 'T4':
                # Check if the product quantity in inventory is sufficient
                    product_in_inventory = self.inventory.find_one({"product_id": self.product_id})
                    if not product_in_inventory or product_in_inventory["quantity"] < self.quantity:
                        print(f"Transaction_4 : Order {self.order_id} cannot be placed due to insufficient quantity.")
                        print(f"Transaction_4 : Hop_1 - Successful - No need to do Hop_2.")
                        return
                    else:
                        # Decrement the quantity in inventory
                        new_quantity = product_in_inventory["quantity"] - self.quantity
                        self.inventory.update_one({"product_id": self.product_id}, {"$set": {"quantity": new_quantity}})
                        # Add order to Orders collection
                        self.orders.insert_one({
                            "order_id": self.order_id,
                            "user_id": self.user_id,
                            "order_date": datetime.datetime.now(),
                            "status": "Pending"
                        })

                        # Add order item to OrderItems collection
                        self.order_items.insert_one({
                            "order_item_id": self.order_id,
                            "order_id": self.order_id,
                            "product_id": self.product_id,
                            "quantity": self.quantity,
                            "price": self.products.find_one({"product_id": self.product_id})["price"]  
                        })

                        print(f"Transaction_4 : Order {self.order_id} has been placed successfully in hop 1.")
                        hop1_time = time.perf_counter()
                        h1_latency = hop1_time - hop_start
                        print(f"Transaction_4 : Hop_1 - Successful")
                        Transaction4.latencies.append(h1_latency)
                        node3['Transaction_4'] = h1_latency
                        node3_queue.remove('T4')
                        runFlag = True
                        start_time = time.perf_counter()
                        while not self.t4_hop2(start_time):
                            continue
                else:
                    time.sleep(1)
        except Exception as e:
            print(f"Transaction_4 : Hop_1 - Aborts - {e}")


    def t4_hop2(self, start_time):
        runFlag = False    
        node2_queue.append('T4') 
        print('Transaction_4 : Hop_2 - Started')
        while not runFlag:
            if node2_queue[0] == 'T4':
                try:
                    # Decrement the product quantity in ProductDatabase
                    product = self.products.find_one({"product_id": self.product_id})
                    if product:
                        new_quantity = product["quantity"] - self.quantity
                        self.products.update_one({"product_id": self.product_id}, {"$set": {"quantity": new_quantity}})
                        print(f"Transaction_4 : Product {self.product_id} quantity has been decremented by {self.quantity}.")
                        hop2_time = time.perf_counter()
                        h2_latency = hop2_time - start_time
                        Transaction4.latencies.append(h2_latency)
                        node2['Transaction_4'] = h2_latency
                        runFlag = True
                        node2_queue.remove('T4')
                    else:
                        print(f"Product {self.product_id} not found in Product Database.")
                        node2_queue.remove('T4')
                        return
                except Exception as e:
                    print(f"Transaction_4 : Hop_2 - Aborts, re-run: {e}")
                    return False
                print(f"Transaction_4 : Hop_2 - Successful")
                return True
            else:
                time.sleep(1)
            

    def run(self):
        print("Transaction_4 : Started")
        start_time = time.perf_counter()
        self.t4_hop1()
        end_time = time.perf_counter()
        print("Transaction_4 : Ended")
        print(f"Transaction_4 : Ended - Node_1 : {node1_queue}")
        print(f"Transaction_4 : Ended - Node_2 : {node2_queue}")
        print(f"Transaction_4 : Ended - Node_3 : {node3_queue}")
        self.total_time = end_time - start_time
        print(f'Transaction_4 : Total Time - {round(self.total_time, 2)} seconds')

# Transaction_5 : Updating the user's email in the database. 
########################################################################################################################################################
########################################################################################################################################################
########################################################################################################################################################
########################################################################################################################################################   
class Transaction5(Thread):
    latencies = []
    def __init__(self, user_id, new_email):
        Thread.__init__(self)
        self.user_id = user_id
        self.new_email = new_email
        self.total_time = 0.0
        self.client = MongoClient("mongodb://localhost:27017")
        self.db1 = self.client.UserDatabase
        
    def t5_hop1(self):
        print("Transaction_5 : Hop_1 - Started")
        print(f"Transaction_5 : Started - Node_1 : {node1_queue}")
        print(f"Transaction_5 : Started - Node_2 : {node2_queue}")
        print(f"Transaction_5 : Started - Node_3 : {node3_queue}")
        node1_queue.append('T5')
        hop_start = time.perf_counter()
        flag = False
        redFlag = False
        try:
            while not redFlag:
                if node1_queue[0] == 'T5':
                    user = self.db1.users.find_one({"user_id": self.user_id})
                    if not user:
                        print(f"T5 - User with user_id {self.user_id} does not exist.")
                        print("Transaction_5 : Hop_1 - Successful - No need to execute further hops.")
                        return True
                    previous_email = user['email']
                    result = self.db1.users.update_one(
                        {"user_id": self.user_id}, {"$set": {"email": self.new_email}}
                    )
                    if result.matched_count:
                        print("Transaction_5 : Hop_1 - Successful")
                        print(f"The previous email was {previous_email} and the new email is {self.new_email}")
                        hop1_time = time.perf_counter()
                        h1_latency = hop1_time - hop_start
                        node1_queue.pop(0)
                        Transaction5.latencies.append(h1_latency)
                        node1['Transaction_5'] = h1_latency
                        flag = True
                        redFlag = True
                    else:
                        time.sleep(1)
        except Exception as e:
            print(f"Transaction_5 : Hop_1 - Aborts - {e}")
        return flag

    def run(self):
        print("Transaction_5 : Started")
        start_time = time.perf_counter()
        self.t5_hop1()
        end_time = time.perf_counter()
        print("Transaction_5 : Ended")
        print(f"Transaction_5 : Ended - Node_1 : {node1_queue}")
        print(f"Transaction_5 : Ended - Node_2 : {node2_queue}")
        print(f"Transaction_5 : Ended - Node_3 : {node3_queue}")
        self.total_time = end_time - start_time
        print(f'Transaction_5 : Total Time - {round(self.total_time, 2)} seconds')

# Transaction_6 : Updating the product's price in the database.
############################################################################################################################################################
############################################################################################################################################################
############################################################################################################################################################
############################################################################################################################################################ 
class Transaction6(Thread):
    latencies = []

    def __init__(self, product_id, new_price):
        Thread.__init__(self)
        self.product_id = product_id
        self.new_price = new_price
        self.total_time = 0.0
        self.client = MongoClient("mongodb://localhost:27018")
        self.db2 = self.client.ProductDatabase
        
    def t6_hop1(self):
        hop_start = time.perf_counter()
        node2_queue.append('T6')
        print("Transaction_6 : Hop_1 - Started")
        print(f"Transaction_6 : Started - Node_1 : {node1_queue}")
        print(f"Transaction_6 : Started - Node_2 : {node2_queue}")
        print(f"Transaction_6 : Started - Node_3 : {node3_queue}")
        flag = False
        runFlag = False
        while not runFlag:
            if node2_queue[0] == 'T6':
                try:
                    product = self.db2.products.find_one({"product_id": self.product_id})
                    if not product:
                        print(f"Transaction_6 : Product with product_id {self.product_id} does not exist.")
                        print("Transaction_6 : Hop_1 - Successful - No need to execute further hops.")
                        return True
                    previous_price = product['price']
                    result = self.db2.products.update_one(
                        {"product_id": self.product_id}, {"$set": {"price": self.new_price}}
                    )
                    if result.matched_count:
                        print("T6 - First hop successful - Price updated")
                        print(f"The previous price was {previous_price} and the new price is {self.new_price}")
                        hop1_time = time.perf_counter()
                        h1_latency = hop1_time - hop_start
                        Transaction6.latencies.append(h1_latency)
                        runFlag = True
                        node2_queue.remove('T6')
                        node2['Transaction_6'] = h1_latency
                        flag = True
                        runFlag = True
                except Exception as e:
                    print(f"Transaction_6 - Hop_1 : Aborts: {e}")
                return flag
            else: 
                time.sleep(1)

    def run(self):
        print("Transaction_6 : Started")
        start_time = time.perf_counter()
        self.t6_hop1()
        end_time = time.perf_counter()
        print("Transaction_6 : Ended")
        print(f"Transaction_6 : Ended - Node_1 : {node1_queue}")
        print(f"Transaction_6 : Ended - Node_2 : {node2_queue}")
        print(f"Transaction_6 : Ended - Node_3 : {node3_queue}")
        self.total_time = end_time - start_time
        print(f'Transaction_6 : Total Time - {round(self.total_time, 2)} seconds')

###########################################################################################################################################################
###########################################################################################################################################################
###########################################################################################################################################################
# Initialize transactions
transaction1 = Transaction1(random.randint(1,100), "Mark", "tbetha@uci.edu", "password")
transaction2 = Transaction2(random.randint(1,100), "Laptop", "15.6", 30, 1300, random.randint(1,100),"Jacob")
transaction3 = Transaction3(561, random.randint(1,50))
transaction4 = Transaction4(order_id=2, user_id=1, product_id=1, quantity=2)
transaction5 = Transaction5(user_id=1, new_email="new_email@example.com")
transaction6 = Transaction6(product_id=1, new_price=1500.00)



# Start transactions
transaction1.start()
transaction2.start()
transaction3.start()
transaction4.start()
transaction5.start()
transaction6.start()

# Wait for all transactions to complete
transaction1.join()
transaction2.join()
transaction3.join()
transaction4.join()
transaction5.join()
transaction6.join()

print(f'\n Hop execution time by Transaction_1 : {transaction1.latencies}')
print(f'Hop execution time by Transaction_2 : {transaction2.latencies}')
print(f'Hop execution time by Transaction_3 : {transaction3.latencies}')
print(f'Hop execution time by Transaction_4 : {transaction4.latencies}')
print(f'Hop execution time by Transaction_5 : {transaction5.latencies}')
print(f'Hop execution time by Transaction_6 : {transaction6.latencies}')



print(f"\nHops execution on Node 1 : {node1}")
print(f"Hops execution on Node 2 : {node2}")
print(f"Hops execution on Node 3 : {node3}")
# # Calculate and display average latency and throughput
# def calculate_metrics(transaction_class):
#     total_time = sum(transaction_class.latencies)
#     avg_latency = total_time / len(transaction_class.latencies)
#     throughput = len(transaction_class.latencies) / total_time
#     return avg_latency, throughput

# avg_latency1, throughput1 = calculate_metrics(Transaction1)
# avg_latency2, throughput2 = calculate_metrics(Transaction2)
# avg_latency3, throughput3 = calculate_metrics(Transaction3)

# print(f"Transaction 1 - Average Latency: {avg_latency1} seconds, Throughput: {throughput1} transactions/second")
# print(f"Transaction 2 - Average Latency: {avg_latency2} seconds, Throughput: {throughput2} transactions/second")
# print(f"Transaction 3 - Average Latency: {avg_latency3} seconds, Throughput: {throughput3} transactions/second")
