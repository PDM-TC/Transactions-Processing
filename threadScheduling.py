import threading
import time
from pymongo import MongoClient
from threading import Thread
import datetime

class Transaction1(Thread):
    latencies = []
    def __init__(self, userId, userName, email, password):
        Thread.__init__(self)
        self.userId = userId
        self.userName = userName
        self.email = email
        self.password = password
        self.start_time = time.time()
        self.client = MongoClient("mongodb://localhost:27017")
        self.db1 = self.client.UserDatabase

    def t1_hop1(self):
        print("Started Hop_1 for Transaction_1")
        hop_start = time.perf_counter()
        try:
            existing_user = self.db1.users.find_one({"$or": [{"user_id": self.userId}, {"email": self.email}]})
            if existing_user:
                print("T1 - The user already exists")
                return
            user = {
                "user_id": self.userId,
                "username": self.userName,
                "email": self.email,
                "password": self.password,
                "created_at": datetime.datetime.now()
            }
            self.db1.users.insert_one(user)
            print("T1 - First hop successful - User added")
            hop_end = time.perf_counter()
            latency = hop_end - hop_start
            Transaction1.latencies.append(latency)
            print(f"Transaction 1 - Hop 1 latency: {latency} seconds")
        except Exception as e:
            print(f"T1 - First hop aborts - {e}")

    def run(self):
        start_time = time.perf_counter()
        self.t1_hop1()
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f"Total time taken by the transaction_1 {total_time} seconds")
        # Transaction1.latencies.append(total_time)

class Transaction2(Thread):
    latencies = []
    def __init__(self, productId, productName, Description, Quantity, Price):
        Thread.__init__(self)
        self.productId = productId
        self.productName = productName
        self.Description = Description
        self.Quantity = Quantity
        self.Price = Price
        self.start_time = time.time()
        self.client2 = MongoClient("mongodb://localhost:27018")
        self.client3 = MongoClient("mongodb://localhost:27019")
        self.db2 = self.client2.ProductDatabase
        self.db3 = self.client3.OrderInformation

    def t2_hop1(self):
        print("Started Hop_1 for Transaction_2")
        hop_start = time.perf_counter()
        flag = False
        try:
            exists = self.db2.products.find_one({"product_id": self.productId})
            if exists:
                print("Product already exists: Transaction_2")
                print("Transaction 2 - Hop 1 is successful")
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
                }
                result = self.db2.products.insert_one(product)
                if result:
                    print("Transaction 2 - Hop 1 is successfully")
                    T2_hop1_Time = time.time()
                    print(f"Transaction 2 - Hop 1 latency: {T2_hop1_Time - self.start_time} seconds")
                    time.sleep(2)  # Introducing a delay of 1 second
                    while not flag:
                        flag = self.t2_hop2()
        except Exception as e:
            print(f"T2 - First hop aborts - {e}")


    def t2_hop2(self):
        print("T2_H2 Started")
        start_time_h2 = time.perf_counter()
        flag = False
        product = {
            "product_id": self.productId,
            "quantity": self.Quantity,
        }
        try:
            result = self.db3.inventory.insert_one(product)
            if result:
                print("Transaction 2 - Hop 2 is successful")
                T2_hop2_Time = time.perf_counter()
                print(f"Time taken to execute T2_H2: {T2_hop2_Time - start_time_h2} seconds")
                flag = True
        except Exception as e:
            T2_hop2_Time = time.perf_counter()
            print(f"T2 hop2 did not commit, re-run: {e}")
            print(f"Time taken to execute T2_H2: {T2_hop2_Time - start_time_h2} seconds")
        return flag

    def run(self):
        start_time = time.perf_counter()
        self.t2_hop1()
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f"Total time taken by the transaction:2 {total_time} seconds")
        Transaction2.latencies.append(total_time)

class Transaction3(Thread):
    latencies = []
    def __init__(self, productId, Quantity):
        Thread.__init__(self)
        self.productId = productId
        self.Quantity = Quantity
        self.start_time = time.time()
        self.client2 = MongoClient("mongodb://localhost:27018")
        self.client3 = MongoClient("mongodb://localhost:27019")
        self.db2 = self.client2.ProductDatabase
        self.db3 = self.client3.OrderInformation
    def t3_hop1(self):
        time.sleep(1)
        print("Started Hop_1 for Transaction_3")
        hop_start = time.perf_counter()
        flag = False
        try:
            exists = self.db2.products.find_one({"product_id": self.productId})
            if not exists:
                print("Product not present: T3")
                print("Transaction 3 - Hop 1 is successful")
                #print(f"Time taken to execute the first hop: { - hop_start} seconds")
                return
            previous_val = exists['quantity']
            result = self.db2.products.update_one(
                {"product_id": self.productId}, {"$set": {"quantity": self.Quantity}}
            )
            if result:
                print("Transaction 3 - Hop 1 is successful")
                print(f"The previous value {previous_val} and the current value is {self.Quantity}: T3")
                T3_hop1_Time = time.time()
                print(f"Time taken to execute the H1_T3: {T3_hop1_Time - hop_start} seconds")
                time.sleep(1)  # Introducing a delay of 1 second
                while not flag:
                    flag = self.t3_hop2()
        except Exception as e:
            print(f"T3 - First hop aborts - {e}")


    def t3_hop2(self):
        T3_hop2_Time = time.time()
        flag = False
        try:
            result = self.db3.inventory.update_one(
                {"product_id": self.productId}, {"$set": {"quantity": self.Quantity}}
            )
            if result:
                print("Transaction 3 - Hop 2 is successfully committed and accessed node 3")
                print(f"Time taken to execute the H2 T3: {T3_hop2_Time - self.start_time} seconds")
                flag = True
        except Exception as e:
            print(f"T3 hop2 did not commit, re-run: {e}")
        return flag

    def run(self):
        start_time = time.perf_counter()
        self.t3_hop1()
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f"Total time taken by the transaction:3 {total_time} seconds")
        Transaction3.latencies.append(total_time)

# Initialize transactions
transaction1 = Transaction1(3, "Chinmayee", "tbetha@uci.edu", "password")
transaction2 = Transaction2(789, "Laptop", "15.6", 30, 1300)
transaction3 = Transaction3(561, 50)

# Start transactions
transaction1.start()
transaction2.start()
transaction3.start()

# Wait for all transactions to complete
transaction1.join()
transaction2.join()
transaction3.join()

# Calculate and display average latency and throughput
def calculate_metrics(transaction_class):
    total_time = sum(transaction_class.latencies)
    avg_latency = total_time / len(transaction_class.latencies)
    throughput = len(transaction_class.latencies) / total_time
    return avg_latency, throughput

avg_latency1, throughput1 = calculate_metrics(Transaction1)
avg_latency2, throughput2 = calculate_metrics(Transaction2)
avg_latency3, throughput3 = calculate_metrics(Transaction3)

print(f"Transaction 1 - Average Latency: {avg_latency1} seconds, Throughput: {throughput1} transactions/second")
print(f"Transaction 2 - Average Latency: {avg_latency2} seconds, Throughput: {throughput2} transactions/second")
print(f"Transaction 3 - Average Latency: {avg_latency3} seconds, Throughput: {throughput3} transactions/second")
