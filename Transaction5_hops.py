from pymongo import MongoClient
import datetime
import time

client = MongoClient('mongodb://localhost:27017')
db1 = client.UserDatabase

# Transaction 5 - Updating the user's email in the database.
def transaction5(userId, new_email):
    start_time = time.time()
    def first_hop():
        try:
            user = db1.users.find_one({"user_id": userId})
            if not user:
                print(f"T5 - User with user_id {userId} does not exist.")
                return
            previous_email = user['email']
            result = db1.users.update_one(
                {"user_id": userId}, {"$set": {"email": new_email}}
            )
            if result.matched_count:
                print("T5 - First hop successful - Email updated")
                print(f"The previous email {previous_email} and the new email is {new_email}")
        except Exception as e:
            print(f"T5 - First hop aborts: {e}")
            return
    first_hop()
    end_time = time.time()
    return start_time, end_time

# Example Usage
start_time, end_time = transaction5(1, "new_email@example.com")
print(f"Transaction completed. Time taken: {round(end_time - start_time, 1)} seconds")
