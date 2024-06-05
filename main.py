from Transaction1_hops import transaction1
from Transaction2_hops import transaction2
from Transaction3_hops import transaction3
import random


if __name__ == "__main__":
    transactions = [
        (transaction1, [10, "tanujabetha2", "tbetha@uciii.edu", "password"]),
        (transaction2, [301, "Laptop", "15.6", 30, 1300]),
        (transaction3, [561, 5]),
    ]


    # Shuffle the transactions to get a random order
    random.shuffle(transactions)

    for transaction, args in transactions:
        print(f"Running {transaction.__name__} with arguments {args}")
        start_time, end_time = transaction(*args)
        print(f"Completed {transaction.__name__}. Time taken: {round(end_time - start_time, 1)} seconds")

