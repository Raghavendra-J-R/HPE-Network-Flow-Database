from pymongo import MongoClient
import csv
import time
import matplotlib.pyplot as plt
from prettytable import PrettyTable

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["packetDB"]


def add_data(file_name, collection_name):
    collection = db[collection_name]
    print(f"Adding data from {file_name} to the database:")
    with open(file_name, mode="r") as file:
        reader = csv.reader(file)
        # Skip the header row
        next(reader)
        start_time = time.time()
        for row in reader:
            document = {
                "Source Address": row[0],
                "Destination Address": row[1],
                "Source Port": int(row[2]),
                "Destination Port": int(row[3]),
                "Protocol": row[4],
            }
            # Insert the document into the collection
            collection.insert_one(document)
    end_time = time.time()  # Record the end time
    time_taken = end_time - start_time
    print(
        f"Data inserted into MongoDB in {time_taken:.2f} seconds for {collection_name} collection."
    )
    return time_taken


def delete_data(collection_name):
    collection = db[collection_name]
    print(f"Deleting data from the {collection_name} collection:")
    source_port = int(input("Enter the source port: "))
    destination_port = int(input("Enter the destination port: "))

    # Doubt to be resolved here
    result = collection.delete_many(
        {"Source Port": destination_port, "Destination Port": source_port}
    )

    if result.deleted_count > 0:
        print(
            f"Deleted {result.deleted_count} document(s) from {collection_name} collection."
        )
    else:
        print("No document found matching the specified criteria.")


csv_files = ["data_100_tuples.csv", "data_1000_tuples.csv", "data_10000_tuples.csv"]


tuples_count = []
time_taken_list = []


table = PrettyTable()
table.field_names = ["Tuples Count", "Insertion Time (seconds)"]


for file_name in csv_files:
    collection_name = f"packet_data_{file_name.split('_')[1].split('.')[0]}"
    tuples_count.append(int(file_name.split("_")[1].split(".")[0]))
    time_taken = add_data(file_name, collection_name)
    time_taken_list.append(time_taken)
    table.add_row([tuples_count[-1], round(time_taken, 2)])
print(table)

plt.plot(tuples_count, time_taken_list, marker="o")
plt.title("Time taken for insertion vs. Number of tuples")
plt.xlabel("Number of tuples")
plt.ylabel("Time taken (seconds)")
plt.grid(True)
plt.show()


def prompt_for_deletion():
    try:
        print("Do you want to delete entries from any collection?")
        print("1. Yes")
        print("2. No")
        choice = int(input("Enter your choice: "))
        if choice == 1:
            print("Choose a collection to delete entries:")
            for idx, collection_name in enumerate(csv_files, start=1):
                print(
                    f"{idx}: packet_data_{collection_name.split('_')[1].split('.')[0]}"
                )
            choice = int(
                input(
                    "Enter the collection number to delete entries from (0 to cancel): "
                )
            )
            if choice in range(1, len(csv_files) + 1):
                selected_collection = (
                    f"packet_data_{csv_files[choice - 1].split('_')[1].split('.')[0]}"
                )
                delete_data(selected_collection)
            elif choice == 0:
                print("Deletion cancelled.")
            else:
                print("Invalid choice. Please enter a valid collection number.")
    except ValueError:
        print("Invalid input. Please enter a valid integer choice.")


prompt_for_deletion()
