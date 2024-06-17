from pymongo import MongoClient
import csv
import time

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["packetDB"]
collection = db["packet_data"]


# Define function to add data to the database
def add_data():
    print("Adding data to the database:")
    start_time = time.time()
    with open("data.csv", mode="r") as file:
        reader = csv.reader(file)
        # Skip the header row
        next(reader)
        for row in reader:

            document = {
                "Source Address": row[0],
                "Destination Address": row[1],
                "Source Port": int(row[2]),
                "Destination Port": int(row[3]),
                "Protocol": "ipv4",
            }
            # Insert the document into the collection
            collection.insert_one(document)


add_data()


# Define function to read data from the database
def read_data():
    print("Reading data from the database:")
    for document in collection.find():
        print(document)
    print()


# Define function to update data in the database
def update_data():
    print("Updating data in the database:")
    source_port = int(input("Enter the new port: "))
    destination_port = int(input("Enter the destination port: "))
    source_address = input("Enter the source address: ")
    destination_address = input("Enter the destination address: ")

    # Update based on source and destination addresses
    result = collection.update_many(
        {"Source Port": source_port, "Destination Port": destination_port},
        {
            "$set": {
                "Source Address": source_address,
                "Destination Address": destination_address,
            }
        },
    )

    if result.modified_count > 0:
        print(f"Updated {result.modified_count} document(s).")
    else:
        print("No document found matching the specified criteria.")


# Define function to delete data from the database
def delete_data():
    print("Deleting data from the database:")
    source_address = input("Enter the source address: ")
    destination_address = input("Enter the destination address: ")

    # Delete based on source and destination addresses
    result = collection.delete_many(
        {"Source Address": source_address, "Destination Address": destination_address}
    )

    if result.deleted_count > 0:
        print(f"Deleted {result.deleted_count} document(s).")
    else:
        print("No document found matching the specified criteria.")


# Define function to exit the program
def exit_program():
    print("Exiting the program")
    quit()


# Define the switch dictionary
switch = {
    1: read_data,
    2: update_data,
    3: delete_data,
    4: exit_program,
}

# Main loop to prompt user for operation choice
while True:
    try:
        choice = int(
            input("Enter your choice (1: Read, 2: Update, 3: Delete, 4: Exit): ")
        )
        if choice not in switch:
            print("Invalid choice. Please enter a valid choice.")
            continue
        switch[choice]()
    except ValueError:
        print("Invalid input. Please enter a valid integer choice.")
