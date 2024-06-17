from pymongo import MongoClient, UpdateOne
from datetime import datetime
import matplotlib.pyplot as plt
import csv
from pymongo import DeleteOne


# Function to read data from file
def read_data_from_file(file_path):
    with open(file_path, "r") as file:
        data = [line.strip().split(",") for line in file]
        data = [
            {
                "src_ip": src_ip,
                "dest_ip": dest_ip,
                "src_port": int(src_port),
                "dest_port": int(dest_port),
                "ip_type": ip_type,
            }
            for src_ip, dest_ip, src_port, dest_port, ip_type in data
        ]
    return data


try:
    # Establish MongoDB connection
    client = MongoClient("localhost", 27017)
    db = client["testDB"]
    flow_table = db["flow_table"]

    cnt = 0
    time_for_deletion = []
    time_for_insertion = []
    time_for_updation = []

    while True:
        print(
            "\n\nMAIN MENU FOR BATCH OPERATIONS(BATCHES OF 1,00,000)\n\n1) Delete tuples from the database\n2) Insert tuples from csv\n3) Update tuples in databases"
        )
        if cnt == 3:
            print("4)plot the common graph\n5)Exit\n")
        else:
            print("4)Exit\n")
        choice = int(input("Enter your choice : "))

        if choice == 1:

            data = read_data_from_file("shuffled_data_10000.csv")
            batch_size = 10
            num_batches = (len(data) + batch_size - 1) // batch_size
            time_for_deletion = []

            for i in range(num_batches):

                start_idx = i * batch_size
                end_idx = min((i + 1) * batch_size, len(data))
                batch_data = data[start_idx:end_idx]
                flow_table.insert_many(batch_data)

                # Construct bulk deletion operations
                bulk_operations = [
                    DeleteOne(
                        {
                            "src_ip": doc["src_ip"],
                            "dest_ip": doc["dest_ip"],
                            "src_port": doc["src_port"],
                            "dest_port": doc["dest_port"],
                            "ip_type": doc["ip_type"],
                        }
                    )
                    for doc in batch_data
                ]

                # Execute bulk deletion operations
                start_time = datetime.now()
                flow_table.bulk_write(bulk_operations)
                end_time = datetime.now()
                time_for_deletion.append(end_time - start_time)
                print("Time for insertion is :", sum(time_for_deletion))
                print(len(time_for_deletion))

        elif choice == 2:

            data = read_data_from_file("data_10000_tuples.csv")
            batch_size = 10
            num_batches = (len(data) + batch_size - 1) // batch_size
            time_for_insertion = []

            for i in range(num_batches):
                start_idx = i * batch_size
                end_idx = min((i + 1) * batch_size, len(data))
                batch_data = data[start_idx:end_idx]

                start_time = datetime.now()
                flow_table.insert_many(batch_data)
                end_time = datetime.now()
                time_for_insertion.append(end_time - start_time)
                print("Time for insertion is :", sum(time_for_insertion))
                print(len(time_for_insertion))

        elif choice == 3:

            data = read_data_from_file("shuffled_data_10000.csv")
            batch_size = 10
            num_batches = (len(data) + batch_size - 1) // batch_size
            time_for_updation = []

            for i in range(num_batches):
                start_idx = i * batch_size
                end_idx = min((i + 1) * batch_size, len(data))
                batch_data = data[start_idx:end_idx]
                bulk_operations = [
                    UpdateOne(
                        {
                            "src_ip": doc["src_ip"],
                            "dest_ip": doc["dest_ip"],
                            "src_port": doc["src_port"],
                            "dest_port": doc["dest_port"],
                            "ip_type": doc["ip_type"],
                        },
                        {"$set": {"src_port": 0}},
                    )
                    for doc in batch_data
                ]

                start_time = datetime.now()
                flow_table.update_many(bulk_operations)
                end_time = datetime.now()
                time_for_updation.append(end_time - start_time)

        else:
            client.close()
            exit(0)

except Exception as e:
    print("An error occurred:", e)
