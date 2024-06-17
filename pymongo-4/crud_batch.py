import pymongo
from datetime import datetime
import matplotlib.pyplot as plt
import csv


# Function to read data from file
def read_data_from_file(file_path):
    with open(file_path, "r") as file:
        data = [line.strip().split(",") for line in file]
        # Convert port values to integers
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


# Function to print table
def print_table(time_seconds):
    header = f"| {'Batch Number':^20} | {'Time (seconds)':^15} |"
    separator = "-" * len(header)
    print(separator)
    print(header)
    print(separator)
    for i, time in enumerate(time_seconds):
        number_of_batches = i + 1
        time_str = str(time)
        print(f"| {number_of_batches:^20} | {time_str:^15} |")
    print(separator)


# Function to plot graph
def print_graph(time_seconds):
    time_seconds_in_seconds = [time.total_seconds() for time in time_seconds]
    plt.figure(figsize=(8, 6))
    plt.plot(
        range(1, len(time_seconds) + 1),
        time_seconds_in_seconds,
        marker="o",
        linestyle="-",
    )
    plt.title("Time in Seconds")
    plt.xlabel("Batch Number")
    plt.ylabel("Time (seconds)")
    plt.grid(True)
    plt.show()


# Establish MongoDB connection
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["flow_database"]
flow_table = db["flow_table"]

while True:
    print(
        "\n\nMAIN MENU FOR BATCH OPERATIONS(BATCHES OF 1,00,000)\n\n1) Delete tuples from the database\n2) Insert tuples from csv\n3) Update tuples in database\n4) Exit\n"
    )
    choice = int(input("Enter your choice : "))

    if choice == 1:
        data = read_data_from_file("data_1000000_tuples.csv")
        batch_size = 100000
        num_batches = (len(data) + batch_size - 1) // batch_size
        time_for_deletion = []

        for i in range(num_batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, len(data))
            batch_data = data[start_idx:end_idx]

            start_time = datetime.now()
            flow_table.insert_many(batch_data)
            flow_table.delete_many({"$or": batch_data})
            end_time = datetime.now()
            time_for_deletion.append(end_time - start_time)

        print_table(time_for_deletion)
        print_graph(time_for_deletion)

    elif choice == 2:
        data = read_data_from_file("data_1000000_tuples.csv")
        batch_size = 100000
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

        print_table(time_for_insertion)
        print_graph(time_for_insertion)

    elif choice == 3:
        data = read_data_from_file("data_1000000_tuples.csv")
        batch_size = 100000
        num_batches = (len(data) + batch_size - 1) // batch_size
        time_for_updation = []

        for i in range(num_batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, len(data))
            batch_data = data[start_idx:end_idx]

            start_time = datetime.now()
            flow_table.insert_many(batch_data)
            flow_table.update_many(
                {},
                {
                    "$set": {
                        "src_ip": "temp_update_table.src_ip",
                        "dest_ip": "temp_update_table.dest_ip",
                        "src_port": "temp_update_table.src_port",
                        "dest_port": "temp_update_table.dest_port",
                        "ip_type": "temp_update_table.ip_type",
                    }
                },
            )
            end_time = datetime.now()
            time_for_updation.append(end_time - start_time)

        print_table(time_for_updation)
        print_graph(time_for_updation)

    elif choice == 4:
        break

client.close()
