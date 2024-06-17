from pymongo import MongoClient
from datetime import datetime
import matplotlib.pyplot as plt
import csv


def print_table(time_seconds, operation):
    header = f"| {'Number of tuples':^20} | {'Time (seconds)':^15} |"

    # Define separator line
    separator = "-" * len(header)

    # Print header and separator
    print(f"\n{operation.upper()}:\n")
    print(separator)
    print(header)
    print(separator)
    points = [100 * (10**i) for i in range(len(time_seconds))]
    # Print table rows
    for i, time in enumerate(time_seconds):
        number_of_tuples = points[i]
        time_str = str(time)
        print(f"| {number_of_tuples:^20} | {time_str:^15} |")

    # Print bottom separator
    print(separator)


def print_graph(time_seconds, operation):
    time_seconds_in_seconds = [time.total_seconds() for time in time_seconds]
    points = [100 * (10**i) for i in range(len(time_seconds))]

    # Plot the graph
    plt.figure(figsize=(8, 6))
    plt.plot(points, time_seconds_in_seconds, marker="o", linestyle="-")
    plt.title(f"{operation.upper()} Time in Seconds")
    plt.xlabel("Number of Tuples")
    plt.ylabel("Time (seconds)")
    plt.grid(True)
    plt.xscale("log")
    plt.show()


def plot_common_graph(time_for_insertions, time_for_deletion, time_for_updation):
    time_for_insertions = time_for_insertions[:-1]
    plt.figure(figsize=(8, 6))
    points = [100 * (10**i) for i in range(len(time_for_insertions))]
    plt.plot(
        points,
        [t.total_seconds() for t in time_for_insertions],
        label="Insertions",
        marker="o",
    )
    plt.plot(
        points,
        [t.total_seconds() for t in time_for_deletion],
        label="Deletions",
        marker="o",
    )
    plt.plot(
        points,
        [t.total_seconds() for t in time_for_updation],
        label="Updates",
        marker="o",
    )
    plt.title("Time in Seconds")
    plt.xlabel("Number of Tuples")
    plt.ylabel("Time (seconds)")
    plt.grid(True)
    plt.xscale("log")
    plt.legend()
    plt.show()


def main():
    client = MongoClient("localhost", 27017)
    db = client["testDB"]
    flow_table = db["flow_table"]

    common_graph_count = 0
    time_for_deletion = []
    time_for_insertions = []
    time_for_updation = []

    while True:
        print(
            "\n\nMAIN MENU\n\n1)Delete random tuples from the database\n2)Insert random tuples from csv\n3)Update random tuples in database"
        )
        if common_graph_count == 3:
            print("4)Plot the common graph\n5)Exit\n")
        else:
            print("4)Exit\n")
        choice = int(input("Enter your choice : "))
        try:
            if choice == 1:
                common_graph_count += 1
                time_for_deletion = []
                files_to_delete = [
                    "data_100_delete.csv",
                    "data_1000_delete.csv",
                    "data_10000_delete.csv",
                    "data_100000_delete.csv",
                ]
                flow_table.delete_many({})
                with open("data_1000000_tuples.csv", "r") as file:
                    reader = csv.reader(file)
                    documents = [
                        {
                            "src_ip": row[0],
                            "dest_ip": row[1],
                            "src_port": int(row[2]),
                            "dest_port": int(row[3]),
                            "ip_type": row[4],
                        }
                        for row in reader
                    ]
                    flow_table.insert_many(documents)
                for filename in files_to_delete:
                    time_before_deletion = datetime.now()
                    with open(filename, "r") as file:
                        reader = csv.reader(file)
                        for row in reader:
                            flow_table.delete_one(
                                {
                                    "src_ip": row[0],
                                    "dest_ip": row[1],
                                    "src_port": int(row[2]),
                                    "dest_port": int(row[3]),
                                    "ip_type": row[4],
                                }
                            )
                    time_after_deletion = datetime.now()
                    time_for_deletion.append(time_after_deletion - time_before_deletion)
                print_table(time_for_deletion, "deletion")
                print_graph(time_for_deletion, "deletion")

            elif choice == 2:
                common_graph_count += 1
                time_for_insertions.clear()
                files = [
                    "data_100_tuples.csv",
                    "data_1000_tuples.csv",
                    "data_10000_tuples.csv",
                    "data_100000_tuples.csv",
                    "data_1000000_tuples.csv",
                ]
                for filename in files:
                    time_before_insertion = datetime.now()
                    with open(filename, "r") as file:
                        reader = csv.reader(file)
                        documents = [
                            {
                                "src_ip": row[0],
                                "dest_ip": row[1],
                                "src_port": int(row[2]),
                                "dest_port": int(row[3]),
                                "ip_type": row[4],
                            }
                            for row in reader
                        ]
                        flow_table.insert_many(documents)
                        if filename != "data_1000000_tuples.csv":
                            flow_table.delete_many({})
                    time_after_insertion = datetime.now()
                    time_for_insertions.append(
                        time_after_insertion - time_before_insertion
                    )
                print_table(time_for_insertions, "insertion")
                print_graph(time_for_insertions, "insertion")

            elif choice == 3:
                common_graph_count += 1
                time_for_updation.clear()
                files = [
                    "data_100_tuples.csv",
                    "data_1000_tuples.csv",
                    "data_10000_tuples.csv",
                    "data_100000_tuples.csv",
                ]
                for filename in files:
                    time_before_updation = datetime.now()
                    with open(filename, "r") as file:
                        reader = csv.reader(file)
                        for row in reader:
                            flow_table.update_many(
                                {
                                    "src_ip": row[0],
                                    "dest_ip": row[1],
                                    "src_port": int(row[2]),
                                    "dest_port": int(row[3]),
                                    "ip_type": row[4],
                                },
                                {"$set": {"src_port": 100}},
                            )
                    time_after_updation = datetime.now()
                    time_for_updation.append(time_after_updation - time_before_updation)
                print_table(time_for_updation, "updation")
                print_graph(time_for_updation, "updation")

            elif common_graph_count >= 3 and choice == 4:
                plot_common_graph(
                    time_for_insertions, time_for_deletion, time_for_updation
                )
            else:
                client.close()
                exit(0)
        except Exception as e:
            print("Error:", e)


if __name__ == "__main__":
    main()
