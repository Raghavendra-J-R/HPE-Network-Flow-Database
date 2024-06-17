from pymongo import MongoClient
from datetime import datetime
import matplotlib.pyplot as plt
import csv


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

                for filename in files_to_delete:

                    with open(filename, "r") as file:
                        reader = csv.reader(file)
                        for row in reader:
                            time_before_deletion = datetime.now()
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
                            time_difference = time_after_deletion - time_before_deletion
                            time_for_deletion.append((time_difference).total_seconds())
                    print(
                        "Time required for deletion (in seconds per record ) for :",
                        filename,
                        " ",
                        sum(time_for_deletion),
                    )
                    print(len(time_for_deletion))
            elif choice == 2:
                time_for_insertions = []
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

                    time_for_insertions.clear()
                    with open(filename, "r") as file:
                        reader = csv.reader(file)
                        for row in reader:
                            document = {
                                "src_ip": row[0],
                                "dest_ip": row[1],
                                "src_port": int(row[2]),
                                "dest_port": int(row[3]),
                                "ip_type": row[4],
                            }

                            time_before_insertion = datetime.now()
                            flow_table.insert_one(document)
                            time_after_insertion = datetime.now()
                            time_difference = (
                                time_after_insertion - time_before_insertion
                            )
                            time_for_insertions.append(time_difference.total_seconds())
                        if filename != "data_1000000_tuples.csv":
                            flow_table.delete_many({})
                    total_time_seconds = sum(time_for_insertions)
                    total_insertions = len(time_for_insertions)
                    print("Total Time for Insertions : ", total_time_seconds)
                    average_time_seconds = total_time_seconds / total_insertions
                    average_time_microseconds = (
                        average_time_seconds * 1e6
                    )  # Convert seconds to microseconds
                    print(
                        "Average time per insertion:",
                        average_time_microseconds,
                        "microseconds",
                    )

            elif choice == 3:
                common_graph_count += 1
                time_for_updation.clear()
                files = [
                    "data_100_delete.csv",
                    # "data_1000_delete.csv",
                    # "data_10000_delete.csv",
                    # "data_100000_delete.csv",
                ]
                for filename in files:
                    time_for_updation.clear()
                    with open(filename, "r") as file:
                        reader = csv.reader(file)
                        for row in reader:
                            time_before_updation = datetime.now()
                            flow_table.update_one(
                                {
                                    "src_ip": row[0],
                                    "dest_ip": row[1],
                                    "src_port": int(row[2]),
                                    "dest_port": int(row[3]),
                                    "ip_type": row[4],
                                },
                                {"$set": {"src_port": 0}},
                            )
                            time_after_updation = datetime.now()
                            time_difference = time_after_updation - time_before_updation
                            time_for_updation.append(time_difference.total_seconds())
                    total_time_seconds = sum(time_for_updation)
                    total_updations = len(time_for_updation)
                    average_time_seconds = total_time_seconds / total_updations
                    average_time_microseconds = (
                        average_time_seconds * 1e6
                    )  # Convert seconds to microseconds
                    print(
                        "Average time per updation:",
                        average_time_microseconds,
                        "microseconds",
                    )

            else:
                client.close()
                exit(0)
        except Exception as e:
            print("Error:", e)


if __name__ == "__main__":
    main()
