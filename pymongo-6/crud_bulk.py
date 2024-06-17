from pymongo import DeleteOne, MongoClient, UpdateOne
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
            "\n\nMAIN MENU\n\n1)Delete random tuples from the database\n2)Insert random tuples from csv\n3)Update random tuples in database\n4)Exit\n"
        )

        choice = int(input("Enter your choice : "))
        try:
            if choice == 1:

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
                    with open(filename, "r") as file:
                        reader = csv.reader(file)
                        bulk_delete_operations = [
                            DeleteOne(
                                {
                                    "src_ip": row[0],
                                    "dest_ip": row[1],
                                    "src_port": int(row[2]),
                                    "dest_port": int(row[3]),
                                    "ip_type": row[4],
                                }
                            )
                            for row in reader
                        ]
                        time_before_deletion = datetime.now()
                        flow_table.bulk_write(bulk_delete_operations)
                        time_after_deletion = datetime.now()
                        time_difference = time_after_deletion - time_before_deletion
                        time_for_deletion.append((time_difference).total_seconds())
                    print(
                        "Time required for deletion (in seconds per record ) for :",
                        filename,
                        " ",
                        sum(time_for_deletion),
                    )

            elif choice == 2:
                time_for_insertions = []

                time_for_insertions.clear()
                files = [
                    "data_100_tuples.csv",
                    "data_1000_tuples.csv",
                    "data_10000_tuples.csv",
                    "data_100000_tuples.csv",
                    "data_1000000_tuples.csv",
                ]
                for filename in files:

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
                        time_before_insertion = datetime.now()
                        flow_table.insert_many(documents)
                        time_after_insertion = datetime.now()
                        if filename != "data_1000000_tuples.csv":
                            flow_table.delete_many({})
                        time_difference = time_after_insertion - time_before_insertion
                        time_for_insertions.append(time_difference.total_seconds())
                    print(
                        "Time required for insertion (in seconds per record ) for  :",
                        filename,
                        " ",
                        sum(time_for_insertions),
                    )
                    print(time_for_insertions)
            elif choice == 3:

                files = [
                    "data_100_delete.csv",
                    "data_1000_delete.csv",
                    "data_10000_delete.csv",
                    "data_100000_delete.csv",
                ]
                for filename in files:
                    time_for_updation.clear()
                    with open(filename, "r") as file:
                        time_for_updation.clear()
                        reader = csv.reader(file)
                        bulk_update_operations = []
                        for row in reader:
                            bulk_update_operations.append(
                                UpdateOne(
                                    {
                                        "src_ip": row[0],
                                        "dest_ip": row[1],
                                        "src_port": int(row[2]),
                                        "dest_port": int(row[3]),
                                        "ip_type": row[4],
                                    },
                                    {"$set": {"src_port": 0}},
                                )
                            )
                        time_before_updation = datetime.now()
                        flow_table.bulk_write(bulk_update_operations)
                        time_after_updation = datetime.now()
                        time_difference = time_after_updation - time_before_updation
                        time_for_updation.append(time_difference.total_seconds())
                    print(
                        "Time required for updation (in seconds per record ) for  :",
                        filename,
                        " ",
                        time_for_updation,
                    )

            else:
                client.close()
                exit(0)
        except Exception as e:
            print("Error:", e)


if __name__ == "__main__":
    main()
