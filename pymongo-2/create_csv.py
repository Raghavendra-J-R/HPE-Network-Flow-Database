import csv
import random
import socket
import struct


def generate_address():
    return socket.inet_ntoa(struct.pack(">I", random.randint(1, 0xFFFFFFFF)))


def generate_port():
    return random.randint(1, 65535)


# Generate 250 random source and destination addresses, source and destination ports
data = [
    [generate_address(), generate_address(), generate_port(), generate_port()]
    for _ in range(250)
]

# File path for the CSV file
csv_file_path = "data.csv"

# Writing data into the CSV file
with open(csv_file_path, mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(
        ["Source Address", "Destination Address", "Source Port", "Destination Port"]
    )
    writer.writerows(data)

print("Network data has been written into the CSV file successfully.")
