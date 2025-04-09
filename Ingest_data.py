from datetime import datetime
import requests
import json
import argparse
import os

# Define argument parser
parser = argparse.ArgumentParser(description="Ingest data from Fake Store API")

parser.add_argument("--force_sync", action = "store_true", help="Force product/user sync regardless of weekday")
# action = store_true make the flag true when present, false when absent

args = parser.parse_args()

# Use the flag
force_sync = args.force_sync

# Save time_stamp for today
today = datetime.now()
timestamp = today.strftime("%Y-%m-%d")
monday_flag = today.weekday() == 0

if monday_flag or force_sync:
    # Access the if only if it is the first day of the week
    # Fetch users and products 

    # Create weekly partition timestamp
    year, week_num, _ = today.isocalendar() # ISO standard week number
    partition_folder = f"week_{year}_{week_num}"

    # Create directory for weekly products list
    os.makedirs(f"data/raw/products/{partition_folder}", exist_ok = True)
    prod_filename = f"data/raw/products/{partition_folder}/products.json"

    # Get list of products 
    response = requests.get('https://fakestoreapi.com/products')
    products = response.json()

    with open(prod_filename, "w") as f:
        json.dump(products, f, indent=2)

    # Create directory for weekly users list
    os.makedirs(f"data/raw/users/{partition_folder}", exist_ok = True)
    users_filename = f"data/raw/users/{partition_folder}/users.json"

    # Get list of users
    response = requests.get('https://fakestoreapi.com/users')
    users = response.json()

    with open(users_filename, "w") as f:
        json.dump(users, f, indent=2)


# Get list of carts
response = requests.get('https://fakestoreapi.com/carts')
carts = response.json()
carts_filename = f"carts_{timestamp}.json"

with open("data/raw/" + carts_filename, "w") as f:
    json.dump(carts, f, indent=2)


