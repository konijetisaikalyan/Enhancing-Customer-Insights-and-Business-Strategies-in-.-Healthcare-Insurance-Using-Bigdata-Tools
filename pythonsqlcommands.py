import json
import mysql.connector
import pandas as pd
from datetime import datetime

# Establish MySQL connection
mydb = mysql.connector.connect(
    host="localhost",
    user="root",  # Replace with your MySQL username
    password="Sunitha@02"  # Replace with your MySQL password
)

print(mydb)

# Create cursor to execute MySQL commands
mycursor = mydb.cursor()

# Step 1: Create the healthcare database if it does not exist
mycursor.execute("SHOW DATABASES")
databases = mycursor.fetchall()

if ('healthcare',) not in databases:

    mycursor.execute("CREATE DATABASE healthcare")

# Step 2: Connect to the healthcare database
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Sunitha@02",
    database="healthcare"
)

mycursor = mydb.cursor()


# Step 3: Drop existing tables (if necessary) for a clean slate
tables = [
    "group_subgroup",
    "claims",
    "patient_details",
    "hospital_details",
    "subscriber",
    "disease",
    "subgroup",
    "groups_tble"
]

for table in tables:
    mycursor.execute(f"DROP TABLE IF EXISTS {table}")

# Step 4: Create tables
mycursor.execute("""CREATE TABLE IF NOT EXISTS groups_tble (
    grp_sk INT NOT NULL UNIQUE AUTO_INCREMENT,
    grp_id VARCHAR(6) NOT NULL PRIMARY KEY,
    grp_name VARCHAR(90),
    premium_written INT NOT NULL,
    city VARCHAR(20),
    zip_code INT,
    country VARCHAR(5),
    grp_type VARCHAR(10)
)""")

mycursor.execute("""CREATE TABLE IF NOT EXISTS subgroup (
    subgrp_sk INT NOT NULL UNIQUE AUTO_INCREMENT,
    subgrp_id VARCHAR(4) NOT NULL PRIMARY KEY,
    subgrp_name VARCHAR(90),
    monthly_premium FLOAT(6,2)
)""")

mycursor.execute("""CREATE TABLE IF NOT EXISTS group_subgroup (
    grpsub_sk INT NOT NULL UNIQUE AUTO_INCREMENT,
    g_id VARCHAR(6) NOT NULL,
    s_id VARCHAR(4) NOT NULL,
    FOREIGN KEY (g_id) REFERENCES groups_tble(grp_id),
    FOREIGN KEY (s_id) REFERENCES subgroup(subgrp_id)
)""")

mycursor.execute("""CREATE TABLE IF NOT EXISTS disease (
    disease_id INT NOT NULL PRIMARY KEY,
    disease_name VARCHAR(30) NOT NULL,
    subgrp_id VARCHAR(4),
    FOREIGN KEY (subgrp_id) REFERENCES subgroup(subgrp_id)
)""")

mycursor.execute("""CREATE TABLE IF NOT EXISTS subscriber (
    sub_id VARCHAR(10) NOT NULL PRIMARY KEY,
    first_name VARCHAR(30),
    last_name VARCHAR(20),
    street VARCHAR(30),
    birth_date DATE,
    gender VARCHAR(6),
    phone VARCHAR(15),
    city VARCHAR(30),
    zip_code INT,
    country VARCHAR(10),
    subgrp_id VARCHAR(4),
    elig_ind VARCHAR(2) NOT NULL,
    eff_date DATE NOT NULL,
    term_date DATE,
    FOREIGN KEY (subgrp_id) REFERENCES subgroup(subgrp_id)
)""")

mycursor.execute("""CREATE TABLE IF NOT EXISTS hospital_details (
    hospital_id VARCHAR(5) NOT NULL PRIMARY KEY,
    hospital_name VARCHAR(255),
    city VARCHAR(20),
    state VARCHAR(20),
    country VARCHAR(6)
)""")

mycursor.execute("""CREATE TABLE IF NOT EXISTS patient_details (
    patient_id INT NOT NULL PRIMARY KEY,
    patient_name VARCHAR(20),
    patient_gender VARCHAR(6),
    patient_birth_date DATE,
    patient_phone VARCHAR(15),
    disease_name VARCHAR(30),
    city VARCHAR(30),
    hospital_id VARCHAR(5),
    FOREIGN KEY (hospital_id) REFERENCES hospital_details(hospital_id)
)""")

mycursor.execute("""CREATE TABLE IF NOT EXISTS claims (
    claim_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    patient_id INT NOT NULL,
    disease_name VARCHAR(20),
    sub_id VARCHAR(10),
    claim_or_rejected VARCHAR(5),
    claim_type VARCHAR(20),
    claim_amount FLOAT(8,2),
    claim_date DATE,
    FOREIGN KEY (sub_id) REFERENCES subscriber(sub_id),
    FOREIGN KEY (patient_id) REFERENCES patient_details(patient_id)
)""")

# Step 5: Insert data into tables from CSV files

# Insert into groups_tble with IGNORE to skip duplicates
with open('/home/hduser/Downloads/HELTHCARE-SYSTEM-main/Processed Data/group.csv', 'r') as file:
    data = file.readlines()
    for row in data[1:]:  # Skip header row
        row_list = row.strip().split(",")
        row_tuple = (row_list[3], int(row_list[1]), int(row_list[2]), row_list[0], row_list[4], row_list[5], row_list[6])
        sql = "INSERT IGNORE INTO groups_tble (country, premium_written, zip_code, grp_id, grp_name, grp_type, city) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        mycursor.execute(sql, row_tuple)
    mydb.commit()

# Insert into subgroup
with open('/home/hduser/Downloads/HELTHCARE-SYSTEM-main/Processed Data/subgroup.csv', 'r') as file:
    data = file.readlines()
    for row in data[1:]:  # Skip header row
        row_list = row.strip().split(",")
        row_tuple = (row_list[0], row_list[1], float(row_list[2]))
        sql = "INSERT IGNORE INTO subgroup (subgrp_id, subgrp_name, monthly_premium) VALUES (%s, %s, %s)"
        mycursor.execute(sql, row_tuple)
    mydb.commit()

# Insert into hospital_details
with open('/home/hduser/Downloads/HELTHCARE-SYSTEM-main/Processed Data/hospital.csv', 'r') as file:
    data = file.readlines()
    for row in data[1:]:  # Skip header row
        row_list = row.strip().split(",")
        row_tuple = tuple(row_list)
        sql = "INSERT IGNORE INTO hospital_details (hospital_id, hospital_name, city, state, country) VALUES (%s, %s, %s, %s, %s)"
        mycursor.execute(sql, row_tuple)
    mydb.commit()

# Insert into disease
with open('/home/hduser/Downloads/HELTHCARE-SYSTEM-main/Processed Data/disease.csv', 'r') as file:
    data = file.readlines()
    for row in data[1:]:  # Skip header row
        row_list = row.strip().split(",")
        row_tuple = (int(row_list[1]), row_list[0], row_list[2])
        sql = "INSERT IGNORE INTO disease (disease_id, disease_name, subgrp_id) VALUES (%s, %s, %s)"
        mycursor.execute(sql, row_tuple)
    mydb.commit()

# Insert into patient_details
with open('/home/hduser/Downloads/HELTHCARE-SYSTEM-main/Processed Data/patient.csv', 'r') as file:
    data = file.readlines()
    for row in data[1:]:  # Skip header row
        row_list = row.strip().split(",")
        pdob = datetime.strptime(row_list[3], '%Y-%m-%d').date()
        row_tuple = (int(row_list[0]), row_list[1], row_list[2], pdob, row_list[4], row_list[5], row_list[6], row_list[7])
        sql = "INSERT IGNORE INTO patient_details (patient_id, patient_name, patient_gender, patient_birth_date, patient_phone, disease_name, city, hospital_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        mycursor.execute(sql, row_tuple)
    mydb.commit()

# Insert into subscriber
with open('/home/hduser/Downloads/HELTHCARE-SYSTEM-main/Processed Data/subscriber.csv', 'r') as file:
    data = file.readlines()
    for row in data[1:]:  # Skip header row
        row_list = row.strip().split(",")
        birth_date = datetime.strptime(row_list[5], '%Y-%m-%d').date()
        eff_date = datetime.strptime(row_list[13], '%Y-%m-%d').date()
        term_date = datetime.strptime(row_list[14].strip(), '%Y-%m-%d').date()
        row_tuple = (row_list[0], row_list[1], row_list[2], row_list[3], birth_date, row_list[6], row_list[7], row_list[8], row_list[9], int(row_list[10]), row_list[11], row_list[12], eff_date, term_date)
        sql = "INSERT IGNORE INTO subscriber (sub_id, first_name, last_name, street, birth_date, gender, phone, city, zip_code, country, subgrp_id, elig_ind, eff_date, term_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        mycursor.execute(sql, row_tuple)




    mydb.commit()

import pandas as pd
import json
from datetime import datetime
import mysql.connector


mycursor = mydb.cursor()

# Load JSON data
with open('/home/hduser/Downloads/HELTHCARE-SYSTEM-main/Processed Data/claims.json') as f:
    data_list = json.load(f)  # Load the JSON data into a list

# Convert the list to a pandas DataFrame
data = pd.DataFrame(data_list)

# Iterate through the DataFrame and insert data into the claims table
for index, row in data.iterrows():
    b = int(row['patient_id'])
    c = row['disease_name']
    d = row['Claim_Or_Rejected']
    e = row['claim_type']
    f = float(row['claim_amount'])
    h = datetime.strptime(row['claim_date'], '%Y-%m-%d').date()

    # Check if patient_id exists in patient_details
    mycursor.execute("SELECT COUNT(*) FROM patient_details WHERE patient_id = %s", (b,))
    count = mycursor.fetchone()[0]

    if count == 0:
        print(f"Patient ID {b} does not exist in patient_details. Skipping...")
        continue  # Skip this claim if the patient does not exist

    # Create a tuple of the values
    val = (b, c, d, e, f, h)

    # Prepare SQL query for insertion
    sql = """
    INSERT INTO claims (patient_id, disease_name, claim_Or_rejected, claim_type, claim_amount, claim_date) 
    VALUES (%s, %s, %s, %s, %s, %s)
    """

    # Execute the SQL command and commit the transaction
    mycursor.execute(sql, val)
    mydb.commit()
    print(f"Inserted: {val}")

# Close the database connection
mycursor.close()
mydb.close()


# Commit the transaction
mydb.commit()


# Close the cursor and connection
mycursor.close()
mydb.close()
