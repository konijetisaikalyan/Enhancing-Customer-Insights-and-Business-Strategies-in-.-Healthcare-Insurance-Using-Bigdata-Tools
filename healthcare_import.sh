#!/bin/bash

# Start Hive metastore if not running
nohup hive --service metastore > /dev/null 2>&1 &

# Create Hive database if not exists
hive -e "CREATE DATABASE IF NOT EXISTS healthcare_System;"

# Import tables from MySQL to Hive
echo "Starting import process..."

# groups_tble import
echo "Importing groups_tble..."
sqoop import \
--connect jdbc:mysql://localhost/healthcare \
--username root \
--password 'Sunitha@02' \
--table groups_tble \
--hive-import \
--hive-table healthcare_System.groups \
-m 1

# subgroup import
echo "Importing subgroup..."
sqoop import \
--connect jdbc:mysql://localhost/healthcare \
--username root \
--password 'Sunitha@02' \
--table subgroup \
--hive-import \
--hive-table healthcare_System.subgroup \
-m 1

# group_subgroup import
echo "Importing group_subgroup..."
sqoop import \
--connect jdbc:mysql://localhost/healthcare \
--username root \
--password 'Sunitha@02' \
--table group_subgroup \
--hive-import \
--hive-table healthcare_System.grp_subgrp \
-m 1

# hospital_details import
echo "Importing hospital_details..."
sqoop import \
--connect jdbc:mysql://localhost/healthcare \
--username root \
--password 'Sunitha@02' \
--table hospital_details \
--hive-import \
--hive-table healthcare_System.hospital \
-m 1

# patient_details import
echo "Importing patient_details..."
sqoop import \
--connect jdbc:mysql://localhost/healthcare \
--username root \
--password 'Sunitha@02' \
--table patient_details \
--hive-import \
--hive-table healthcare_System.patient \
-m 1

# disease import
echo "Importing disease..."
sqoop import \
--connect jdbc:mysql://localhost/healthcare \
--username root \
--password 'Sunitha@02' \
--table disease \
--hive-import \
--hive-table healthcare_System.disease \
-m 1

# subscriber import
echo "Importing subscriber..."
sqoop import \
--connect jdbc:mysql://localhost/healthcare \
--username root \
--password 'Sunitha@02' \
--table subscriber \
--hive-import \
--hive-table healthcare_System.subscriber \
-m 1

# claims import
echo "Importing claims..."
sqoop import \
--connect jdbc:mysql://localhost/healthcare \
--username root \
--password 'Sunitha@02' \
--table claims \
--hive-import \
--hive-table healthcare_System.claims \
-m 1

echo "Import process completed!"
