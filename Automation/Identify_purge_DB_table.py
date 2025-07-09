# To Process partitions: python3 database_maintenance.py identify_partitions_to_purge --csv ILM_Tables_Quickwins.csv
# To validate partitions: python3 database_maintenance.py verify_data_status_for_purging --csv ILM_Tables_Quickwins.csv
# To validate the table: python3 database_maintenance.py objects_validation 
# To create a backup: python3 database_maintenance.py create_backup --csv ILM_Tables_Quickwins.csv --backup-dir /backup_directory
# To purge partitions: python3 database_maintenance.py purge_partitions --csv ILM_Tables_Quickwins.csv

import subprocess
import pandas as pd
import datetime
import os
import argparse
import tempfile
import csv

# Function to execute a SQL query using SQL*Plus
def execute_sqlplus_query(username, password, dsn, query):
    # Escape special characters in the password
    escaped_password = password.replace("(", "\\(").replace(")", "\\)").replace("&", "\\&").replace("{", "\\{").replace("}", "\\}")
    
    # Modify the SQLPlus command to use the escaped password
    sqlplus_command = f'sqlplus -s "{username}/{escaped_password}@{dsn}"'
    
    # Prepare the command to be executed with the query
    full_query = f"""
    SPOOL {dsn}_purge.log;
    {query}
    SPOOL OFF;
    """
    
    # Execute the SQLPlus command and pass the query through a pipe
    process = subprocess.Popen(sqlplus_command, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    
    stdout, stderr = process.communicate(input=full_query)
    
    if process.returncode != 0:
        print("Error:", stderr)
        return False
    return stdout.strip()

# Function to get the instance name
def get_instance_name(username, password, dsn):
    instance_name_query = "SELECT INSTANCE_NAME FROM V$INSTANCE;"
    instance_name_result = execute_sqlplus_query(username, password, dsn, instance_name_query)
    if instance_name_result:
        instance_name = instance_name_result.splitlines()[-1].strip()
        print(f"Instance Name: {instance_name}")
        return instance_name
    else:
        print("Failed to retrieve the instance name.")
        return None

def extract_partition_names(query_result):
    lines = query_result.splitlines()
    partition_names = [line.strip() for line in lines if line and not line.startswith('PARTITION_NAME') and not line.startswith('-')and 'rows selected' not in line]
    return partition_names

# Function to parse partition names and sort by year and month
def parse_partition_name(name):
    try:
        year = int(name[1:5])
        month_abbr = name[5:8].strip()
        month_number = {
            'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
            'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
        }.get(month_abbr, 0)
        return (year, month_number)
    except ValueError:
        return (0, 0)  # Return a tuple that will be sorted at the end

def sort_partitions(partitions):
    return sorted(partitions, key=parse_partition_name)

# Function to write data to a plain text file
def write_to_text_file(data, filename):
    with open(filename, mode='w') as file:
        file.write(data)

def write_partitions_to_file(partition_data,instance_details, filename):
    """Write the partition data to a text file."""
    with open(filename, 'w') as file:
        file.write(f"Instance Details: {instance_details}\n\n")
        for table_name, partitions in partition_data.items():
            file.write(f"Table: {table_name}\n")
            file.write("Partitions to purge:\n")
            for partition in partitions:
                file.write(f" - {partition}\n")
            file.write("\n")

# Function to handle partition operations
def identify_partitions_to_purge(df, username, password, dsn):
    partition_results = ""
    partition_data ={}
    instance_name = get_instance_name(username, password, dsn)
    if not instance_name:
        print("Cannot proceed without instance name.")
        return partition_data

    instance_details = f"Instance: {instance_name}"
    for index, row in df.iterrows():
        table_name = row['Table_name']
        retain_period = row['Retention Period (Data Partition Purge After)']
        
        # Calculate retention date
        retention_date_query = f"SELECT TO_CHAR(SYSDATE - {retain_period + 30}, 'YYYY-MM-DD') AS retention_date FROM DUAL;"
        retention_date_result = execute_sqlplus_query(username, password, dsn, retention_date_query)
        if retention_date_result:
            retention_date = retention_date_result.splitlines()[-1].strip()
            print(f"Retention Date for {table_name}: {retention_date}")
            partition_results += f"Retention Date for {table_name}: {retention_date}\n"
            
            # Calculate retention month and year
            retention_year = retention_date[:4]
            retention_month = retention_date[5:7]
            retention_month_name = {
                '01': 'JAN', '02': 'FEB', '03': 'MAR', '04': 'APR', '05': 'MAY', '06': 'JUN',
                '07': 'JUL', '08': 'AUG', '09': 'SEP', '10': 'OCT', '11': 'NOV', '12': 'DEC'
            }[retention_month]
            
            # Fetch all partitions for the table
            all_partitions_query = f"""
            SELECT partition_name
            FROM dba_tab_partitions
            WHERE table_name = '{table_name.upper()}'
            ORDER BY partition_name;
            """
            all_partitions_query_result = execute_sqlplus_query(username, password, dsn, all_partitions_query)
            if all_partitions_query_result:
                all_partitions = extract_partition_names(all_partitions_query_result)
                sorted_partitions = sort_partitions(all_partitions)
                retention_cutoff = f'P{retention_year}{retention_month_name}'
                partition_results += f"Retention Cutoff: {retention_cutoff}\n"
                
                # Filter partitions to purge
                filtered_partitions = [p for p in sorted_partitions 
                                       if 'MAX' not in p and parse_partition_name(p) <= 
                                       (int(retention_year), 
                                        list({
                                            'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                                            'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
                                        }.values()).index(parse_partition_name(retention_cutoff)[1]) + 1)]
                print(f"Partitions identified to purge for {table_name}: {filtered_partitions}")
                partition_data[table_name] = filtered_partitions
                # Return filtered partitions for further processing
            
            else:
                print(f"Failed to retrieve partitions for table: {table_name}")
                #return [], table_name
        else:
            print(f"Failed to retrieve retention date for table: {table_name}")
           #return [], table_name
    date_suffix = datetime.datetime.now().strftime('%Y_%m-%d')
    filename = f"partition_results_{date_suffix}.txt"
    write_partitions_to_file(partition_data,instance_details, filename)
    return partition_data

# Function to validate partitions
def verify_data_status_for_purging(partition_data, username, password, dsn):
    date_suffix = datetime.datetime.now().strftime('%Y_%m-%d')
    filename = f'partition_validation_{date_suffix}.txt'
    validation_results= ""
    for table_name, partition_names in partition_data.items():
        validation_results += f"Partition Validation Results for Table: {table_name}\n"
        validation_results += "Partitions:\n"
    
        for partition in partition_names:
            validation_results += f" - {partition}\n"
    
        main_partitions = [partition for partition in partition_names if '_S' not in partition]
    
        for partition in main_partitions:
            partition = partition.strip("'")
        
            validation_query = f"""
            SELECT /*+ parallel 15 */ COUNT(1)
            FROM CISADM.{table_name.upper()} PARTITION ({partition})
            WHERE ILM_ARCH_SW = 'N';
            """
            validation_result = execute_sqlplus_query(username, password, dsn, validation_query)
            print(f"validating partitions in table {table_name.upper()}")
            if "rows selected" in validation_result.lower():
                continue
            count = int(validation_result.split('\n')[-1].strip())
            if count == 0:
                validation_results += f"Partition {partition}: Validated successfully with result {count}.\n"
            else:
                validation_results += f"Partition {partition}: Validation failed with result {count}.\n"
        
    write_to_text_file(validation_results, filename)
    print(f"Partition validation results have been written to '{filename}'.")

# Function to validate the table for invalid objects and unusable indexes
def objects_validation(username, password, dsn):
    date_suffix = datetime.datetime.now().strftime('%Y_%m-%d')
    filename = f'invalid_objects_{date_suffix}.txt'
    invalid_objects_query = """
    SELECT object_name, status
    FROM dba_objects
    WHERE status <> 'VALID';
    """
    unusable_indexes_query = """
    SELECT 'ALTER INDEX CISADM.' || index_name || ' REBUILD PARTITION ' || PARTITION_NAME || ' ONLINE PARALLEL 25;'
    FROM dba_ind_partitions
    WHERE index_owner = 'CISADM' AND status = 'UNUSABLE';
    """
    invalid_objects_result = execute_sqlplus_query(username, password, dsn, invalid_objects_query)
    unusable_indexes_result = execute_sqlplus_query(username, password, dsn, unusable_indexes_query)
    # Handle query failures
    if not invalid_objects_result:
        invalid_objects_result = "Error retrieving invalid objects."
    
    if not unusable_indexes_result:
        unusable_indexes_result = "Error retrieving unusable indexes."
    
    combined_result = f"Invalid Objects:\n{invalid_objects_result}\n\nUnusable Indexes:\n{unusable_indexes_result}"
    write_to_text_file(combined_result, filename)
    print(f"Results have been written to '{filename}'.")

def purge_partitions(username, password, dsn, partition_data):
    log_dir = f"purge_partition/{datetime.datetime.now().strftime('%d-%m-%Y')}"
    os.makedirs(log_dir, exist_ok=True)
    tablespace_csv_file = f"{log_dir}/tablespace_info.csv"
    dropped_log = f"{log_dir}/dropped_tablespaces.log"
    skipped_log = f"{log_dir}/skipped_tablespaces.log"

    # List of tablespaces to block from being dropped
    blocked_tablespaces = {'CISTS_01', 'SYSAUX', 'SYSTEM', 'USERS'}

    with open(tablespace_csv_file, 'w', newline='') as csvfile, \
         open(dropped_log, 'w') as dropped_file, \
         open(skipped_log, 'w') as skipped_file:
        
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['table_name', 'partition_name', 'tablespace_name'])

        for table_name, partition_names in partition_data.items():
            for partition in partition_names:
                try:
                    spool_file = f"{log_dir}/{table_name}_{partition}_purge.log"
                    tablespace_query = f"""
                    SELECT tablespace_name
                    FROM DBA_TAB_PARTITIONS 
                    WHERE table_name = '{table_name.upper()}'
                    AND partition_name = '{partition}';
                    """
                    tablespace_name_result = execute_sqlplus_query(username, password, dsn, tablespace_query)
                    tablespace_name = tablespace_name_result.splitlines()[-1].strip()
                    
                    # Check if the tablespace is in the blocked list
                    if tablespace_name in blocked_tablespaces:
                        skipped_file.write(f"Tablespace {tablespace_name} for table {table_name} is blocked from being dropped.\n")
                        print(f"Skipping drop for blocked tablespace {tablespace_name}.")
                        continue
                    
                    csv_writer.writerow([table_name, partition, tablespace_name])
                    drop_partition_query = f"ALTER TABLE CISADM.{table_name.upper()} DROP PARTITION {partition} UPDATE GLOBAL INDEXES;"
                    drop_result = execute_sqlplus_query(username, password, dsn, drop_partition_query)
                    
                    verify_tablespace_query = f"SELECT segment_name FROM dba_segments WHERE tablespace_name = '{tablespace_name}';"
                    verify_result = execute_sqlplus_query(username, password, dsn, verify_tablespace_query)

                    if 'no rows selected' in verify_result.lower():
                        drop_tablespace_query = f"DROP TABLESPACE {tablespace_name} INCLUDING CONTENTS AND DATAFILES;"
                        drop_ts_result = execute_sqlplus_query(username, password, dsn, drop_tablespace_query)
                        dropped_file.write(f"Tablespace {tablespace_name} for table {table_name} was dropped successfully.\n")
                        print(f"Tablespace {tablespace_name} dropped for {table_name}.")
                    else:
                        skipped_file.write(f"Tablespace {tablespace_name} for table {table_name} was not dropped. Segments still exist.\n")
                        print(f"Tablespace {tablespace_name} not dropped for {table_name} due to existing segments.")
                
                except Exception as e:
                    print(f"Error in processing tablespace {tablespace_name} for table {table_name}, partition {partition}: {str(e)}")
    print(f"Tablespace information saved to {tablespace_csv_file}.")
    print(f"Dropped tablespaces log saved to {dropped_log}.")
    print(f"Skipped tablespaces log saved to {skipped_log}.")

# Main function to parse arguments and call appropriate function
def main():
    parser = argparse.ArgumentParser(description="Run database maintenance tasks.")
    parser.add_argument('function', choices=['identify_partitions_to_purge', 'verify_data_status_for_purging', 'objects_validation', 'purge_partitions'],
                        help="The function to execute.")
    parser.add_argument('--csv', type=str, help="Path to the CSV file for processing partitions.")
    parser.add_argument('--backup-dir', type=str, help="Directory path for backups.")
    parser.add_argument('--db_dsn', type=str, help="Database Name.")
    parser.add_argument('--db_username', type=str, help="Database username.")
    parser.add_argument('--db_password', type=str, help="Database password.")

    args = parser.parse_args()
    dsn = args.db_dsn
    username = args.db_username
    password = args.db_password
    
    if args.function in ['identify_partitions_to_purge', 'verify_data_status_for_purging', 'purge_partitions']:
        if not args.csv:
            print("CSV file path is required for this function.")
            return
        df = pd.read_csv(args.csv)
    if args.function == 'identify_partitions_to_purge':
        if not args.csv:
            print("CSV file path is required for processing partitions.")
            return
        partition_data=identify_partitions_to_purge(df, username, password, dsn)

    elif args.function == 'verify_data_status_for_purging':
        if not args.csv:
            print("CSV file path is required for validating partitions.")
            return
        partition_data=identify_partitions_to_purge(df, username, password, dsn)
        verify_data_status_for_purging(partition_data, username, password, dsn)
    
    elif args.function == 'objects_validation':
        objects_validation(username, password, dsn)
    
    elif args.function == 'purge_partitions':
        if not args.csv:
            print("CSV file path is required for purging partitions.")
            return
        partition_data=identify_partitions_to_purge(df, username, password, dsn)
        purge_partitions(username, password, dsn, partition_data)

if __name__ == "__main__":
    main()
