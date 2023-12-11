import psycopg2
import pandas as pd
from sqlalchemy import create_engine

# Database Credentials:
dbhost = '10.0.0.7'
dbname_stage = 'pension_stage'
dbname_db = 'pension_database'
dbname_kri = 'kri_dw_database'
dbname_load = 'template_load_register'
dbuser = 'ashish'
dbpassword = '#Ash$&*sh2460#'
dbport = 5432

# Connecting to Databases:
conn_1 = psycopg2.connect(database=dbname_load, user=dbuser, password=dbpassword, host=dbhost, port=dbport) #connection register
conn_2 = psycopg2.connect(database=dbname_stage, user=dbuser, password=dbpassword, host=dbhost, port=dbport) #connection stage
conn_3 = psycopg2.connect(database=dbname_kri, user=dbuser, password=dbpassword, host=dbhost, port=dbport) #connection kri
conn_4 = psycopg2.connect(database=dbname_db, user=dbuser, password=dbpassword, host=dbhost, port=dbport) #connection stage

db_load_tables=[] #Empty Table list for pension_register
db_stage_tables=[] #Empty Table list for pension_stage
db_kri_tables=[] #Empty Table list for kri_dw_database
db_db_tables=[] #Empty Table list for pension_database

def fetch_template_load_register_table_list():
    #Creating a cursor object using the cursor() method
    cursor = conn_1.cursor()
    #Retrieving the list of tables:
    cursor.execute("""SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid();""")
    cursor.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'""")
    #Fetching all Table names from template_load_register
    for table in cursor.fetchall():
        table_name=str(table).replace('(','').replace(')','').replace(',','').replace("'",'')
        if table_name.startswith("submission_"):
            db_load_tables.append(table_name)
    #conn_1.close()
    print("Fetched Tables names from template_load_register DB.")

def fetch_pension_stage_table_list():
    #Creating a cursor object using the cursor() method
    cursor = conn_2.cursor()
    #Retrieving the list of tables:
    cursor.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'""")
    #Fetching all Table names from pension_stage
    for table in cursor.fetchall():
        table_name=str(table).replace('(','').replace(')','').replace(',','').replace("'",'')
        if table_name.startswith("Dataset_"):
            db_stage_tables.append(table_name)
    #conn_2.close()
    print("Fetched Tables names from pension_stage DB.")

def fetch_kri_dw_database_table_list():
    #Creating a cursor object using the cursor() method
    cursor = conn_3.cursor()
    #Retrieving the list of tables:
    cursor.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'""")
    #Fetching all Table names from kri_dw_database
    for table in cursor.fetchall():
        table_name=str(table).replace('(','').replace(')','').replace(',','').replace("'",'')
        db_kri_tables.append(table_name)
    #conn_3.close()
    print("Fetched Tables names from kri_dw_database DB.")

def fetch_pension_database_table_list():
    #Creating a cursor object using the cursor() method
    cursor = conn_4.cursor()
    #Retrieving the list of tables:
    cursor.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'""")
    #Fetching all Table names from pension_database
    for table in cursor.fetchall():
        table_name=str(table).replace('(','').replace(')','').replace(',','').replace("'",'')
        db_db_tables.append(table_name)
    #conn_4.close()
    print("Fetched Tables names from pension_database DB.")

# DROP TABLES function:
def drop_tables(conn,table_list):
    cursor = conn.cursor()
    try:
        for table in table_list:
            cursor.execute(f"drop table \"{table}\" cascade")  
    except:
        print("No Tables to Drop")
    conn.commit()
    
if __name__ == '__main__':

    print(
        """
        Type 1 : DROP TABLES FROM templates_load_register DB
        Type 2 : DROP TABLES FROM pension_stage DB
        Type 3 : DROP TABLES FROM kri_dw_database DB
        Type 4 : DROP TABLES FROM pension_database DB
        Type 5 : DROP ALL THE ABOVE 4 TABLES
        """
    )

    answer_1=input('Type Your Response: ')
    if answer_1 == 1 or answer_1 == '1' :
        answer_2=input("Do you Really want to DROP ALL TABLES FROM 'templates_load_register': Y or N ? ")
        if answer_2 == 'Y' or answer_2 == 'y' :
            fetch_template_load_register_table_list() #Fetch Tables
            print(db_load_tables)
            drop_tables(conn_1,db_load_tables) #drop tables from templates_load_register DB
            print("ALL Tables Dropped from 'templates_load_register'")
        elif answer_2 == 'N' or answer_2 == 'n' :
            print('No Selected, Exiting the Program Now ...')
        else:
            print('Invalid Input, Exiting the Program Now ... ')
    elif answer_1 == 2 or answer_1 == '2' :
        answer_2=input("Do you Really want to DROP ALL TABLES FROM 'pension_stage': Y or N ? ")
        if answer_2 == 'Y' or answer_2 == 'y' :
            fetch_pension_stage_table_list() #Fetch Tables
            print(db_stage_tables)
            drop_tables(conn_2,db_stage_tables) #drop tables from pension_stage DB
            print("ALL Tables Dropped from 'pension_stage'")
        elif answer_2 == 'N' or answer_2 == 'n' :
            print('No Selected, Exiting the Program Now ...')
        else:
            print('Invalid Input, Exiting the Program Now ... ')
    elif answer_1 == 3 or answer_1 == '3' :
        answer_2=input("Do you Really want to DROP ALL TABLES FROM 'kri_dw_database': Y or N ? ")
        if answer_2 == 'Y' or answer_2 == 'y' :
            fetch_kri_dw_database_table_list() #Fetch Tables
            print(db_kri_tables)
            drop_tables(conn_3,db_kri_tables) #drop tables from kri_dw_database DB
            print("ALL Tables Dropped from 'kri_dw_database'")
        elif answer_2 == 'N' or answer_2 == 'n' :
            print('No Selected, Exiting the Program Now ...')
        else:
            print('Invalid Input, Exiting the Program Now ... ')
    elif answer_1 == 4 or answer_1 == '4' :
        answer_2=input("Do you Really want to DROP ALL TABLES FROM 'pension_database': Y or N ? ")
        if answer_2 == 'Y' or answer_2 == 'y' :
            fetch_pension_database_table_list() #Fetch Tables
            print(db_db_tables)
            drop_tables(conn_4,db_db_tables) #drop tables from pension_database DB
            print("ALL Tables Dropped from 'pension_database'")
        elif answer_2 == 'N' or answer_2 == 'n' :
            print('No Selected, Exiting the Program Now ...')
        else:
            print('Invalid Input, Exiting the Program Now ... ')
    elif answer_1 == 5 or answer_1 == '5' :
        answer_2=input("Do you Really want to DROP ALL TABLES FROM 'template_load_register', 'pension_stage', 'kri_dw_database' and 'pension_database': Y or N ? ")
        if answer_2 == 'Y' or answer_2 == 'y' :
            fetch_template_load_register_table_list() #Fetch Tables
            drop_tables(conn_1,db_load_tables) #drop tables from templates_load_register DB
            print("ALL Tables Dropped from 'template_load_register'")
            fetch_pension_stage_table_list() #Fetch Tables
            drop_tables(conn_2,db_stage_tables) #drop tables from pension_stage DB
            print("ALL Tables Dropped from 'pension_stage'")
            fetch_kri_dw_database_table_list() #Fetch Tables
            drop_tables(conn_3,db_kri_tables) #drop tables from kri_dw_database DB
            print("ALL Tables Dropped from 'kri_dw_database'")
            fetch_pension_database_table_list() #Fetch Tables
            drop_tables(conn_4,db_db_tables) #drop tables from pension_database DB
            print("ALL Tables Dropped from 'pension_database'")
        elif answer_2 == 'N' or answer_2 == 'n' :
            print('No Selected, Exiting the Program Now ...')
        else:
            print('Invalid Input, Exiting the Program Now ... ')
    else:
        print('Invalid Input, Exiting the Program Now ... ')