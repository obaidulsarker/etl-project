import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import os
import datetime
import sys

# Read Config from env file
import environ

env = environ.Env()
environ.Env.read_env()

dbhost = env('DB_SERVER')
dbport = env('DB_PORT')
dbname = env('DB_NAME_STG')
dbname_reg = env('DB_NAME_REG')
dbname_load = env('DB_NAME_LOAD')
dbuser = env('DB_USER')
dbpassword = env('DB_PASSWORD')
log_path_register_to_stage=env('LOG_PATH_REGISTER_TO_STAGE')

# # Database Credentials:
# dbhost = '10.0.0.7'
# dbname_stage = 'pension_stage'
# dbname_reg = 'partner_register'
# dbuser = 'ashish'
# dbpassword = '#Ash$&*sh2460#'
# dbport = 5432

#Log Functionality:
class Logger(object):
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open(f"{log_path_register_to_stage}register_to_stage_{datetime.datetime.now()}.log", "a")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)  

    def flush(self):
        # this flush method is needed for python 3 compatibility.
        # this handles the flush command by doing nothing.
        # you might want to specify some extra behavior here.
        pass

# Connecting to Databases:
chunk=10000 #setting the chunk size
path='Partner_Register_Data/' #local location for storing the pension_register DB data (if needed)
conn = psycopg2.connect(database=dbname_reg, user=dbuser, password=dbpassword, host=dbhost, port=dbport) #connection register
conn_ = psycopg2.connect(database=dbname, user=dbuser, password=dbpassword, host=dbhost, port=dbport) #connection stage

selected_partner_register_tables=[] #Empty Table list for pension_register
selected_partner_stage_tables=[] #Empty Table list for pension_stage

def fetch_pension_register_table_list():
    #Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    #Retrieving the list of tables:
    cursor.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'""")
    #Fetching all Table names from pension_register
    for table in cursor.fetchall():
        table_name=str(table).replace('(','').replace(')','').replace(',','').replace("'",'')
        if table_name.startswith("schemes_"):
            selected_partner_register_tables.append(table_name)
    #conn.close()
    print("Fetched 'scheme_' Prefix Tables from Pension_Register DB...")
    #print(selected_partner_register_tables)

def fetch_pension_stage_table_list():
    #Creating a cursor object using the cursor() method
    cursor = conn_.cursor()
    #Retrieving the list of tables:
    cursor.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'""")
    #Fetching all Table names from pension_stage
    for table in cursor.fetchall():
        table_name=str(table).replace('(','').replace(')','').replace(',','').replace("'",'')
        if table_name.startswith("schemes_"):
            selected_partner_stage_tables.append(table_name)
    #conn_.close()
    print("Fetched 'scheme_' Prefix Tables from Pension_Stage DB...")
    #print(selected_partner_stage_tables)
        
def add_tables_to_stage():
    # Create an engine instance
    pension_register_engine = create_engine(f'postgresql://{dbuser}:{dbpassword}@{dbhost}/{dbname_reg}'); #partner_reg
    pension_stage_engine = create_engine(f'postgresql://{dbuser}:{dbpassword}@{dbhost}/{dbname}')
    # Connect to PostgreSQL server
    dbConnection = pension_register_engine.connect();
    print("Adding Tables to Pension_Stage DB:")
    for table in selected_partner_register_tables:
        # Read data from PostgreSQL database table and load into a DataFrame instance
        df_temp=pd.read_sql(f"select * from {table}", dbConnection)
        #df_temp.to_csv(path+table+'.csv',index=False) # To add files as CSV inside a folder
        try:
            df_temp.to_sql(table, con = pension_stage_engine, chunksize=chunk, method='multi', index=False, if_exists='append')
            print(f"{table} Table, Updated.")
        except:
            df_fetched=pd.read_sql(f"select * from \"{table}\"", con=pension_stage_engine)
            # df_fetched.columns=df_fetched.columns.str.strip() # Remove Whitespace from Column Names
            # df_fetched.columns=df_fetched.columns.str.lower() # Changing all column names to lower case
            # df_fetched.columns=df_fetched.columns.str.replace('"','')
            df_temp=pd.concat([df_fetched,df_temp], axis=0, ignore_index=True)
            df_temp.to_sql(table, con = pension_stage_engine, chunksize=chunk, method='multi', index=False, if_exists='replace')
            print(f"{table} Table, Fetched and Updated.")

    # Print the List of Extracted Tables:
    print('List of Extracted Tables:',selected_partner_register_tables)
    # Close the database connection
    dbConnection.close();

def drop_scheme_prefix_tables():
    try:
        cursor = conn_.cursor()
        for table in selected_partner_stage_tables:
            cursor.execute(f"drop table {table}")  
        conn_.commit()
        #conn_.close()
    except:
        print("No Tables to Drop with 'schemes_' prefix")

def main():
    sys.stdout = Logger()
    
    # pension_register DB:
    fetch_pension_register_table_list() #Fetching the filtered pension_register table list
    #print(selected_partner_register_tables) #printing partner_register tables

    # pension_stage DB:
    fetch_pension_stage_table_list() #Fetching the filtered pension_stage table list
    #print(selected_partner_stage_tables) #printing filtered pension_stage tables

    add_tables_to_stage() #Adding tables to pension_stage DB
    print('Table Data Appended without DROP')


if __name__ == '__main__':
    main()
    # print('\n\n')
    # answer=input('Would you like to DROP the "schemes_" Prefix Tables from partner_stage || Y or N: ')

    # if answer == 'Y' or answer == 'y' :
    #     drop_scheme_prefix_tables() #drop tables starting with "scheme_" prefix from "pension_stage" DB
    #     print('Tables Dropped...')
    #     add_tables_to_stage() #Adding tables to pension_stage DB
    #     print('New Tables Added by DROPPING old tables.')
    # elif answer == 'N' or answer == 'n':
    #     add_tables_to_stage() #Adding tables to pension_stage DB
    #     print('Table Data Appended without DROP')
    # else:
    #     print('Invalid Input, Restarting the Program Again... ')
    #     print('\n')
    #     os.execl(sys.executable, sys.executable, *sys.argv)
