import pandas as pd
from sqlalchemy import create_engine
#from postgreSQL_config import *

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

df=pd.read_excel('Mandatory_Fields_Data/Submission_Datafields_Inventory_Validation_Rules_20230426_v023_NEW.xlsx',\
                 header=1,sheet_name='NORMALIZED_DATAFIELD_Invenvtory')
df.columns=df.columns.str.strip() # Remove Whitespace from Column Names
df.columns=df.columns.str.lower() # Changing all column names to lower case

cols = ['dataset_code_id','datafield name','validation for submission deadline']
cols_ = ['datafield name','validation for submission deadline']
df[cols] = df[cols].apply(lambda x: x.str.strip())
df[cols_] = df[cols_].apply(lambda x: x.str.lower())

filtered=df[['dataset_code_id','datafield name','validation for submission deadline']]
filtered=filtered.loc[filtered['validation for submission deadline']=='mandatory field']
filtered=filtered.reset_index()
filtered=filtered.drop(['index'], axis=1)
#filtered
print('Table Filtered')

engine_ = create_engine(f'postgresql://{dbuser}:{dbpassword}@{dbhost}/{dbname_load}'); #template_load_register
#filtered.to_csv('mandatory_fields.csv',index=False)
filtered.to_sql('mandatory_fields', con = engine_, index=False, if_exists='replace')
print('Table added to Postgre DB.')