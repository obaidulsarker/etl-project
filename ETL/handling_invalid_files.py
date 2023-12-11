import os, glob
import pandas as pd
from numpy import nan
import psycopg2
import re
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

directory='Invalid_Data'
latest_folder=max(glob.glob(os.path.join(directory, '*/')), key=os.path.getmtime)
#print(latest_folder)

files_list_with_path=[]
for path, subdirs, files in os.walk(latest_folder):
    for file in files:
        #print(file)
        if (file.endswith('.xlsx') or file.endswith('.xls') or file.endswith('.XLS')):
            temp=os.path.join(path, file)
            files_list_with_path.append(temp)

print(files_list_with_path)

conn_imp = psycopg2.connect(database=dbname_load, user=dbuser, password=dbpassword, host=dbhost, port=dbport)
# Get submission_templates table into a dataframe:
imp_fields=pd.read_sql(f"select * from mandatory_fields", con=conn_imp) #fetching from template_load_register DB

for index,excel in enumerate(files_list_with_path):
    file_name_tmp=excel.split("/")[-1]
    regex='^(.+?)-'
    file_name=re.findall(regex,file_name_tmp)
    print(file_name)
    df=pd.read_excel(excel,engine='openpyxl')
    df_col_list=df.columns.to_list()
    #print(df_col_list)
    df_comp=imp_fields.loc[(imp_fields['dataset_code_id'] == file_name[0]) & \
                           (imp_fields['validation for submission deadline'] == 'mandatory field')]
    important_columns=df_comp['datafield name'].to_list()
    if not important_columns:
        important_columns=['reportcode','entity_id','entityname','referenceperiodyear','referenceperiod']
    #print(important_columns)
    try:
        df['NULL_VALUES']=df[important_columns].isnull().sum(axis=1)
    except:
        missing_cols=list(set(important_columns).difference(df_col_list))
        #print('Missing_Cols:',missing_cols)
        for i in missing_cols:
            df[i]=nan
        df['NULL_VALUES']=df[important_columns].isnull().sum(axis=1)

    df.to_excel(f"Data_Upload/{file_name[0]}-{index}.xlsx",index=False)
    print(f"{file_name}: Edited and Moved to Local Folder.")