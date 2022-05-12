#!/usr/bin/env python
# coding: utf-8

# ## DS 3002 - Data Project 2 (Course Capstone) // Jupyter Notebook Component
# Ashley Kim | 5/12/22

# ### CSV to JSON conversion and moving JSON files into Mongo DB Atlas

# In[113]:


import os
import json
import csv
import pymongo
from pymongo import MongoClient
import pandas as pd

import pymongo
from sqlalchemy import create_engine


# In[111]:


host_name = "localhost"
ports = {"mongo" : 27017, "mysql" : 3306}

user_id = "root"
pwd = "Password0!"

src_dbname = "ecommerce"
dst_dbname = "ecommerce_dw"


# In[110]:


def get_sql_dataframe(user_id, pwd, host_name, db_name, sql_query):
    # Create a connection to the MySQL database
    conn_str = f"mysql+pymysql://{user_id}:{pwd}@{host_name}/{db_name}"
    sqlEngine = create_engine(conn_str, pool_recycle=3600)
    
    # Invoke the pd.read_sql() function to query the database, and fill a Pandas DataFrame
    conn = sqlEngine.connect()
    dframe = pd.read_sql(sql_query, conn);
    conn.close()
    
    return dframe


def get_mongo_dataframe(user_id, pwd, host_name, port, db_name, collection, query):
    # Create a connection to MongoDB, with or without authentication credentials
    if user_id and pwd:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db_name)
        client = pymongo.MongoClient(mongo_uri)
    else:
        conn_str = f"mongodb://{host_name}:{port}/"
        client = pymongo.MongoClient(conn_str)
    
    # Query MongoDB, and fill a python list with documents to create a DataFrame
    db = client[db_name]
    dframe = pd.DataFrame(list(db[collection].find(query)))
    dframe.drop(['_id'], axis=1, inplace=True)
    client.close()
    
    return dframe
    
def set_dataframe(user_id, pwd, host_name, db_name, df, table_name, pk_column, db_operation):
    # Create a connection to the MySQL database
    conn_str = f"mysql+pymysql://{user_id}:{pwd}@{host_name}/{db_name}"
    sqlEngine = create_engine(conn_str, pool_recycle=3600)
    connection = sqlEngine.connect()
    
    # Invoke the Pandas DataFrame .to_sql( ) function to either create, or append to, a table
    if db_operation == "insert":
        df.to_sql(table_name, con=connection, index=False, if_exists='replace')
        sqlEngine.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({pk_column});")
            
    elif db_operation == "update":
        df.to_sql(table_name, con=connection, index=False, if_exists='append')
    
    connection.close()


# In[13]:


products = pd.read_csv('/Users/ashleykim/Documents/Y3 2021-2022/Spring 2022/DS 3002/data/products.csv')
products["price"] = pd.to_numeric(products["price"])

products.head(5)


# In[15]:


products.to_json('/Users/ashleykim/Documents/Y3 2021-2022/Spring 2022/DS 3002/data/products.json', orient = 'split', compression = 'infer')


# In[17]:


orders = pd.read_csv('/Users/ashleykim/Documents/Y3 2021-2022/Spring 2022/DS 3002/data/orders.csv')
orders["total_item_quantity"] = pd.to_numeric(orders["total_item_quantity"])
orders["purchase_revenue_in_usd"] = pd.to_numeric(orders["purchase_revenue_in_usd"])
orders["unique_items"] = pd.to_numeric(orders["unique_items"])

orders.head(5)


# In[18]:


orders.to_json('/Users/ashleykim/Documents/Y3 2021-2022/Spring 2022/DS 3002/data/orders.json', orient = 'split', compression = 'infer')


# In[20]:


users = pd.read_csv('/Users/ashleykim/Documents/Y3 2021-2022/Spring 2022/DS 3002/data/users.csv')

users.head(5)


# In[33]:


users.to_json('/Users/ashleykim/Documents/Y3 2021-2022/Spring 2022/DS 3002/data/users.json', orient = 'split', compression = 'infer')


# In[92]:


# Was having trouble with original code from class that was writing entire JSON file and every element
# into one complete list, so instead used the following code (from https://pythonexamples.org/python-csv-to-json/)

def csv_to_json(csvFilePath, jsonFilePath):
    jsonArray = []
      
    with open(r'/Users/ashleykim/Documents/Y3 2021-2022/Spring 2022/DS 3002/data/orders.csv', encoding='utf-8') as csvf: 

        csvReader = csv.DictReader(csvf) 

        for row in csvReader: 
            jsonArray.append(row)
  
    with open(r'/Users/ashleykim/Documents/Y3 2021-2022/Spring 2022/DS 3002/data/orders.json', 'w', encoding='utf-8') as jsonf: 
        jsonString = json.dumps(jsonArray, indent=4)
        jsonf.write(jsonString)
          
csvFilePath = r'/Users/ashleykim/Documents/Y3 2021-2022/Spring 2022/DS 3002/data/orders.csv'
jsonFilePath = r'/Users/ashleykim/Documents/Y3 2021-2022/Spring 2022/DS 3002/data/orders.json'
csv_to_json(csvFilePath, jsonFilePath)


# In[24]:


atlas_cluster_name = "sandbox"
atlas_dbname = "ecommerce_dw"
atlas_user_name = "ayk2ea"
atlas_password = "mongo"


# In[39]:


host_name = "localhost"
ports = {"mongo" : 27017}

user_id = "ayk2ea"
pwd = "mongo"

src_dbname = "ecommerce"
dst_dbname = "ecommerce_dw"


# In[40]:


def get_mongo_dataframe(user_id, pwd, host_name, port, db_name, collection, query):
    # Create a connection to MongoDB, with or without authentication credentials
    if user_id and pwd:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db_name)
        client = pymongo.MongoClient(mongo_uri)
    else:
        conn_str = f"mongodb://{host_name}:{port}/"
        client = pymongo.MongoClient(conn_str)
    
    # Query MongoDB, and fill a python list with documents to create a DataFrame
    db = client[db_name]
    dframe = pd.DataFrame(list(db[collection].find(query)))
    dframe.drop(['_id'], axis=1, inplace=True)
    client.close()
    
    return dframe


# #### Connecting to MongoDB Atlas and uploading json files to collection 'ecommerce'

# In[60]:


port = ports["mongo"]
conn_str = f"mongodb+srv://ayk2ea:mongo@sandbox.zzfhd.mongodb.net/ecommerce_dw?retryWrites=true&w=majority"
client = pymongo.MongoClient(conn_str)
db = client[src_dbname]

data_dir = os.path.join(os.getcwd(), 'data')

json_files = {"orders" : 'orders.json',
              "users" : 'users.json',
             "products" : 'products.json'}

for file in json_files:
    json_file = os.path.join(data_dir, json_files[file])
    with open(json_file, 'r') as openfile:
        json_object = json.load(openfile)
        file = db[file]
        result = file.insert_many(json_object)
        #print(f"{file} was successfully loaded.")

client.close()      

# just add columns to fact table here instead of on databricks


# ### Dimension Tables into mySQL (products + users from MongoDB)

# In[65]:


query = {}
port = ports["mongo"]
collection = "users"

df_users = get_mongo_dataframe(None, None, host_name, port, src_dbname, collection, query)


# In[66]:


query = {}
port = ports["mongo"]
collection = "products"

df_products = get_mongo_dataframe(None, None, host_name, port, src_dbname, collection, query)


# In[ ]:


exec_sql = f"CREATE DATABASE `{dst_dbname}`;"

conn_str = f"mysql+pymysql://{user_id}:{pwd}@{host_name}"
sqlEngine = create_engine(conn_str, pool_recycle=3600)
sqlEngine.execute(exec_sql) # create db
sqlEngine.execute("USE ecommerce_dw;") # select new db


# In[ ]:


dataframe = df_users
table_name = 'dim_users'
primary_key = 'user_key'
db_operation = "insert"

set_dataframe(user_id, pwd, host_name, dst_dbname, dataframe, table_name, primary_key, db_operation)


# In[ ]:


dataframe = df_products
table_name = 'dim_products'
primary_key = 'product_key'
db_operation = "insert"

set_dataframe(user_id, pwd, host_name, dst_dbname, dataframe, table_name, primary_key, db_operation)


# In[ ]:


sql_users = "SELECT * FROM ecommerce_dw.dim_users;"
df_dim_users = get_sql_dataframe(user_id, pwd, host_name, dst_dbname, sql_listings)
df_dim_users.head(5)


# In[ ]:


sql_reviews = "SELECT * FROM ecommerce_dw.dim_products;"
df_dim_products = get_sql_dataframe(user_id, pwd, host_name, dst_dbname, sql_reviews)
df_dim_products.head(5)


# ###  To attach needed columns and join with fact table the code is below
# Chose to do it here because I was running into technical errors trying to join in Databricks. The data below could also be exported as a json, uploaded to MongoDB Atlas using code from above and then displayed in Databricks using the display() after spark.read.

# In[125]:


query = {}
port = ports["mongo"]
collection = "orders"

df_orders = get_mongo_dataframe(None, None, host_name, port, src_dbname, collection, query)
df_orders.head(5)


# In[126]:


df_orders = pd.merge(df_orders, df_users, on=['email','email']) # joining users and orders based on email

df_orders.insert(0, 'date_key', range(20100101, 20100101 + len(df_orders))) # adding date key to connect to date dimension
df_orders.insert(0, "fact_purchase_order_key", range(1, df_orders.shape[0]+1)) # adding primary fact key
df_orders = df_orders.drop_duplicates(subset=['email']) # removing multiple instances here because events 
                                                        # can happen multiple times based on users interaction
                                                        # with the ecommerce site (just for visualizing purposes)
        
df_orders.head(5)


# In[114]:


dataframe = df_orders
table_name = 'fact_orders'
primary_key = 'fact_purchase_order_key'
db_operation = "insert"

set_dataframe(user_id, pwd, host_name, dst_dbname, dataframe, table_name, primary_key, db_operation)


# In[115]:


sql_purchase_orders = "SELECT * FROM ecommerce_dw.fact_orders;"

df_fact_purchase_orders = get_sql_dataframe(user_id, pwd, host_name, dst_dbname, sql_purchase_orders)
df_fact_purchase_orders.head(2)

