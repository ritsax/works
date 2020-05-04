# -*- coding: utf-8 -*-
"""
Created on Sat May  2 02:02:11 2020

@author: ritsa
"""

import json
import requests
from datetime import datetime


# Load JSON data
with open('orders.json') as f:
  namaste_dataset = json.load(f)
  

# Add currency conversion to each record
# currency rate = CAD : USD for the particular order date

for rows in namaste_dataset:
    order_date = datetime.strptime(rows['created_at'],"%Y-%m-%dT%H:%M:%SZ").date().strftime("%Y-%m-%d")    
    response = requests.get("https://api.exchangeratesapi.io/" + order_date + "?symbols=USD,CAD").text
    order_dict = json.loads(response)
    rows["currency_rate"] = order_dict["rates"]["CAD"]/order_dict["rates"]["USD"]
    

# Creating a new database in PSQL
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
con = psycopg2.connect(user = "postgres", password = "abcd@1234", host = "127.0.0.1", port = "5432")
con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT);


# Create db namaste
cursor = con.cursor()
dbname = "namaste"
sqlcreatedb = "create database " + dbname
cursor.execute(sqlcreatedb)
cursor.close()


# Connect to new namaste database
con_namaste = psycopg2.connect(dbname = "namaste", user = "postgres", password = "abcd@1234", host = "127.0.0.1", port = "5432")
cursor_namaste = con_namaste.cursor()


# Create tables customer, product and order in namaste db
create_customer_table = '''CREATE TABLE customer (id INT PRIMARY KEY NOT NULL, name VARCHAR(50) NOT NULL, email VARCHAR(50)); '''
create_product_table = '''CREATE TABLE product (id INT PRIMARY KEY NOT NULL, product_id INT NOT NULL, product_sku VARCHAR(50), product_name VARCHAR(50), price INT); '''
create_order_table = '''CREATE TABLE orders (
                                            created_at VARCHAR(20), 
                                            currency_rate FLOAT8, 
                                            cust_id VARCHAR(50) NOT NULL, 
                                            id BIGINT NOT NULL, 
                                            line_id INT NOT NULL, 
                                            total_price FLOAT8,
                                            PRIMARY KEY (id,cust_id,line_id)); '''

cursor_namaste.execute("DROP TABLE IF EXISTS customer")
cursor_namaste.execute(create_customer_table)
cursor_namaste.execute("DROP TABLE IF EXISTS product")
cursor_namaste.execute(create_product_table)
cursor_namaste.execute("DROP TABLE IF EXISTS orders")
cursor_namaste.execute(create_order_table)
con_namaste.commit()



#Remove duplicates from customer
cust = [dict(t) for t in {tuple(rows['customer'].items()) for rows in namaste_dataset}]
from psycopg2.extensions import AsIs
columns = namaste_dataset[0]['customer'].keys()
insert_statement = 'INSERT INTO customer (%s) VALUES %s;'

#Remove duplicates from products
product_lines = []
for rows in namaste_dataset:
    for lines in rows['line_items']:
        product_lines += [lines]

p_lines=[dict(m) for m in {tuple(rows.items()) for rows in product_lines}]        
columns_prod = p_lines[0].keys()
insert_statement_prod = 'INSERT INTO product (%s) VALUES %s;'


#create a list of orders
orders= []  
for rows in namaste_dataset:
    for lines in rows['line_items']:
        orders.append(dict({'created_at':rows['created_at'], 'currency_rate' : rows['currency_rate'], 'cust_id': rows['customer']['id'], 'id': rows['id'], 'line_id':lines['id'], 'total_price': rows['total_price']}))
columns_orders = orders[0].keys()
insert_statement_orders = 'INSERT INTO orders (%s) VALUES %s;'




# insert rows into customer table
for rows in cust:
    cursor_namaste.execute(insert_statement, (AsIs(','.join(columns)), tuple(rows.values())))
    con_namaste.commit()

# insert rows into product table
for rows in p_lines:
    cursor_namaste.execute(insert_statement_prod, (AsIs(','.join(columns_orders)), tuple(rows.values())))
    con_namaste.commit()
    

#insert rows into orders table
for rows in orders:
    cursor_namaste.execute(insert_statement_orders, (AsIs(','.join(columns_orders)), tuple(rows.values())))
    con_namaste.commit()


cursor_namaste.close()
