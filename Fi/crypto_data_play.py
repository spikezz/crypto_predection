# -*- coding: utf-8 -*-
"""
Created on Sun Dec 12 21:18:40 2021

@author: spike
"""

import json
import sys
import pandas as pd
import matplotlib.pyplot as plt
from pprint import pprint
from sqlite3 import connect

eth_data_path="data/price-eth-24h.json"
btc_data_path="data/price-btc-24h.json"

with open(eth_data_path) as json_file:
    eth_list=json.loads(json_file.read())
    df_eth = pd.DataFrame(data=eth_list)
    
with open(btc_data_path) as json_file:
    btc_list=json.loads(json_file.read())
    df_btc = pd.DataFrame(data=btc_list)

def sync_data(df_1,df_2,df_1_name,df_2_name):
    
    conn = connect(':memory:')
    df_1.to_sql(df_1_name+"_data", conn)
    df_2.to_sql(df_2_name+"_data", conn)
    sql=f'''
        WITH unioned_table AS (
            SELECT 
                t,
                v,
                "{df_1_name+"_data"}" AS datasource
            FROM
                {df_1_name+"_data"}
            UNION ALL
            SELECT 
                t,
                v,
                "{df_2_name+"_data"}" AS datasource
            FROM
                {df_2_name+"_data"}
            ORDER BY t ASC
        ),
        
        stacked_table AS (
            SELECT
                t,
                IIF(datasource = "{df_1_name+"_data"}", v, NULL) AS {df_1_name}_price,
                IIF(datasource = "{df_2_name+"_data"}", v, NULL) AS {df_2_name}_price,
                datasource
            FROM
                unioned_table
        ),
        
        sync_table AS (
            SELECT
                t,
                {df_1_name}_price,
                {df_2_name}_price,
                SUM(CASE 
                        WHEN 
                            {df_1_name}_price IS NULL 
                        THEN 0 
                        ELSE 1 
                    END) 
                OVER (ORDER BY t) AS {df_1_name}_sync_partition,
                SUM(CASE 
                        WHEN 
                            {df_2_name}_price IS NULL 
                        THEN 0 
                        ELSE 1 
                    END) 
                OVER (ORDER BY t) AS {df_2_name}_sync_partition,
                datasource
            FROM
                stacked_table
        ),
        
        filtered_table AS (
            SELECT 
                t,
                FIRST_VALUE({df_1_name}_price) 
                    OVER (PARTITION BY {df_1_name}_sync_partition ORDER BY t) AS {df_1_name}_price,
                LAST_VALUE({df_2_name}_price) 
                    OVER (PARTITION BY {df_2_name}_sync_partition ORDER BY t) AS {df_2_name}_price,
                datasource
            FROM
                sync_table
            WINDOW time_window AS (
                ORDER BY t ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) 
        )
        
        SELECT
            *
        FROM 
            filtered_table

        '''
    table=pd.read_sql(sql, conn)
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
        print(table)
    return table

def plot_data(table):
    pass
    fig,ax=plt.subplots(1,1,figsize=(12,6))
    table.plot(ax=ax)


table=sync_data(df_eth,df_btc,"eth","btc")
plot_data(table)