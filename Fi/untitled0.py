# -*- coding: utf-8 -*-
"""
Created on Mon Dec 13 23:28:15 2021

@author: spike
"""

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
    
    filtered_table AS (
        SELECT 
            t,
            IIF(datasource = "{df_1_name+"_data"}", {df_1_name}_price, LAG({df_1_name}_price,1) OVER time_window) AS {df_1_name}_price,
            IIF(datasource = "{df_2_name+"_data"}", {df_2_name}_price, LAG({df_2_name}_price,1) OVER time_window) AS {df_2_name}_price,
            datasource
        FROM
            stacked_table
        WINDOW time_window AS (
            ORDER BY t ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) 
    )
    
    SELECT
        *
    FROM 
        filtered_table

    '''