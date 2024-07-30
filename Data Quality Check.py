##### 0. Setup #####

# Import packages
import streamlit as st
import pandas as pd
import plotly.express as px
from snowflake.snowpark.context import get_active_session

# Get Snowflake session
session = get_active_session()
st.title('Data Quality Check 🔢')
st.write("\n\n")

##### 1. Filters #####

# Create filters
filter1, filter2, filter3 = st.columns(3)

## Database
with filter1:
    selected_db = st.text_input(
        label = 'Insert Database:',
        value = 'FROSTBYTE_TASTY_BYTES'
    )

## Schema
with filter2:
    selected_schema = st.text_input(
        label= 'Insert Schema:',
        value = 'RAW_POS'
    )

## Table
with filter3:
    selected_tb = st.text_input(
        label = 'Insert Table:',
        value = 'TRUCK'
    )


##### 2. Data Qaulity Summary #####

st.divider()
st.subheader('Column Overview')

# Query the unique and duplicate count view
if selected_db and selected_schema and selected_tb:
    col_names_query = '''
        SELECT COLUMN_NAME 
        FROM {0}.INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_CATALOG = '{0}' 
        AND TABLE_SCHEMA = '{1}' 
        AND TABLE_NAME = '{2}'
    '''.format(selected_db, selected_schema, selected_tb)
    
    df_col_names = session.sql(col_names_query).to_pandas()
    col_names = df_col_names['COLUMN_NAME'].tolist()
    
    # Create query to check for duplicates based on concatenated columns
    concat_cols = " || ".join(col_names)
    
    uni_dup_cnt_select_query = '''
        SELECT
          COUNT(DISTINCT {0}) AS UNIQUE_COUNT,
          COUNT(*) - COUNT(DISTINCT {0}) AS DUPLICATE_COUNT
        FROM {1}.{2}.{3}
    '''.format(concat_cols, selected_db, selected_schema, selected_tb)
else:
    uni_dup_cnt_select_query = '''
        SELECT
          0 AS UNIQUE_COUNT,
          0 AS DUPLICATE_COUNT
    '''

# Convert to DataFrame
df_uni_dup_cnt = session.sql(uni_dup_cnt_select_query).to_pandas()

# Create data quality summary visual
uni_cnt, dup_cnt = st.columns(2)

## Total cost in USD
with uni_cnt:
    st.metric(
        label = 'No. of Unqiue Records:',
        value = df_uni_dup_cnt['UNIQUE_COUNT'][0]
    )
    
## Total cost in credits
with dup_cnt:
    st.metric(
        label = 'No. of Duplicate Records:',
        value = df_uni_dup_cnt['DUPLICATE_COUNT'][0]
    )

# Query to get duplicated records
if selected_db and selected_schema and selected_tb:
    dup_records_query = '''
        SELECT *
        FROM {0}.{1}.{2}
        QUALIFY ROW_NUMBER() OVER(PARTITION BY {3} ORDER BY {3}) > 1
    '''.format(selected_db, selected_schema, selected_tb, concat_cols)

    df_dup_records = session.sql(dup_records_query).to_pandas()

    # Display duplicated records if any
    if not df_dup_records.empty:
        st.write('\n')
        st.dataframe(df_dup_records.sort_values(by = col_names), use_container_width = True)


##### 3. Load History #####

st.divider()
st.subheader('Table Load History')

# Successful load history query
if selected_db and selected_schema and selected_tb:
    load_hist_success_query ='''
        SELECT 
            CATALOG_ID,
            CATALOG_NAME,
            SCHEMA_ID,
            SCHEMA_NAME,
            TABLE_ID,
            TABLE_NAME,
            FILE_NAME,
            TO_CHAR(LAST_LOAD_TIME, 'YYYY-MM-DD') AS LAST_LOAD_TIME,
            STATUS,
            ROW_COUNT,
            ROW_PARSED
        FROM SNOWFLAKE.ACCOUNT_USAGE.LOAD_HISTORY
        WHERE CATALOG_NAME = '{}' 
        AND SCHEMA_NAME = '{}'
        AND TABLE_NAME = '{}'
        AND STATUS = 'LOADED'
        ORDER BY LAST_LOAD_TIME ASC
    '''.format(
        selected_db,
        selected_schema,
        selected_tb
    )
else:
    load_hist_success_query ='''
        SELECT 
            NULL AS CATALOG_ID,
            NULL AS CATALOG_NAME,
            NULL AS SCHEMA_ID,
            NULL AS SCHEMA_NAME,
            NULL AS TABLE_ID,
            NULL AS TABLE_NAME,
            NULL AS FILE_NAME,
            NULL AS LAST_LOAD_TIME,
            NULL AS STATUS,
            0 AS ROW_COUNT,
            0 AS ROW_PARSED
    '''

# Convert to DataFrame
df_load_hist_success = session.sql(load_hist_success_query).to_pandas()

# Create a Plotly bar chart
fig_load_hist_success = px.bar(df_load_hist_success, x = 'LAST_LOAD_TIME', y = 'ROW_PARSED', 
             labels = {'LAST_LOAD_TIME': 'Load Time', 'ROW_PARSED': 'No. of Rows Loaded'},
             title = 'Successful Load History')

fig_load_hist_success.update_layout(
    xaxis_title = '',
    yaxis_title = 'No. of Rows Loaded',
    bargap = 0.2,
    width = 800,
    height = 400,
    title = {
        'text': 'Successful Load History',
        'y':0.9,
        'x':0.5,
        'xanchor': 'center',
        'yanchor': 'top'
    }
)

fig_load_hist_success.update_xaxes(
    dtick = "D1",
    tickformat = '%Y-%m-%d'
)

# Display the Plotly chart in Streamlit
st.plotly_chart(fig_load_hist_success, use_container_width = True)

# All load history query
if selected_db and selected_schema and selected_tb:
    load_hist_all_query ='''
        SELECT 
            CATALOG_ID,
            CATALOG_NAME,
            SCHEMA_ID,
            SCHEMA_NAME,
            TABLE_ID,
            TABLE_NAME,
            FILE_NAME,
            LAST_LOAD_TIME,
            STATUS,
            ROW_COUNT,
            ROW_PARSED
        FROM SNOWFLAKE.ACCOUNT_USAGE.LOAD_HISTORY
        WHERE CATALOG_NAME = '{}' 
        AND SCHEMA_NAME = '{}'
        AND TABLE_NAME = '{}'
        ORDER BY LAST_LOAD_TIME DESC
    '''.format(
        selected_db,
        selected_schema,
        selected_tb
    )
else:
    load_hist_all_query ='''
        SELECT 
            NULL AS CATALOG_ID,
            NULL AS CATALOG_NAME,
            NULL AS SCHEMA_ID,
            NULL AS SCHEMA_NAME,
            NULL AS TABLE_ID,
            NULL AS TABLE_NAME,
            NULL AS FILE_NAME,
            NULL AS LAST_LOAD_TIME,
            NULL AS STATUS,
            0 AS ROW_COUNT,
            0 AS ROW_PARSED
    '''

# Convert to DataFrame
df_load_hist_all = session.sql(load_hist_all_query).to_pandas()

# Display all load history DataFrame
st.dataframe(df_load_hist_all, use_container_width = True)