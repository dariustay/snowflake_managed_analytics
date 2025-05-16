##### 0. Setup #####

# Import packages
import pytz
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
from snowflake.snowpark.context import get_active_session

# Get Snowflake session
session = get_active_session()
st.title('Cost Monitoring Dashboard 📊')
st.write("\n\n")

##### 1. Filters #####

# Accounts query
acc_query = '''
    SELECT DISTINCT(ACCOUNT_NAME) 
    FROM SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY
'''

# Convert to DataFrame and extract accounts
df_acc = session.sql(acc_query).to_pandas()
acc_list = sorted(df_acc['ACCOUNT_NAME'].tolist())

# Define default date range (i.e., previous 1 week)
sg_timezone = pytz.timezone('Asia/Singapore')
default_end_date = datetime.now(sg_timezone).date()
default_start_date = default_end_date - timedelta(days = 7)

# Service types query
service_query = '''
    SELECT DISTINCT(SERVICE_TYPE) 
    FROM SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY
'''

# Convert to DataFrame and extract service types
df_service = session.sql(service_query).to_pandas()
service_list = sorted(df_service['SERVICE_TYPE'].tolist())

# Create filters
filter1, filter2, filter3 = st.columns(3)

## Accounts
with filter1:
    selected_acc = st.multiselect(
        label = 'Select Accounts:', 
        options = acc_list,
        default = acc_list
    )

## Date range
with filter2:
    selected_date = st.date_input(
        label= 'Select Date Range:', 
        value = [default_start_date, default_end_date]
    )

## Service types
with filter3:
    selected_service = st.multiselect(
        label = 'Select Service Types:', 
        options = service_list,
        default = service_list
    )


##### 2. Overall Cost #####

st.divider()
st.subheader('Overall Cost')

# Quote each account name
quoted_selected_acc = ["'{}'".format(acc) for acc in selected_acc]

# Quote each service type
quoted_selected_service = ["'{}'".format(service) for service in selected_service]

# Extract start and end dates from selected_date
start_date, end_date = selected_date
start_date_str = start_date.strftime('%Y-%m-%d')
end_date_str = end_date.strftime('%Y-%m-%d')

# Total cost query
if quoted_selected_acc and quoted_selected_service:
    cost_query = '''
        SELECT 
            SUM(USAGE_IN_CURRENCY) AS TOTAL_COST_USD,
            SUM(USAGE) AS TOTAL_COST_CREDITS,
            AVG(USAGE_IN_CURRENCY) AS AVG_COST_USD,
            AVG(USAGE) AS AVG_COST_CREDITS,
        FROM SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY 
        WHERE ACCOUNT_NAME IN ({})
        AND SERVICE_TYPE IN ({})
        AND USAGE_DATE BETWEEN '{}' AND '{}'
    '''.format(
        ', '.join(quoted_selected_acc),
        ', '.join(quoted_selected_service),
        start_date_str,
        end_date_str
    )
else:
    cost_query = '''
        SELECT 
            0 AS TOTAL_COST_USD,
            0 AS TOTAL_COST_CREDITS,
            0 AS AVG_COST_USD,
            0 AS AVG_COST_CREDITS
    '''

# Convert to DataFrame
df_cost = session.sql(cost_query).to_pandas()

# Extract total cost (USD)
total_cost_usd = df_cost['TOTAL_COST_USD'].iloc[0]
formatted_total_cost_usd = "${:,.2f}".format(total_cost_usd)

# Extract total cost (credits)
total_cost_credits = df_cost['TOTAL_COST_CREDITS'].iloc[0]
formatted_total_cost_credits = "{:,.2f}".format(total_cost_credits)

# Extract total cost (USD)
avg_cost_usd = df_cost['AVG_COST_USD'].iloc[0]
formatted_avg_cost_usd = "${:,.2f}".format(avg_cost_usd)

# Extract total cost (credits)
avg_cost_credits = df_cost['AVG_COST_CREDITS'].iloc[0]
formatted_avg_cost_credits = "{:,.2f}".format(avg_cost_credits)

# Remaining credits query 
remaining_credits_query = '''
    SELECT 
        DATE, 
        FREE_USAGE_BALANCE + CAPACITY_BALANCE AS TOTAL_BALANCE
    FROM SNOWFLAKE.ORGANIZATION_USAGE.REMAINING_BALANCE_DAILY
    ORDER BY DATE DESC
    LIMIT 1
'''

# Convert to DataFrame
df_remaining_credits = session.sql(remaining_credits_query).to_pandas()
if df_remaining_credits.empty:
    df_remaining_credits = pd.DataFrame({
        'DATE': [default_end_date], 
        'TOTAL_BALANCE': [0]
    })

# Extract remaining credits (USD)
remaining_credits_usd = df_remaining_credits['TOTAL_BALANCE'].iloc[0]
formatted_remaining_credits_usd = "${:,.2f}".format(remaining_credits_usd)

# Create tabs
overall_cost_tab1, overall_cost_tab2 = st.tabs(["Overall Cost", "Remaining Credits"])

# Create overall cost visual
with overall_cost_tab1:
    cost1, cost2, cost3, cost4 = st.columns(4)
    
    ## Total cost in USD
    with cost1:
        st.metric(
            label = 'Total Cost in USD:',
            value = formatted_total_cost_usd
        )
        
    ## Total cost in credits
    with cost2:
        st.metric(
            label = 'Total Cost in Credits:',
            value = formatted_total_cost_credits
        )
    
    ## Average cost in USD
    with cost3:
        st.metric(
            label = 'Avg Daily Cost in USD:',
            value = formatted_avg_cost_usd
        )
        
    ## Average cost in credits
    with cost4:
        st.metric(
            label = 'Avg Daily Cost in Credits:',
            value = formatted_avg_cost_credits
        )

# Create remaining credits visual
with overall_cost_tab2:
    st.metric(
        label = 'Remaining Credits in USD:',
        value = formatted_remaining_credits_usd
    )


##### 3. Graphs #####

### 3.1 Daily / Monthly Cost ###

st.write('\n')
st.write('\n')
st.write('\n')
st.subheader('Daily / Monthly Cost')

# Daily cost table query (for forecast model training)
if quoted_selected_acc and quoted_selected_service:
    daily_cost_table_train_query = '''
        CREATE OR REPLACE TABLE MANAGED_ANALYTICS.STREAMLIT_APP.ORG_DAILY_COST_TRAIN AS (
            SELECT 
                TO_TIMESTAMP_NTZ(USAGE_DATE) AS USAGE_DATE,
                SUM(USAGE_IN_CURRENCY) AS TOTAL_COST_USD
            FROM SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY 
            WHERE ACCOUNT_NAME IN ({})
            AND SERVICE_TYPE IN ({})
            GROUP BY USAGE_DATE
            ORDER BY USAGE_DATE ASC
        )
    '''.format(
        ', '.join(quoted_selected_acc),
        ', '.join(quoted_selected_service)
    )
else:
    daily_cost_table_train_query = '''
        CREATE OR REPLACE TABLE MANAGED_ANALYTICS.STREAMLIT_APP.ORG_DAILY_COST_TRAIN AS (
            SELECT 
                TO_TIMESTAMP_NTZ(USAGE_DATE) AS USAGE_DATE,
                0 AS TOTAL_COST_USD
            FROM SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY
            GROUP BY USAGE_DATE
            ORDER BY USAGE_DATE ASC
        )
    '''

# Execute the query
session.sql(daily_cost_table_train_query).collect()

# Daily cost table to be displayed based on data range
daily_cost_display_query = '''
    CREATE OR REPLACE TABLE MANAGED_ANALYTICS.STREAMLIT_APP.ORG_DAILY_COST_DISPLAY AS (
        SELECT * 
        FROM MANAGED_ANALYTICS.STREAMLIT_APP.ORG_DAILY_COST_TRAIN
        WHERE USAGE_DATE BETWEEN '{}' AND '{}'
    )
'''.format(
    start_date_str,
    end_date_str
)

# Execute the query
session.sql(daily_cost_display_query).collect()

# Forecast model and predictions table queries
create_forecast_query = '''
    CREATE OR REPLACE SNOWFLAKE.ML.FORECAST MANAGED_ANALYTICS.STREAMLIT_APP.ORG_DAILY_COST_FORECAST (
        INPUT_DATA => TABLE(MANAGED_ANALYTICS.STREAMLIT_APP.ORG_DAILY_COST_TRAIN),
        TIMESTAMP_COLNAME => 'USAGE_DATE',
        TARGET_COLNAME => 'TOTAL_COST_USD'
    )
'''

call_forecast_query = '''
    CALL MANAGED_ANALYTICS.STREAMLIT_APP.ORG_DAILY_COST_FORECAST!FORECAST(FORECASTING_PERIODS => 7)
'''

create_predictions_query = '''
    CREATE OR REPLACE TABLE MANAGED_ANALYTICS.STREAMLIT_APP.ORG_DAILY_COST_PREDICTIONS AS (
        SELECT *
        FROM TABLE(RESULT_SCAN(-1))
    )
'''

# Execute the queries 
session.sql(create_forecast_query).collect()
session.sql(call_forecast_query).collect()
session.sql(create_predictions_query).collect()

# Query to combine the daily cost combined tables
daily_cost_combined_query = '''
    SELECT
        USAGE_DATE,
        TOTAL_COST_USD,
        'Actual' AS DATA_TYPE
    FROM MANAGED_ANALYTICS.STREAMLIT_APP.ORG_DAILY_COST_DISPLAY
    UNION
    SELECT
        TS AS USAGE_DATE,
        FORECAST AS TOTAL_COST_USD,
        'Forecast' AS DATA_TYPE
    FROM MANAGED_ANALYTICS.STREAMLIT_APP.ORG_DAILY_COST_PREDICTIONS
    ORDER BY USAGE_DATE ASC
'''

# Convert to DataFrame
df_daily_cost_combined = session.sql(daily_cost_combined_query).to_pandas()

# Clip / Zero out negative predictions
df_daily_cost_combined['TOTAL_COST_USD'] = df_daily_cost_combined['TOTAL_COST_USD'].clip(lower = 0)

# Create a Plotly bar chart
fig_daily_cost_combined = px.bar(df_daily_cost_combined, x = 'USAGE_DATE', y = 'TOTAL_COST_USD', color = 'DATA_TYPE', 
                    labels = {'USAGE_DATE': 'Usage Date', 'TOTAL_COST_USD': 'Total Cost (USD)', 'DATA_TYPE': 'Data Type'},
                    title = 'Daily Cost in USD',  color_discrete_map = {'Forecast': '#FFD700'})

fig_daily_cost_combined.update_layout(
    xaxis_title = '',
    yaxis_title = '',
    bargap = 0.2,
    width = 800,
    height = 400,
    title = {
        'text': 'Daily Cost in USD',
        'y': 0.9,
        'x': 0.5,
        'xanchor': 'center',
        'yanchor': 'top'
    }
)

fig_daily_cost_combined.update_xaxes(
    dtick = "D1",
    tickformat = '%Y-%m-%d'
)

# Monthly cost table query
if quoted_selected_acc and quoted_selected_service:
    monthly_cost_table_query = '''
        CREATE OR REPLACE TABLE MANAGED_ANALYTICS.STREAMLIT_APP.ORG_MONTHLY_COST AS (
            SELECT 
                TO_CHAR(USAGE_DATE, 'YYYY-MM') AS USAGE_MONTH,
                SUM(USAGE_IN_CURRENCY) AS TOTAL_COST_USD
            FROM SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY 
            WHERE ACCOUNT_NAME IN ({})
            AND SERVICE_TYPE IN ({})
            AND USAGE_DATE BETWEEN '{}' AND '{}'
            GROUP BY USAGE_MONTH
            ORDER BY USAGE_MONTH ASC
        )
    '''.format(
        ', '.join(quoted_selected_acc),
        ', '.join(quoted_selected_service),
        start_date_str,
        end_date_str
    )
else:
    monthly_cost_table_query = '''
        CREATE OR REPLACE TABLE MANAGED_ANALYTICS.STREAMLIT_APP.ORG_MONTHLY_COST AS (
            SELECT
                TO_CHAR(USAGE_DATE, 'YYYY-MM') AS USAGE_MONTH,
                0 AS TOTAL_COST_USD
            FROM SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY 
            WHERE USAGE_DATE BETWEEN '{}' AND '{}'
            GROUP BY USAGE_MONTH
            ORDER BY USAGE_MONTH ASC
        )
    '''.format(
        start_date_str,
        end_date_str
    )

# Execute the query
session.sql(monthly_cost_table_query).collect()

# Query the daily cost table
monthly_cost_query = '''
    SELECT * FROM MANAGED_ANALYTICS.STREAMLIT_APP.ORG_MONTHLY_COST
'''

# Convert to DataFrame
df_monthly_cost = session.sql(monthly_cost_query).to_pandas()

# Create a Plotly bar chart
fig_monthly_cost = px.bar(df_monthly_cost, x = 'USAGE_MONTH', y = 'TOTAL_COST_USD', 
             labels = {'USAGE_MONTH': 'Usage Month', 'TOTAL_COST_USD': 'Total Cost (USD)'},
             title = 'Monthly Cost in USD')

fig_monthly_cost.update_layout(
    xaxis_title = '',
    yaxis_title = '',
    bargap = 0.2,
    width = 800,
    height = 400,
    title = {
        'text': 'Monthly Cost in USD',
        'y':0.9,
        'x':0.5,
        'xanchor': 'center',
        'yanchor': 'top'
    }
)

fig_monthly_cost.update_xaxes(
    dtick = "M1",
    tickformat = '%Y-%m'
)

# Create tabs
daily_cost_tab, monthly_cost_tab = st.tabs(["Cost by Day", "Cost by Month"])

# Display the Plotly chart in Streamlit
with daily_cost_tab:
    st.plotly_chart(fig_daily_cost_combined, use_container_width = True)

with monthly_cost_tab:
    st.write("📣 Note: Select the entire month for the date range filter to view the full result.")
    st.plotly_chart(fig_monthly_cost, use_container_width = True)


### 3.2 Cost by Account / Service Type ###

st.subheader('Cost Breakdown')

# Cost by account query
if quoted_selected_acc and quoted_selected_service:
    cost_acc_query = '''
        SELECT 
            ACCOUNT_NAME,
            SUM(USAGE_IN_CURRENCY) AS TOTAL_COST_USD
        FROM SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY 
        WHERE ACCOUNT_NAME IN ({})
        AND SERVICE_TYPE IN ({})
        AND USAGE_DATE BETWEEN '{}' AND '{}'
        GROUP BY ACCOUNT_NAME
        ORDER BY TOTAL_COST_USD DESC
    '''.format(
        ', '.join(quoted_selected_acc),
        ', '.join(quoted_selected_service),
        start_date_str,
        end_date_str
    )
else:
    cost_acc_query = '''
        SELECT 
            NULL AS ACCOUNT_NAME,
            0 AS TOTAL_COST_USD
    '''

# Cost by service type query
if quoted_selected_acc and quoted_selected_service:
    cost_service_query = '''
        SELECT 
            SERVICE_TYPE,
            SUM(USAGE_IN_CURRENCY) AS TOTAL_COST_USD
        FROM SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY 
        WHERE ACCOUNT_NAME IN ({})
        AND SERVICE_TYPE IN ({})
        AND USAGE_DATE BETWEEN '{}' AND '{}'
        GROUP BY SERVICE_TYPE
        ORDER BY TOTAL_COST_USD DESC
    '''.format(
        ', '.join(quoted_selected_acc),
        ', '.join(quoted_selected_service),
        start_date_str,
        end_date_str
    )
else:
    cost_service_query = '''
        SELECT 
            NULL AS SERVICE_TYPE,
            0 AS TOTAL_COST_USD
    '''

# Convert to DataFrame
df_cost_acc = session.sql(cost_acc_query).to_pandas()
df_cost_service = session.sql(cost_service_query).to_pandas()

# Create the 1st Plotly pie chart: Cost by account
fig_acc = px.pie(df_cost_acc, values = 'TOTAL_COST_USD', names = 'ACCOUNT_NAME', 
             title = 'Cost by Account')

fig_acc.update_layout(
    title = {
        'text': 'Cost by Account',
        'y':0.9,
        'x':0.5,
        'xanchor': 'center',
        'yanchor': 'top'
    }
)

# Create the 2nd Plotly pie chart: Cost by service type
fig_service = px.pie(df_cost_service, values = 'TOTAL_COST_USD', names = 'SERVICE_TYPE', 
                     title = 'Cost by Service Type')

fig_service.update_layout(
    title = {
        'text': 'Cost by Service Type',
        'y':0.9,
        'x':0.5,
        'xanchor': 'center',
        'yanchor': 'top'
    }
)

# Display the Plotly charts side by side in Streamlit
pie1, pie2 = st.columns(2)

with pie1:
    st.plotly_chart(fig_acc, use_container_width = True)

with pie2:
    st.plotly_chart(fig_service, use_container_width = True)


### 3.3 Top 10 cost by Account AND Service Type ###

st.subheader('Top Cost by Account & Service Type')
st.write("\n\n")

# Cost by account AND service type query
if quoted_selected_acc and quoted_selected_service:
    cost_acc_service_query = '''
        SELECT 
            ACCOUNT_NAME,
            SERVICE_TYPE,
            SUM(USAGE_IN_CURRENCY) AS TOTAL_COST_USD
        FROM SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY 
        WHERE ACCOUNT_NAME IN ({})
        AND SERVICE_TYPE IN ({})
        AND USAGE_DATE BETWEEN '{}' AND '{}'
        GROUP BY ACCOUNT_NAME, SERVICE_TYPE
        ORDER BY TOTAL_COST_USD DESC
        LIMIT 10
    '''.format(
        ', '.join(quoted_selected_acc),
        ', '.join(quoted_selected_service),
        start_date_str,
        end_date_str
    )
else:
    cost_acc_service_query = '''
        SELECT 
            NULL AS ACCOUNT_NAME,
            NULL AS SERVICE_TYPE,
            0 AS TOTAL_COST_USD
    '''

# Convert to DataFrame
df_cost_acc_service = session.sql(cost_acc_service_query).to_pandas()

# Display the filtered DataFrame
st.dataframe(df_cost_acc_service, use_container_width = True)