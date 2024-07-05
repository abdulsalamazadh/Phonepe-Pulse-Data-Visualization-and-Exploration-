import streamlit as st
import pandas as pd
import psycopg2
from psycopg2 import sql
import plotly.express as plt
from streamlit_option_menu import option_menu
import requests
import json
from PIL import Image

# Table creation from DB

# Connection with Postgres
db_conn = psycopg2.connect(host="localhost",
                           user="postgres",
                           password="password",
                           database="phonepe_project",
                           port="5432")
cur = db_conn.cursor()

# aggregated_transaction dataframe creation

# Execute the SQL query to fetch data from the 'aggregated_transaction' table
cur.execute("select * from aggregated_transaction")
db_conn.commit()
table_1 = cur.fetchall()

# Convert the fetched data into a DataFrame
aggregated_transaction_df = pd.DataFrame(table_1)
aggregated_transaction_df.columns = ["State", "Year", "Quater", "Transaction_type", "Transaction_count", "Transaction_amount"]
# aggregated_transaction_df

# aggregated_user dataframe creation

# Execute the SQL query to fetch data from the 'aggregated_user' table
cur.execute("select * from aggregated_user")
db_conn.commit()
table_2 = cur.fetchall()

# Convert the fetched data into a DataFrame
aggregated_user_df = pd.DataFrame(table_2)
aggregated_user_df.columns = ["State", "Year", "Quater", "Percentage", "Transaction_count", "Brands"]
# aggregated_user_df

# map_transcation dataframe creation

# Execute the SQL query to fetch data from the 'map_transcation' table
cur.execute("select * from map_transcation")
db_conn.commit()
table_3 = cur.fetchall()

# Convert the fetched data into a DataFrame
map_transcation_df = pd.DataFrame(table_3)
map_transcation_df.columns = ["State", "Year", "Quater", "Districts", "Transaction_count", "Transaction_amount"]
# map_transcation_df

# map_user dataframe creation

# Execute the SQL query to fetch data from the 'map_user' table
cur.execute("select * from map_user")
db_conn.commit()
table_4 = cur.fetchall()

# Convert the fetched data into a DataFrame
map_user_df = pd.DataFrame(table_4)
map_user_df.columns = ["State", "Year", "Quater", "Districts", "No_of_Registered_Users", "No_of_times_App_opened"]
# map_user_df

# top_transaction dataframe creation

# Execute the SQL query to fetch data from the 'top_transaction' table
cur.execute("select * from top_transaction")
db_conn.commit()
table_5 = cur.fetchall()

# Convert the fetched data into a DataFrame
top_transaction_df = pd.DataFrame(table_5)
top_transaction_df.columns = ["State", "Year", "Quater", "Pincode", "Transaction_count", "Transaction_amount"]
# top_transaction_df

# top_user dataframe creation

# Execute the SQL query to fetch data from the 'top_user' table
cur.execute("select * from top_user")
db_conn.commit()
table_6 = cur.fetchall()

# Convert the fetched data into a DataFrame
top_user_df = pd.DataFrame(table_6)
top_user_df.columns = ["State", "Year", "Quater", "Pincode", "No_of_Registered_Users"]
# top_user_df

# Year-wise analysis function
def agg_trans_yr(df, year):
    # Filter the DataFrame for the selected year
    agg_trans_yr = df[df["Year"] == year]
    agg_trans_yr.reset_index(drop=True, inplace=True)

    # Group the filtered data by State and aggregate the Transaction_count and Transaction_amount
    agg_trans_yr_gr = agg_trans_yr.groupby("State")[["Transaction_count", "Transaction_amount"]].sum()
    agg_trans_yr_gr.reset_index(inplace=True)

    agg_trans_yr_gr.sort_values(by='Transaction_count', ascending=False, inplace=True)
    agg_trans_yr_gr.sort_values(by='Transaction_amount', ascending=False, inplace=True)
    
    # Create two columns layout in Streamlit
    col1, col2 = st.columns(2)
    with col1:
        # Bar chart for Transaction Amount by State
        f_amnt = plt.bar(agg_trans_yr_gr, x="State", y="Transaction_amount", title=f"Transaction Amount in {year}",
                         labels={"State": "States", "Transaction_amount": "Transaction Amount"},
                         color_discrete_sequence=plt.colors.sequential.Bluered, height=650, width=600)
        st.plotly_chart(f_amnt)
    with col2:
        # Bar chart for Transaction Count by State
        f_count = plt.bar(agg_trans_yr_gr, x="State", y="Transaction_count", title=f"Transaction Count in {year}", 
                          labels={"State": "States", "Transaction_count": "Transaction Count"},
                          color_discrete_sequence=plt.colors.sequential.amp_r, height=650, width=600)
        st.plotly_chart(f_count)

    # Create two columns layout in Streamlit for map analysis
    col1, col2 = st.columns(2)
    with col1:
        # Load the GeoJSON data for India states
        india_map_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response = requests.get(india_map_url)
        map_data = json.loads(response.content)

        # Extract state names from the GeoJSON data
        states_name = []
        for states in map_data["features"]:
            states_name.append(states["properties"]["ST_NM"])
        states_name.sort()

        # Choropleth map for Transaction Amount by State
        india_figure_1 = plt.choropleth(agg_trans_yr_gr, geojson=map_data, locations=states_name,
                                        featureidkey="properties.ST_NM", color="Transaction_amount",
                                        color_continuous_scale="YlOrRd", 
                                        range_color=(agg_trans_yr_gr["Transaction_amount"].min(), agg_trans_yr_gr["Transaction_amount"].max()),
                                        hover_name="State", title=f"Transaction Amount by States {year}", 
                                        fitbounds="locations", height=650, width=600)
        
        india_figure_1.update_geos(visible=False)
        st.plotly_chart(india_figure_1)

    with col2:
        # Choropleth map for Transaction Count by State
        india_figure_2 = plt.choropleth(agg_trans_yr_gr, geojson=map_data, locations=states_name,
                                        featureidkey="properties.ST_NM", color="Transaction_count",
                                        color_continuous_scale="YlGnBu", 
                                        range_color=(agg_trans_yr_gr["Transaction_count"].min(), agg_trans_yr_gr["Transaction_count"].max()),
                                        hover_name="State", title=f"Transaction Count by States {year}", 
                                        fitbounds="locations", height=650, width=600)
        
        india_figure_2.update_geos(visible=False)
        st.plotly_chart(india_figure_2)

    return agg_trans_yr

# Quarter-wise analysis function
def agg_trans_yr_quater(df, quater):
    # Filter the DataFrame for the selected quarter
    agg_trans_yr = df[df["Quater"] == quater]
    agg_trans_yr.reset_index(drop=True, inplace=True)

    # Group the filtered data by State and aggregate the Transaction_count and Transaction_amount
    agg_trans_yr_gr = agg_trans_yr.groupby("State")[["Transaction_count", "Transaction_amount"]].sum()
    agg_trans_yr_gr.reset_index(inplace=True)

    agg_trans_yr_gr.sort_values(by='Transaction_count', ascending=False, inplace=True)
    agg_trans_yr_gr.sort_values(by='Transaction_amount', ascending=False, inplace=True)

    # Create two columns layout in Streamlit
    col1, col2 = st.columns(2)
    with col1:
        # Bar chart for Transaction Amount by State in the selected quarter
        f_amnt = plt.bar(agg_trans_yr_gr, x="State", y="Transaction_amount",
                         title=f"Transaction Amount in {agg_trans_yr['Year'].min()} {quater} Quater",
                         labels={"Transaction_amount": "Transaction Amount", "State": "States"},
                         color_discrete_sequence=plt.colors.sequential.Bluered, height=650, width=600)
        st.plotly_chart(f_amnt)
    with col2:
        # Bar chart for Transaction Count by State in the selected quarter
        f_count = plt.bar(agg_trans_yr_gr, x="State", y="Transaction_count",
                          title=f"Transaction Count in {agg_trans_yr['Year'].min()} {quater} Quater",
                          labels={"Transaction_count": "Transaction Count", "State": "States"},
                         color_discrete_sequence=plt.colors.sequential.amp_r, height=650, width=600)
        st.plotly_chart(f_count)

    # Create two columns layout in Streamlit for map analysis
    col1, col2 = st.columns(2)
    with col1:
        # Load the GeoJSON data for India states
        india_map_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response = requests.get(india_map_url)
        map_data = json.loads(response.content)

        # Extract state names from the GeoJSON data
        states_name = []
        for states in map_data["features"]:
            states_name.append(states["properties"]["ST_NM"])
        states_name.sort()

        # Choropleth map for Transaction Amount by State in the selected quarter
        india_figure_1 = plt.choropleth(agg_trans_yr_gr, geojson=map_data, locations=states_name,
                                        featureidkey="properties.ST_NM", color="Transaction_amount",
                                        color_continuous_scale="YlOrRd", 
                                        range_color=(agg_trans_yr_gr["Transaction_amount"].min(), agg_trans_yr_gr["Transaction_amount"].max()),
                                        hover_name="State", title=f"Transaction Amount by States {agg_trans_yr['Year'].min()} {quater} rd Quater", 
                                        fitbounds="locations", height=650, width=600)
        
        india_figure_1.update_geos(visible=False)
        st.plotly_chart(india_figure_1)

    with col2:
        # Choropleth map for Transaction Count by State in the selected quarter
        india_figure_2 = plt.choropleth(agg_trans_yr_gr, geojson=map_data, locations=states_name,
                                        featureidkey="properties.ST_NM", color="Transaction_count",
                                        color_continuous_scale="YlGnBu", 
                                        range_color=(agg_trans_yr_gr["Transaction_count"].min(), agg_trans_yr_gr["Transaction_count"].max()),
                                        hover_name="State", title=f"Transaction Count by States {agg_trans_yr['Year'].min()} {quater} rd Quater", 
                                        fitbounds="locations", height=650, width=600)
        
        india_figure_2.update_geos(visible=False)
        st.plotly_chart(india_figure_2)

    return agg_trans_yr

# Analysis by transaction type function
def agg_trans_type(df, state):
    # Filter the DataFrame for the selected state
    agg_trans_state = df[df["State"] == state]
    agg_trans_state.reset_index(drop=True, inplace=True)

    # Group the filtered data by Transaction_type and aggregate the Transaction_count and Transaction_amount
    agg_trans_type_grp = agg_trans_state.groupby("Transaction_type")[["Transaction_count", "Transaction_amount"]].sum()
    agg_trans_type_grp.reset_index(inplace=True)

    agg_trans_type_grp.sort_values(by='Transaction_count', ascending=False, inplace=True)
    agg_trans_type_grp.sort_values(by='Transaction_amount', ascending=False, inplace=True)

    # Create two columns layout in Streamlit
    col1, col2 = st.columns(2)
    with col1:
        # Bar chart for Transaction Count by type in the selected state
        fig_type_count = plt.bar(agg_trans_type_grp, x="Transaction_type", y="Transaction_count",
                                 labels={"Transaction_type": "Type of Transaction", "Transaction_count": "Transaction Count"},
                                 title=f"No of transaction by type in {state}", color_discrete_sequence=['Red'])
        st.plotly_chart(fig_type_count)
    with col2:
        # Bar chart for Transaction Amount by type in the selected state
        fig_type_amt = plt.bar(agg_trans_type_grp, x="Transaction_type", y="Transaction_amount",
                               labels={"Transaction_type": "Type of Transaction", "Transaction_amount": "Transaction Amount"},
                               title=f"Distribution of spending through {state}", color_discrete_sequence=['green'])
        st.plotly_chart(fig_type_amt)

    return agg_trans_state

def agg_user_plot1(df, year):
    # Filter the DataFrame to include only rows where the 'Year' column matches the specified year
    agg_user_yr = df[df["Year"] == year]
    
    # Reset the index of the DataFrame and drop the old index
    agg_user_yr.reset_index(drop=True, inplace=True)

    # Group the DataFrame by 'Brands' and sum the 'Transaction_count' for each group
    agg_user_yr_gr = agg_user_yr.groupby("Brands")[["Transaction_count"]].sum()
    
    # Reset the index of the grouped DataFrame
    agg_user_yr_gr.reset_index(inplace=True)

    # Sort the grouped DataFrame by 'Transaction_count' in descending order
    agg_user_yr_gr.sort_values(by='Transaction_count', ascending=False, inplace=True)

    # Create a bar plot with 'Brands' on the x-axis and 'Transaction_count' on the y-axis
    fig_1 = plt.bar(
        agg_user_yr_gr,
        x='Brands',
        y='Transaction_count',
        title=f'Transaction Count by Brand for the Year {year}',
        labels={'Transaction_count': 'Transaction Count', 'Brands': 'Brands'}
    )
    
    # Display the plot using Streamlit
    st.plotly_chart(fig_1)

    # Return the filtered DataFrame
    return agg_user_yr

def agg_user_plot2(df, quater):
    # Filter the DataFrame to include only rows where the 'Quater' column matches the specified quarter
    agg_user_qr = df[df["Quater"] == quater]
    
    # Reset the index of the DataFrame and drop the old index
    agg_user_qr.reset_index(drop=True, inplace=True)

    # Group the DataFrame by 'Brands' and sum the 'Transaction_count' for each group
    agg_user_qr_gr = agg_user_qr.groupby("Brands")[["Transaction_count"]].sum()
    
    # Reset the index of the grouped DataFrame
    agg_user_qr_gr.reset_index(inplace=True)

    # Sort the grouped DataFrame by 'Transaction_count' in descending order
    agg_user_qr_gr.sort_values(by="Transaction_count", ascending=False, inplace=True)

    # Create a bar plot with 'Brands' on the x-axis and 'Transaction_count' on the y-axis
    fig = plt.bar(
        agg_user_qr_gr, 
        x="Brands", 
        y="Transaction_count", 
        title=f"Transaction Count by Brand for the Quarter {quater}", 
        labels={'Transaction_count': 'Transaction Count', 'Brands': 'Brands'}
    )
    
    # Display the plot using Streamlit
    st.plotly_chart(fig)

    # Return the filtered DataFrame
    return agg_user_qr

def agg_user_plot3(df, state):
    # Filter the DataFrame to include only rows where the 'State' column matches the specified state
    agg_user_yr_qu_state = df[df["State"] == state]
    
    # Reset the index of the DataFrame and drop the old index
    agg_user_yr_qu_state.reset_index(drop=True, inplace=True)

    # Create a line plot with 'Brands' on the x-axis, 'Transaction_count' on the y-axis, and additional hover data
    fig = plt.line(
        agg_user_yr_qu_state, 
        x="Brands", 
        y="Transaction_count", 
        hover_data=["Percentage"], 
        width=1000,
        title=f"Transaction Counts by Brand in {state} with Percentage Details", 
        labels={'Transaction_count': 'Transaction Count', 'Brands': 'Brands'}
    )

    # Display the plot using Streamlit
    st.plotly_chart(fig)

    return agg_user_yr_qu_state

def map_trans_state(df, states):
    # Filter the DataFrame for the given state
    map_trans_states = df[df["State"] == states]
    # Reset the index of the filtered DataFrame
    map_trans_states.reset_index(drop=True, inplace=True)

    # Group by 'Districts' and sum 'Transaction_count' and 'Transaction_amount'
    map_trans_states_gr = map_trans_states.groupby("Districts")[["Transaction_count", "Transaction_amount"]].sum()
    # Reset the index of the grouped DataFrame
    map_trans_states_gr.reset_index(inplace=True)

    # Sort the DataFrame by 'Transaction_count' and 'Transaction_amount' in ascending order
    map_trans_states_gr.sort_values(by=["Transaction_count", "Transaction_amount"], ascending=[True, True], inplace=True)

    # Create two columns layout in Streamlit
    col1, col2 = st.columns(2)
    with col1:
        # Plotting Transaction Count by Districts
        bar_fig_count = plt.bar(map_trans_states_gr, x="Transaction_count", y= "Districts", orientation= "h",
                            title= f"Transaction Count by Districts in {states}", 
                            labels={'Transaction_count': 'Transaction Count', 'Districts': 'Districts'})
        st.plotly_chart(bar_fig_count)

    with col2:
        # Plotting Transaction Amount by Districts
        bar_fig_amt = plt.bar(map_trans_states_gr, x="Transaction_amount", y= "Districts", orientation= "h",
                        title= f"Transaction Amount by Districts in {states}", 
                        labels={'Transaction_amount': 'Transaction Amount', 'Districts': 'Districts'})
        st.plotly_chart(bar_fig_amt)

    return map_trans_states

def map_user_plot1(df, year):

    map_user_yr = df[df["Year"] == year]
    map_user_yr.reset_index(drop= True, inplace=True)

    map_user_yr_gr = map_user_yr.groupby("State")[["No_of_Registered_Users", "No_of_times_App_opened"]].sum()
    map_user_yr_gr.reset_index(inplace= True)

    map_user_yr_gr_rn = map_user_yr_gr.rename(columns={"No_of_Registered_Users":"Registered User",
                        "No_of_times_App_opened":"App Opened"})
    map_user_yr_gr_rn.reset_index(drop=True, inplace=True)

    fig_line = plt.line(map_user_yr_gr_rn, x= "State", y= ["Registered User", "App Opened"], 
                        labels= {"State": "States", "No_of_Registered_Users" : "Register Users", "No_of_times_App_opened" : "App Opened"},
                        title = f"{year} User Statistics by States")

    st.plotly_chart(fig_line)

    return map_user_yr

def map_user_plot2(df, quater):

    map_user_qr = df[df["Quater"] == quater]
    map_user_qr.reset_index(drop=True, inplace=True)

    map_user_qr_gr = map_user_qr.groupby("State")[["No_of_Registered_Users", "No_of_times_App_opened"]].sum()
    map_user_qr_gr.reset_index(inplace=True)

    map_user_qr_gr_rn = map_user_qr_gr.rename(columns={"No_of_Registered_Users":"Registered Users", 
                                                    "No_of_times_App_opened": "App Opens"})
    map_user_qr_gr_rn.reset_index(drop=True, inplace=True)

    fig_line_1 = plt.line(map_user_qr_gr_rn, x= "State", y= ["Registered Users", "App Opens"], 
                        title= f"Registered Users and App Opens per State in Quater {quater}")
    st.plotly_chart(fig_line_1)

    return map_user_qr

def map_user_plot3(df, states):
    map_user_states = df[df["State"] == states]
    map_user_states.reset_index(drop=True, inplace=True)

    map_user_states.sort_values(by="No_of_Registered_Users", ascending=False, inplace=True)

    col1, col2 = st.columns(2)
    with col1:
        fig_bar_1 = plt.bar(map_user_states, x="Districts", y="No_of_Registered_Users",
                            labels={"Districts": "Districts", "No_of_Registered_Users": "Registered Users"},
                            title="Number of Registered Users per District in Tamil Nadu")
        st.plotly_chart(fig_bar_1)
    with col2:
        fig_bar_2 = plt.bar(map_user_states, x = "Districts", y = "No_of_times_App_opened",
                            labels = {"Districts": "Districts", "No_of_times_App_opened": "App Opens"},
                            title = f"Number of App Opens per District in {states}")
        st.plotly_chart(fig_bar_2)

    return map_user_states

def top_trans_plot1(df, states):
    top_trans_yr = df[df["State"] == states]
    top_trans_yr.reset_index(drop= True, inplace=True)

    col1, col2 = st.columns(2)
    with col1:
        fig_top_1 = plt.bar(top_trans_yr, x= "Quater", y= "Transaction_count", hover_data= ["Pincode"], 
                            labels={"Quater": "Quarter", "Transaction_count": "Transaction Count"}, 
                            title=f"Transaction Count per Quarter in {states}")
        st.plotly_chart(fig_top_1)
    
    with col2:
        fig_top_2 = plt.bar(top_trans_yr, x= "Quater", y= "Transaction_amount", hover_data= ["Pincode"],
                            labels={"Quater": "Quarter", "Transaction_amount": "Transaction Amount"},
                            title=f"Transaction Amount per Quarter in {states}")
        st.plotly_chart(fig_top_2)

    return top_trans_yr

def top_user_plot1(df, year):

    top_user_yr = df[df["Year"] == year]
    top_user_df.reset_index(drop=True, inplace=True)

    top_user_df_gr = top_user_df.groupby(["State", "Quater"])[["No_of_Registered_Users"]].sum()
    top_user_df_gr.reset_index(inplace=True)

    top_user_df_gr.sort_values(by=["No_of_Registered_Users"], ascending=False, inplace=True)

    fig_user_1 = plt.bar(top_user_df_gr, x= "State", y= "No_of_Registered_Users", color= "Quater",
                        labels={"State": "State", "No_of_Registered_Users": "Number of Registered Users", "Quater": "Quarter"},
                        title=f"Trend of Registered Users per State by Quarter in {year}")
    st.plotly_chart(fig_user_1)

    return top_user_yr

def top_user_plot2(df, state):

    top_user_st = df[df["State"] == state]
    top_user_st.reset_index(drop=True, inplace=True)

    fig_user_2 = plt.bar(top_user_st, x= "Quater", y= "No_of_Registered_Users", hover_data= "Pincode", color= "No_of_Registered_Users",
                        labels={"Quater": "Quarter", "No_of_Registered_Users": "Number of Registered Users"},
                        title=f"Number of Registered Users per Quarter in {state}")
    st.plotly_chart(fig_user_2)

    return top_user_st

# Function to fetch data from PostgreSQL and create charts for different queries
def display_total_transactions_by_state():
    # Connect to PostgreSQL
    db_conn = psycopg2.connect(host="localhost",
                               user="postgres",
                               password="password",
                               database="phonepe_project",
                               port="5432")
    cur = db_conn.cursor()

    # Execute SQL query to fetch total transactions by state
    cur.execute("SELECT State, SUM(Transaction_count) AS Total_Transactions FROM aggregated_transaction GROUP BY State")
    table_data = cur.fetchall()

    # Convert fetched data into a DataFrame
    df = pd.DataFrame(table_data, columns=["State", "Total_Transactions"])

    df_1 = df.sort_values(by="Total_Transactions", ascending=False, inplace=True)

    # Plot using Plotly Express
    fig = plt.bar(df, x="State", y="Total_Transactions", title="Total Transactions by State")
    st.plotly_chart(fig)

def display_quarterly_transactions_by_year():
    # Connect to PostgreSQL (assuming connection details are same)
    db_conn = psycopg2.connect(host="localhost",
                               user="postgres",
                               password="password",
                               database="phonepe_project",
                               port="5432")
    cur = db_conn.cursor()

    # Execute SQL query to fetch quarterly transactions by year
    cur.execute("SELECT Year, Quater, SUM(Transaction_count) AS Total_Transactions FROM aggregated_transaction GROUP BY Year, Quater")
    table_data = cur.fetchall()

    # Convert fetched data into a DataFrame
    df = pd.DataFrame(table_data, columns=["Year", "Quater", "Total_Transactions"])

    # Plot using Plotly Express
    fig = plt.line(df, x="Quater", y="Total_Transactions", color="Year",
             labels={'Total_Transactions': 'Number of Transactions'}, title="Quarterly Transactions by Year")
    st.plotly_chart(fig)

def display_transaction_types_distribution():
    # Connect to PostgreSQL (assuming connection details are same)
    db_conn = psycopg2.connect(host="localhost",
                               user="postgres",
                               password="password",
                               database="phonepe_project",
                               port="5432")
    cur = db_conn.cursor()

    # Execute SQL query to fetch transaction types distribution
    cur.execute("SELECT Transaction_type, SUM(Transaction_count) AS Total_Transactions FROM aggregated_transaction GROUP BY Transaction_type")
    table_data = cur.fetchall()

    # Convert fetched data into a DataFrame
    df = pd.DataFrame(table_data, columns=["Transaction_type", "Total_Transactions"])

    # Plot using Plotly Express
    fig = plt.pie(df, values="Total_Transactions", names="Transaction_type", title="Transaction Types Distribution")
    st.plotly_chart(fig)

def display_top_districts_by_transaction_amount():
    # Connect to PostgreSQL (assume connection details are same as before)
    db_conn = psycopg2.connect(host="localhost",
                               user="postgres",
                               password="password",
                               database="phonepe_project",
                               port="5432")
    cur = db_conn.cursor()

    # Execute SQL query to fetch top districts by transaction amount
    cur.execute("SELECT Districts, SUM(Transaction_amount) AS Total_Amount FROM map_transcation GROUP BY Districts ORDER BY Total_Amount DESC LIMIT 10")
    table_data = cur.fetchall()

    # Convert fetched data into a DataFrame
    df = pd.DataFrame(table_data, columns=["Districts", "Total_Amount"])

    # Plot using Plotly Express horizontal bar chart
    fig = plt.bar(df, x="Total_Amount", y="Districts", orientation="h", title="Top Districts by Transaction Amount")
    st.plotly_chart(fig)

def display_user_registration_trends():
    # Connect to PostgreSQL (assume connection details are same as before)
    db_conn = psycopg2.connect(host="localhost",
                               user="postgres",
                               password="password",
                               database="phonepe_project",
                               port="5432")
    cur = db_conn.cursor()

    # Execute SQL query to fetch user registration trends
    cur.execute("SELECT Year, SUM(No_of_Registered_Users) AS Total_Users FROM map_user GROUP BY Year")
    table_data = cur.fetchall()

    # Convert fetched data into a DataFrame
    df = pd.DataFrame(table_data, columns=["Year", "Total_Users"])

    # Plot using Plotly Express line plot
    fig = plt.pie(df, values="Total_Users", names="Year", title="User Registration Trends")
    st.plotly_chart(fig)

def display_app_open_frequency():
    # Connect to PostgreSQL (assume connection details are same as before)
    db_conn = psycopg2.connect(host="localhost",
                               user="postgres",
                               password="password",
                               database="phonepe_project",
                               port="5432")
    cur = db_conn.cursor()

    # Execute SQL query to fetch app open frequency
    cur.execute("SELECT Year, SUM(No_of_times_App_opened) AS Total_Opens FROM map_user GROUP BY Year")
    table_data = cur.fetchall()

    # Convert fetched data into a DataFrame
    df = pd.DataFrame(table_data, columns=["Year", "Total_Opens"])

    # Plot using Plotly Express line plot
    fig = plt.pie(df, names="Year", values="Total_Opens", title="App Open Frequency")
    st.plotly_chart(fig)

def display_top_pincodes_by_transaction_count():
    # Connect to PostgreSQL (assume connection details are same as before)
    db_conn = psycopg2.connect(host="localhost",
                               user="postgres",
                               password="password",
                               database="phonepe_project",
                               port="5432")
    cur = db_conn.cursor()

    # Execute SQL query to fetch top pincodes by transaction count
    cur.execute("SELECT Pincode, SUM(Transaction_count) AS Total_Transactions FROM top_transaction GROUP BY Pincode ORDER BY Total_Transactions DESC LIMIT 10")
    table_data = cur.fetchall()

    # Convert fetched data into a DataFrame
    df = pd.DataFrame(table_data, columns=["Pincode", "Total_Transactions"])

    # Plot using Plotly Express horizontal bar chart
    fig = plt.pie(df, names="Pincode", values="Total_Transactions", title="Top Pincodes by Transaction Count")
    st.plotly_chart(fig)

def display_average_transaction_amount_by_state():
    # Connect to PostgreSQL (assume connection details are same as before)
    db_conn = psycopg2.connect(host="localhost",
                               user="postgres",
                               password="password",
                               database="phonepe_project",
                               port="5432")
    cur = db_conn.cursor()

    # Execute SQL query to fetch average transaction amount by state
    cur.execute("SELECT State, AVG(Transaction_amount) AS Avg_Amount FROM aggregated_transaction GROUP BY State")
    table_data = cur.fetchall()

    # Convert fetched data into a DataFrame
    df = pd.DataFrame(table_data, columns=["State", "Avg_Amount"])

    # Plot using Plotly Express bar chart
    fig = plt.bar(df, x="State", y="Avg_Amount", title="Average Transaction Amount by State")
    st.plotly_chart(fig)

def display_brand_popularity_in_transactions():
    # Connect to PostgreSQL (assume connection details are same as before)
    db_conn = psycopg2.connect(host="localhost",
                               user="postgres",
                               password="password",
                               database="phonepe_project",
                               port="5432")
    cur = db_conn.cursor()

    # Execute SQL query to fetch brand popularity in transactions
    cur.execute("SELECT Brands, COUNT(*) AS Transactions FROM aggregated_user GROUP BY Brands ORDER BY Transactions DESC")
    table_data = cur.fetchall()

    # Convert fetched data into a DataFrame
    df = pd.DataFrame(table_data, columns=["Brands", "Transactions"])

    # Plot using Plotly Express horizontal bar chart
    fig = plt.pie(df, values="Transactions", names="Brands", title="Brand Popularity in Transactions")
    st.plotly_chart(fig)

def display_yearly_comparison_of_transaction_data():
    # Connect to PostgreSQL (assume connection details are same as before)
    db_conn = psycopg2.connect(host="localhost",
                               user="postgres",
                               password="password",
                               database="phonepe_project",
                               port="5432")
    cur = db_conn.cursor()

    # Execute SQL query to fetch yearly comparison of transaction data
    cur.execute("SELECT Year, SUM(Transaction_amount) AS Total_Amount FROM aggregated_transaction GROUP BY Year")
    table_data = cur.fetchall()

    # Convert fetched data into a DataFrame
    df = pd.DataFrame(table_data, columns=["Year", "Total_Amount"])

    # Plot using Plotly Express line plot
    fig = plt.scatter(df, x="Year", y="Total_Amount", title="Yearly Comparison of Transaction Data")
    st.plotly_chart(fig)




# Streamlit Part

# Set the page layout and title
st.set_page_config(layout="wide")


# Sidebar menu for navigation
with st.sidebar:
    select = option_menu("Main Menu", ["Home", "Analysis", "Top 10 Questions", "Exit"])

# Check if the selected section is "Home"
if select == "Home":
    st.title(":violet[PhonePe Pulse Data Visualization]")
    # Create two columns for layout
    col1, col2 = st.columns(2)

    # Add content to the first column
    with col1:
        # Display the main header
        st.header("PHONEPE")
        
        # Display a subheader
        st.subheader("INDIA'S BEST TRANSACTION APP")
        
        # Add a markdown description
        st.markdown("PhonePe is an Indian digital payments and financial technology company")
        
        # List of features
        st.write("****FEATURES****")
        st.write("****Credit & Debit card linking****")
        st.write("****Bank Balance check****")
        st.write("****Money Storage****")
        st.write("****PIN Authorization****")

    # Add content to the second column
    with col2:
        # Display an image with a specified width
        st.image(Image.open(r"phonepe-1716012608.jpg"), width=600)

    # Create another two columns for additional layout
    col3, col4 = st.columns(2)
    
    # Add content to the third column
    with col3:
        # Display another image with a specified width
        st.image(Image.open(r"phonepe-1693910378.jpg"), width=600)

    # Add content to the fourth column
    with col4:
        # List of additional features
        st.write("****Easy Transactions****")
        st.write("****One App For All Your Payments****")
        st.write("****Your Bank Account Is All You Need****")
        st.write("****Multiple Payment Modes****")
        st.write("****PhonePe Merchants****")
        st.write("****Multiple Ways To Pay****")
        st.write("****1.Direct Transfer & More****")
        st.write("****2.QR Code****")
        st.write("****Earn Great Rewards****")

    # Create two more columns for further layout
    col5, col6 = st.columns(2)

    # Add content to the fifth column
    with col5:
        # Add vertical spacing using markdown for alignment purposes
        st.markdown(" ")

        # List of additional features related to payment and wallet usage
        st.write("****No Wallet Top-Up Required****")
        st.write("****Pay Directly From Any Bank To Any Bank A/C****")
        st.write("****Instantly & Free****")
# Analysis section
if select == "Analysis":
    col1, col2 = st.columns([3, 1])
    with col1:
        st.title(":violet[PhonePe Pulse Data Analysis]")
    with col2:
        st.image(r"PhonePe-Statistics.jpg", width= 200)
    tab1, tab2, tab3 = st.tabs(["Aggregated Analysis", "Map Analysis", "Top Analysis"])

    with tab1:
        tab_1 = st.radio("Select the option below:", ["Transaction Analysis", "User Analysis"])

        if tab_1 == "Transaction Analysis":
            col1, col2 = st.columns(2)
            with col1:
                # Dropdown to select the year
                years = aggregated_transaction_df["Year"].unique()
                selected_year = st.selectbox("Select the year", years)
            # Call the function to perform year-wise analysis
            df_yr = agg_trans_yr(aggregated_transaction_df, selected_year)

            col1, col2 = st.columns(2)
            with col1:
                # Dropdown to select the quarter
                quaters = df_yr["Quater"].unique()
                selected_qu = st.selectbox("Select the Quater", quaters)
            # Call the function to perform quarter-wise analysis
            agg_trans_yr_qu = agg_trans_yr_quater(df_yr, selected_qu)

            col1, col2 = st.columns(2)
            with col1:
                # Dropdown to select the state
                states = st.selectbox("Select the State", agg_trans_yr_qu["State"].unique())
                # Call the function to perform transaction type analysis
                agg_trans_type(agg_trans_yr_qu, states)
        
        elif tab_1 == "User Analysis":

            col1,col2= st.columns(2)
            with col1:

                years= st.selectbox("Select the Year",aggregated_user_df["Year"].unique())
            agg_user_yr= agg_user_plot1(aggregated_user_df, years)

            col1,col2= st.columns(2)
            with col1:

                quater = st.selectbox("Select the Quater", aggregated_user_df["Quater"].unique())
            agg_user_yr_qu = agg_user_plot2(agg_user_yr, quater)

            col1,col2= st.columns(2)
            with col1:

                states= st.selectbox("Select The State", agg_user_yr_qu["State"].unique())
            agg_user_plot3(agg_user_yr_qu, states)
            
    with tab2:
        tab_2 = st.radio("Select the option below:", ["Transaction Analysis", "User Analysis"], key="tab2_radio")

        if tab_2 == "Transaction Analysis":

            col1, col2 = st.columns(2)
            with col1:
                years = map_transcation_df["Year"].unique()
                selected_year = st.selectbox("Select the year", years, key="tab2_year_select")
            df_yr_map = agg_trans_yr(map_transcation_df, selected_year)

            col1, col2 = st.columns(2)
            with col1:
                states= st.selectbox("Select The States", df_yr_map["State"].unique())
            map_trans_state(df_yr_map, states)

            col1,col2= st.columns(2)
            with col1:
                quaters = df_yr_map["Quater"].unique()
                selected_qu = st.selectbox("Select the Quater", quaters, key= "tab2_selected_qu")
            df_yr_qr_map= agg_trans_yr_quater(df_yr_map, selected_qu)

            col1,col2= st.columns(2)
            with col1:
                states = st.selectbox("Select The State", df_yr_qr_map["State"].unique(), key= "tab2_state_dis")
            map_trans_state(df_yr_qr_map, states)
            
        elif tab_2 == "User Analysis":

            col1, col2 = st.columns(2)
            with col1:
                sl_year = map_user_df["Year"].unique()
                year = st.selectbox("Select the Year", sl_year, key= "tab2_user_year")
            map_user_yr = map_user_plot1(map_user_df, year)

            col1, col2 = st.columns(2)
            with col1:
                quater = st.selectbox("Select the Quater", map_user_yr["Quater"].unique(), key= "tab2_user_quater")
            map_user_qr = map_user_plot2(map_user_df, quater)

            col1, col2 = st.columns(2)
            with col1:
                states = st.selectbox("Select the State", map_user_qr["State"].unique(), key= "tab2_user_states")
            map_user_plot3(map_user_qr, states)

    with tab3:
        tab_3 = st.radio("Select the Option below", ["Transaction Analysis", "User Analysis"], key= "tab3_radio")
        if tab_3 == "Transaction Analysis":
            
            col1, col2 = st.columns(2)
            with col1:
                years = top_transaction_df["Year"].unique()
                selected_year = st.selectbox("Select the year", years, key="tab3_year_select")
            df_yr_top = agg_trans_yr(top_transaction_df, selected_year)

            col1, col2 = st.columns(2)
            with col1:
                states= st.selectbox("Select The States", df_yr_top["State"].unique(), key="tab3_states")
            top_trans_plot1(df_yr_top, states)

            col1,col2= st.columns(2)
            with col1:
                quaters = df_yr_top["Quater"].unique()
                selected_qu = st.selectbox("Select the Quater", quaters, key= "tab3_selected_qu")
            df_yr_qr_top= agg_trans_yr_quater(df_yr_top, selected_qu)

        elif tab_3 == "User Analysis":

            col1, col2 = st.columns(2)
            with col1:
                sl_year = top_user_df["Year"].unique()
                year = st.selectbox("Select the Year", sl_year, key= "tab3_user_year")
            map_user_yr = top_user_plot1(top_user_df, year)

            col1, col2 = st.columns(2)
            with col1:
                states = st.selectbox("Select the State", map_user_yr["State"].unique(), key= "tab3_user_states")
            top_user_plot2(map_user_yr, states)

elif select == "Top 10 Questions":

    st.title(":violet[Explore PhonePe FAQs: Data-Driven Answers]")

    options = [
        'Total Transactions by State',
        'Quarterly Transactions by Year',
        'Transaction Types Distribution',
        'Top Districts by Transaction Amount',
        'User Registration Trends',
        'App Open Frequency',
        'Top Pincodes by Transaction Count',
        'Average Transaction Amount by State',
        'Brand Popularity in Transactions',
        'Yearly Comparison of Transaction Data'
    ]

    selected_option = st.selectbox("Select a query to visualize", options)

    # Display data based on selection
    if selected_option == 'Total Transactions by State':
        display_total_transactions_by_state()
    elif selected_option == 'Quarterly Transactions by Year':
        display_quarterly_transactions_by_year()
    elif selected_option == 'Transaction Types Distribution':
        display_transaction_types_distribution()
    elif selected_option == 'Top Districts by Transaction Amount':
        display_top_districts_by_transaction_amount()
    elif selected_option == 'User Registration Trends':
        display_user_registration_trends()
    elif selected_option == 'App Open Frequency':
        display_app_open_frequency()
    elif selected_option == 'Top Pincodes by Transaction Count':
        display_top_pincodes_by_transaction_count()
    elif selected_option == 'Average Transaction Amount by State':
        display_average_transaction_amount_by_state()
    elif selected_option == 'Brand Popularity in Transactions':
        display_brand_popularity_in_transactions()
    elif selected_option == 'Yearly Comparison of Transaction Data':
        st.subheader("Yearly Comparison of Transaction Data Vizualisation")
        display_yearly_comparison_of_transaction_data()
    
elif select == "Exit":
    st.success('Thank you for your time. Exiting the application')
    st.image(Image.open(r"PhonePe-SmartSpeaker_Amitab.jpg"), width=600)
    st.balloons()