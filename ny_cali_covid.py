# Step 1: Import necessary libraries
from urllib.request import urlopen
import json
import pandas as pd
from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks
from airflow.operators.python import PythonOperator

# Step 2: Connect to API and extract data
def covidExtract():
    api = "https://api.covidtracking.com/v1/states/ca/daily.json"
    api_opener = urlopen(api)
    data = json.load(api_opener)
    df1 = pd.DataFrame(data)
    df1 = df1[df1['date'] >= 20210101]

# Step 3: Extract data from csv and combine
    df2 = pd.read_csv(r"C:\Users\Chris\airflowhome\dags\New_york_covid_data.csv")
    df2 = df2[df2['date'] >= 20210101]
    df3 = df1.append(df2)
    return df3

# Step 3: Transform data
def covidTransform(df):
    try:
        df['dateLastUpdated'] = df["lastUpdateEt"].str.split('',expand=True)[0]
        df['timeLastUpdated'] = df["lastUpdateEt"].str.split(' ',expand=True)[1]
        df['checkInDate'] = df["checkTimeEt"].str.split(' ',expand=True)[0]
        df['checkInTime'] = df["checkTimeEt"].str.split(' ',expand=True)[1]
        df.drop('dateModified',axis=1,inplace=True)
        df.drop('date', axis = 1, inplace = True)
        df.drop('checkTimeEt', axis=1, inplace=True)
        df.drop('lastUpdateEt', axis = 1, inplace = True)
        df.drop('dateChecked', axis = 1, inplace = True)
        return df
    except KeyError:
        return df

# Step 4: Load data to bigquery table and csv file
def covidLoad(dataframe):
    dataframe = dataframe.astype(str)
    dataframe.to_gbq(destination_table='covid.states_data',
                    project_id='projects-compartment',
                    if_exists='replace')

# Step 5: Log each function call time and date
def covidLog(message):
    timestamp_format = '%H:%M:%S on %h/%d/%Y'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("covid.txt", "a") as file:
        file.write(message + " at " + timestamp + '\n')

default_args = {
    'owner': 'Christopher Wilson',
    'start_date': datetime(2022, 12, 16),
    'email': ['cwilson83@live.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='Covid-ETL',
    default_args=default_args,
    description='Simple ETL DAG using Python',
    schedule=timedelta(days=1),
)

covidExtractor = PythonOperator(
        task_id='Extract',
        python_callable=covidExtract,
        dag=dag
    )

covidTransformer = PythonOperator(
    task_id='transform',
    python_callable=covidTransform,
    dag=dag
)

covidLoader = PythonOperator(
    task_id='load',
    python_callable=covidLoad,
    dag=dag
)

covidExtractor >> covidTransformer >> covidLoader

covidLog("EXTRACTING DATA!")
covidExtract()
covidExtractedData = covidExtract()
covidLog("FINISHED EXTRACTING")
covidLog("TRANSFORMING DATA!")
covidTransform(covidExtractedData)
covidTransformedData = covidTransform(covidExtractedData)
covidLog("FINISHED TRANSFORMING!")
covidLog("LOADING DATA!")
covidLoad(covidTransformedData)
covidLog("FINISHED LOADING!")