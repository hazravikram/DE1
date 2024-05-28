from airflow import DAG
from datetime import timedelta,datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import pandas as pd
import requests

api_key = "api_key"

default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime(2024,5,20),
    'email' : ['hazrasintu7@gmail.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 2,
    'retry_delay' : timedelta(minutes=2)
}


def etl_weather_data():
    combined_df = pd.DataFrame()
    names_of_cities = ["Delhi", "Mumbai","Kolkata","Bangalore","Chennai"]
    base_url = "https://api.openweathermap.org/data/2.5/weather?q="
    for city in names_of_cities:
        full_url = base_url + city + "&APPID=" + api_key
        r= requests.get(full_url)
        data = r.json()

        city = data["name"]
        weather_description = data["weather"][0]["description"]
        feels_like = data["main"]["feels_like"]-273.15
        temp = data["main"]["temp"]-273.15
        max_temp = data["main"]["temp_max"]-273.15
        min_temp = data["main"]["temp_min"]-273.15
        humidity = data["main"]["humidity"]
        pressure = data["main"]["pressure"]
        wind_speed = data["wind"]['speed']
        sunrise_time = datetime.utcfromtimestamp(data["sys"]["sunrise"] + data['timezone'])
        sunset_time = datetime.utcfromtimestamp(data["sys"]["sunrise"] + data['timezone'])
        timeofrecord = datetime.utcfromtimestamp(data['dt'] + data['timezone'])

        transformed_data = {
            "City" : city,
            "Description" : weather_description,
            "Feels_like" : feels_like,
            "Current_Temprature" : temp,
            "Maximum_Temprature" : max_temp,
            "Minimum_Temprature" : min_temp,
            "Humidity" : humidity,
            "Pressure" : pressure,
            "Wind_speed" : wind_speed,
            "SunRise" : sunrise_time,
            "SunSet" : sunset_time,
            "Time" : timeofrecord
        }

        transformed_data_list = [transformed_data]
        df_data = pd.DataFrame(transformed_data_list)
        combined_df = pd.concat([combined_df,df_data],ignore_index=True)

    now = datetime.now()
    file_name = 'current_weather_data'+now.strftime("%d%m%Y%H%M%S")
    combined_df.to_csv(f"/home/uk/Test/DE_Projects/{file_name}.csv",index=False)
    output_file = f"/home/uk/Test/DE_Projects/{file_name}.csv"
    return output_file






with DAG('weather_dag',
         default_args=default_args,
         schedule_interval= '@daily',
         catchup=False) as dag:
    
    extract_transform_weather_data = PythonOperator(
        task_id='tsk_extract_weather_data',
        python_callable= etl_weather_data
    )

    load_to_s3 = BashOperator(
        task_id = 'task_load_to_s3',
        bash_command= 'aws s3 mv {{ ti.xcom_pull("tsk_extract_weather_data")}} s3://mongo-bucket-24052024'
    )

    slack_notification = SlackWebhookOperator(
        task_id = 'Slack_Notification',
        http_conn_id = "slack_conn_id",
        message = "Data Moved to S3",
        channel = "Daily data loads",
    )
    extract_transform_weather_data >> load_to_s3 >> slack_notification