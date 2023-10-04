#Загружаем библиотеки для дага
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.hooks.base import BaseHook

#Загружаем библиотеки для Python оператораd
from time import localtime, strftime
from datetime import datetime
import requests
import psycopg2
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
import pandas as pd
import re
import sys
from clickhouse_driver import Client


default_args = {
    "owner": "airflow",
    'start_date': days_ago(1)
}


def news():
    
    client = Client(host='localhost', port=8123)
    query = '''CREATE TABLE IF NOT EXISTS default.products
(
    product_id UInt8,
    product_name String,
    price Float(15,2)
)
ENGINE = MergeTree() ORDER BY (product_id);'''
    result = client.execute(query)


#определяем даг
with DAG(dag_id = "test", schedule_interval = None,
   default_args = default_args, catchup = False) as dag:
        

   create = PythonOperator(task_id = "test",
                                                python_callable = news)
    
