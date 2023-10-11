#Загружаем библиотеки для дага
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.hooks.base import BaseHook

#Загружаем библиотеки для Python оператораt
from elasticsearch import Elasticsearch

default_args = {
    "owner": "airflow",
    'start_date': days_ago(1)
}

def get_vacancy():
    es = Elasticsearch([{'host': 'host.docker.internal', 'port': 9200, 'scheme': 'public'}])

    index_name = 'vacancy'  # Имя индекса Elasticsearch
    doc_type = 'vacancy'  # Тип документа (в более новых версиях Elasticsearch это устарело)
    doc_limit = 5000  # Количество документов, которое вы хотите вывести

    # Создайте запрос без фильтрации по _id
    query = {
        "query": {
            "match_all": {}  # Этот запрос выбирает все документы
        }
    }

    try:
        # Выполните исходный запрос, чтобы получить итератор прокрутки
        scroll = es.search(index=index_name, body=query, scroll='1m', size=doc_limit)

        # Переменная для хранения всех результатов
        all_results = []

        # Получайте итерации результатов с использованием метода прокрутки
        while scroll['hits']['hits'] and len(all_results) < doc_limit:
            # Получите текущую порцию результатов
            current_results = scroll['hits']['hits']

            # Добавьте их к общему списку результатов
            all_results.extend(current_results)

            # Прокрутите к следующей порции результатов
            scroll_id = scroll['_scroll_id']
            scroll = es.scroll(scroll_id=scroll_id, scroll='1m')

        # Теперь all_results содержит первые n результатов
        print(f"Первые {doc_limit} документов:")
        for result in all_results:
            product_data = result['_source']
            print(product_data)

    except Exception as e:
        print(f"Ошибка при выполнении запроса: {e}")

if __name__ == "__main__":
    get_vacancy()
 
 
with DAG(dag_id = "import_all_vacancies", schedule_interval = None,
   default_args = default_args, catchup = False) as dag:
        
   import_all = PythonOperator(task_id = "import_vacancies_from_elastic",
                                                python_callable = get_vacancy)

import_all