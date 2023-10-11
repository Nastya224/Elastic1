#Загружаем библиотеки для дага
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


#Загружаем библиотеки для Python оператораt
import requests
import psycopg2
from bs4 import BeautifulSoup
import pandas as pd
import re

default_args = {
    "owner": "airflow",
    'start_date': days_ago(1)
}

def get_vacancy_habr():
    URL = "https://career.habr.com/vacancies?page=1&sort=date&type=all"
    HEADERS = {
        "Accept": "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,*/*;q=0.8",
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu;"
        " Linux x86_64; rv:98.0) Gecko/20100101 Firefox/98.0",
    }

    db_params = {
        "dbname": "postgres",
        "user": "postgres",
        "password": "postgres",
        "host": "host.docker.internal",  # Или другой хост, если PostgreSQL запущен на другом компьютере
        "port": 5430,  # Укажите порт 5430
    }

    def get_html(url, params=""):
        # Парсим данные страницы
        r = requests.get(url, headers=HEADERS, params=params)
        return r

    def get_vacancy(html):
        soup = BeautifulSoup(html, "html.parser")
        items = soup.find_all("div", class_="vacancy-card")

        vacancy = []

        for item in items:
            description_url = "https://career.habr.com" + item.find("a", class_="vacancy-card__title-link").get("href")
            description_html = get_html(description_url).text
            description_soup = BeautifulSoup(description_html, 'lxml')
            description_text = description_soup.find("div", class_="vacancy-description__text")
            description = ' '.join(description_text.stripped_strings) if description_text else ""

            vacancy.append(
                {
                    "company": item.find("div", class_="vacancy-card__company-title").get_text("href"),
                    "vacancy": item.find("a", class_="vacancy-card__title-link").get_text(strip=False),
                    "skills": item.find("div", class_="vacancy-card__skills").get_text(strip=False),
                    "meta": item.find("div", class_="vacancy-card__meta").get_text(strip=False),
                    "salary": item.find("div", class_="basic-salary").get_text(strip=False),
                    "date": item.find("time", class_="basic-date").get_text(strip=False),
                    "link_vacancy": "https://career.habr.com" + item.find("a", class_="vacancy-card__title-link").get("href"),
                    "description": description,
                }
            )

        return vacancy

    def save_vacancy_to_db(items):
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        cursor.execute("""CREATE TABLE IF NOT EXISTS public.vacancies (id serial PRIMARY KEY, company VARCHAR(100), vacancy VARCHAR(100), 
                                                                    skills VARCHAR(1000), meta VARCHAR(100), 
                                                                    salary VARCHAR(100), date VARCHAR(200), link_vacancy VARCHAR(300),
                                                                    description TEXT);""")
        for item in items:
            cursor.execute(
                "INSERT INTO vacancies (company, vacancy, skills, meta, salary, date, link_vacancy, description) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (item["company"], item["vacancy"], item["skills"], item["meta"], item["salary"], item["date"], item["link_vacancy"], item["description"])
            )

        conn.commit()
        conn.close()

    paginator = int(80)  # количество страниц для парсинга
    vacancy = []  # Создаем пустой список для хранения результатов

    for page in range(1, paginator):
        print(f"идет парсинг страницы №{page}")
        html = get_html(URL, params={"page": page})
        vacancy.extend(get_vacancy(html.text))

    save_vacancy_to_db(vacancy)
    print("Парсинг и сохранение в базу данных успешно завершены.")

if __name__ == "__main__":
    main()

#определяем даг
with DAG(dag_id = "import_vacancy_habr_to_postgres", schedule_interval = None,
   default_args = default_args, catchup = False) as dag:
        

   import_vacancy_habr = PythonOperator(task_id = "import_vacancy_habr_to_postgres"",
                                                python_callable = get_vacancy_habr)
 
#Определяем последовательность тасков    
import_vacancy_habr