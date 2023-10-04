#Загружаем библиотеки для дага
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.hooks.base import BaseHook

#Загружаем библиотеки для Python оператораt
from time import localtime, strftime
from datetime import datetime
import requests
import psycopg2
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
import pandas as pd
import re

default_args = {
    "owner": "airflow",
    'start_date': days_ago(1)
}

def get_conn_credentials(connection_name) -> BaseHook.get_connection:
    conn = BaseHook.get_connection(connection_name)
    return conn

#Создаем переменную с параметрами API источника Тасс
variables = Variable.set(key="news_tass",
                         value={"table_name": "news_tass",
                                "connection_name": "connection_name",
                                "url":"https://tass.ru/rss/v2.xml"},
                         serialize_json=True)
dag_variables1 = Variable.get("news_tass", deserialize_json=True)

#Создаем переменную с параметрами API источника Ведомости
variables = Variable.set(key="news_vedomosti",
                         value={"table_name": "news_vedomosti",
                                "connection_name": "connection_name",
                                "url":"https://www.vedomosti.ru/rss/news"},
                         serialize_json=True)
dag_variables2 = Variable.get("news_vedomosti", deserialize_json=True)

#Создаем переменную с параметрами API источника Ведомости
variables = Variable.set(key="news_lenta",
                         value={"table_name": "news_lenta",
                                "connection_name": "connection_name",
                                "url":"https://lenta.ru/rss/"},
                         serialize_json=True)
dag_variables3 = Variable.get("news_lenta", deserialize_json=True)

"""
#Функция загрузки новостей с источника Лента
"""
def import_news_lenta():
#Парсим данные с сайта Ленты
 url = dag_variables3.get('url')
 try:
    response= requests.get(url)
 except Exception as err:
    print(f'Error occured: {err}')
    return
 html = response.text
 soup = BeautifulSoup(html, "xml")
 date, title, category, source, link = [], [], [], [], []
 names = soup.find_all('item')
 
 #В каждой новости нахожим нужные данные
 for name in names:
    dates=name.pubDate
    date.append(dates)
    links=name.link.text
    link.append(links)
    titles=name.title.text
    title.append(titles)
    categories=name.find_all('category')
    categories=categories[0].text
    category.append(categories)
    sources='Lenta'
    source.append(sources)
    
#Создаем датафрейм данных
 news_lenta = pd.concat([pd.Series(date), pd.Series(link), pd.Series(title), pd.Series(category), pd.Series(source) ], axis=1)
 news_lenta.index += 1
 news_lenta.columns = ['date', 'link', 'title',  'category', 'source']
 news_lenta=news_lenta.explode('date')
 
#Загружаем датафрейм с новостями Ленты в БД в сырой слой данных
 engine = create_engine('postgresql://postgres:postgres@host.docker.internal:5430/postgres')
 news_lenta.to_sql('news_lenta', engine, index=False, if_exists= 'replace')
 

def import_news_tass():
#Парсим данные с сайта Тасс
 url = dag_variables1.get('url')
 try:
    response= requests.get(url)
 except Exception as err:
    print(f'Error occured: {err}')
    return
 html = response.text
 soup = BeautifulSoup(html, "xml")
 date, title, category, source, link = [], [], [], [], []
 names = soup.find_all('item')
 
 #В каждой новости нахожим нужные данные
 for name in names:
    dates=name.pubDate
    date.append(dates)
    links=name.link.text
    link.append(links)
    titles=name.title.text
    title.append(titles)
    categories=name.find_all('category')
    categories=categories[0].text
    category.append(categories)
    sources='Tass'
    source.append(sources)
#Создаем датафрейм данных
 news_tass = pd.concat([pd.Series(date), pd.Series(link), pd.Series(title), pd.Series(category), pd.Series(source) ], axis=1)
 news_tass.index += 1
 news_tass.columns = ['date', 'link', 'title', 'category', 'source']
 news_tass=news_tass.explode('date')
#Загружаем датафрейм с новостями Тасс в БД в сырой слой данных
 engine = create_engine('postgresql://postgres:postgres@host.docker.internal:5430/postgres')
 news_tass.to_sql('news_tass', engine, index=False, if_exists= 'replace')


def import_news_vedomosti():
#Парсим данные с сайта Ведомости
 url = dag_variables2.get('url')
 try:
    response= requests.get(url)
 except Exception as err:
    print(f'Error occured: {err}')
    return
 response.encoding = response.apparent_encoding
 html = response.text
 soup = BeautifulSoup(html, "xml")
 date, title, category, source, link = [], [], [], [], []
 names = soup.find_all('item')

 #В каждой новости нахожим нужные данные
 for name in names:
    dates=name.pubDate
    date.append(dates)
    links=name.link.text
    link.append(links)
    titles=name.title.text
    title.append(titles)
    categories=name.find_all('category')
    categories=categories[0].text
    category.append(categories)
    sources='Vedmosti'
    source.append(sources)
#Создаем датафрейм данных
 news_vedomosti = pd.concat([pd.Series(date), pd.Series(link), pd.Series(title), pd.Series(category), pd.Series(source) ], axis=1)
 news_vedomosti.index += 1
 news_vedomosti.columns = ['date', 'link', 'title', 'category', 'source']
 news_vedomosti=news_vedomosti.explode('date')
#Загружаем датафрейм с новостями Ведомостей в БД в сырой слой данных
 engine = create_engine('postgresql://postgres:postgres@host.docker.internal:5430/postgres')
 news_vedomosti.to_sql('news_vedomosti', engine, index=False, if_exists= 'replace')

"""
#Функция создания core слоя в БД
"""
def news_make_core():
    
#Подключаемся к Postgres при помощи Connections в Airflow    
    pg_conn = get_conn_credentials(dag_variables1.get('connection_name'))
    pg_hostname, pg_port, pg_username, pg_pass, pg_db = pg_conn.host, pg_conn.port, pg_conn.login, pg_conn.password, pg_conn.schema
    conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)
#Так как категории новостей в разных источниках могут различаться, 
# то создаем дополнительный справочник   
    cursor = conn.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS public.categories_common 
                         (CategoryID serial PRIMARY KEY, Category_common VARCHAR(100));""")
    cursor.execute("""INSERT INTO public.categories_common (Category_common) VALUES
                         ('Экономика/Бизнес'), ('Финансы/ Инвестиции'), ('Общество'),
                         ('Россия'), ('Москва'),('Политика'), 
                         ('Технологии'), ('Мир'), ('Культура'),
                         ('Путешествия'), ('Недвижимость'), ('Спорт'), 
                         ('Другое'), ('Интернет/ СМИ'), ('Происшествия');""")
    cursor.execute("""CREATE TABLE IF NOT EXISTS public.categories 
                         (ID serial PRIMARY KEY, Category_common int, Category VARCHAR, 
                         FOREIGN KEY (Category_common) REFERENCES categories_common (CategoryID));""")
#Заполняем справочники с категориями со слоя с сырыми данными 
# (как будут объединяться категории нужно уточнять у аналитиков    
    cursor.execute("""INSERT INTO public.categories (Category) 
                         SELECT DISTINCT Category from news_lenta;""")
    cursor.execute("""INSERT INTO public.categories (Category) 
                         SELECT DISTINCT Category from news_tass;""")
    cursor.execute("""INSERT INTO public.categories (Category) 
                         SELECT DISTINCT Category from news_vedomosti;""")
    cursor.execute("""UPDATE public.categories
                         SET Category_common =
                           CASE 
	                           WHEN Category LIKE ANY (ARRAY['%кономика%', '%изнеc']) THEN 1
	                           WHEN Category LIKE ANY (ARRAY['%нвестиции%', '%инансы%']) THEN 2
	                           WHEN Category LIKE ANY (ARRAY['%реда обитания%', '%бщество%', '%жизни%', '%енности%',  '%абота%']) THEN 3
	                           WHEN Category LIKE ANY (ARRAY['%стран%', '%оссия%', '%9 паралл%']) THEN 4
	                           WHEN Category LIKE ANY (ARRAY['%осква%', '%еверо-Запад%']) THEN 5
	                           WHEN Category LIKE ANY (ARRAY['%олити%', '%иловые%', '%рмия%']) THEN 6
	                           WHEN Category LIKE ANY (ARRAY['%ехнолог%', '%аука%']) THEN 7
	                           WHEN Category LIKE ANY (ARRAY['%еждунар%', 'Мир%']) THEN 8
	                           WHEN Category like '%ультур%' THEN 9
	                           WHEN Category LIKE ANY (ARRAY['%нтернет%', '%овости партнер%']) THEN 14
	                           WHEN Category like '%утешествия%' THEN 10
	                           WHEN Category like '%роисшествия%' THEN 15
	                           WHEN Category like '%едвижимост%' THEN 11
	                           WHEN Category like '%порт%' THEN 12
	                           ELSE 13
                            END
                         WHERE Category IS NOT NULL;""")
#Заполняем справочник с источниками
    cursor.execute("""CREATE TABLE IF NOT EXISTS public.sources 
                         (SourceID serial PRIMARY KEY, Source VARCHAR(30));""")
    cursor.execute("""INSERT INTO public.sources (source) 
                         SELECT DISTINCT Source FROM public.news_tass;""")
    cursor.execute("""INSERT INTO public.sources (source) 
                         SELECT DISTINCT Source FROM public.news_lenta;""")
    cursor.execute("""INSERT INTO public.sources (source) 
                         SELECT DISTINCT Source FROM public.news_vedomosti;""")
#Создаем и заполняем таблицу со всеми новостями со слоя с сырыми данными
    cursor.execute("""CREATE TABLE IF NOT EXISTS public.news 
                         (ID serial PRIMARY KEY, Date_new TIMESTAMP, Link VARCHAR(200), 
                         Title VARCHAR(200), Category_common int, 
                         FOREIGN KEY (Category_common) REFERENCES categories_common (CategoryID), 
                         SourceID int, FOREIGN KEY (SourceID) REFERENCES sources (SourceID));;""")
    cursor.execute("""INSERT INTO public.news (Date_new, Link, Title, Category_common, Sourceid)
                         SELECT CAST (Date as TiMESTAMP), Link, Title, Category_common, Sourceid
                         FROM public.news_tass t
                         JOIN categories c ON t.Category=c.Category
                         JOIN sources s ON t.Source=s.Source;""")
    cursor.execute("""INSERT INTO public.news (Date_new, Link, Title, Category_common, Sourceid)
                         SELECT CAST (Date as TiMESTAMP), Link, Title, Category_common, Sourceid
                         FROM public.news_lenta t
                         JOIN categories c ON t.Category=c.Category
                         JOIN sources s ON t.Source=s.Source;""")
    cursor.execute("""INSERT INTO public.news (Date_new, Link, Title, Category_common, Sourceid)
                         SELECT CAST (Date as TiMESTAMP), Link, Title, Category_common, Sourceid
                         FROM public.news_vedomosti t
                         JOIN categories c ON t.Category=c.Category
                         JOIN sources s ON t.Source=s.Source;""")
    conn.commit()
    cursor.close()
    conn.close()



def news_make_vitrina():
    
#Подключаемся к Postgres при помощи Connections в Airflow    
    pg_conn = get_conn_credentials(dag_variables1.get('connection_name'))
    pg_hostname, pg_port, pg_username, pg_pass, pg_db = pg_conn.host, pg_conn.port, pg_conn.login, pg_conn.password, pg_conn.schema
    conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)
#Cоздаем витрину    
    cursor = conn.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS public.vitrina_news 
                         (CategoryID int, Category VARCHAR(100), Count_news_all_sources int,
                         Count_news_tass int, Count_news_lenta int, Count_news_vedomosti int,   
                         Count_news_all_sources_24_hours int, Count_news_tass_24_hours int, 
                         Count_news_lenta_24_hours int, Count_news_vedomosti_24_hours int,
                         Avg_count_news_per_day float, Date_with_max_count_news DATE, 
                         Count_news_mon int, Count_news_tue int, Count_news_wed int, 
                         Count_news_thr int, Count_news_fr int, Count_news_sat int, Count_news_sun int);""")
    cursor.execute("""INSERT INTO public.vitrina_news (Categoryid, Category, Count_news_all_sources)
                         SELECT n.Category_common, c.Category_common, COUNT(*)
                           FROM public.news n
                           JOIN public.categories_common c ON c.Categoryid=n.Category_common
                           GROUP BY 1,2
                           ORDER BY 1;""")
    cursor.execute("""WITH count AS (
                                SELECT DISTINCT Category_common, COUNT(*) as co
                                FROM public.news n WHERE sourceid=1 GROUP BY 1)
                      UPDATE vitrina_news
                                SET Count_news_tass = (SELECT co FROM count c 
                                WHERE  vitrina_news.Categoryid = c.Category_common);""")
    cursor.execute("""WITH count AS (SELECT DISTINCT Category_common, COUNT(*) as co
                                FROM public.news n WHERE sourceid=2 GROUP BY 1)
                      UPDATE vitrina_news
                                SET Count_news_lenta= (SELECT co FROM count c 
                                WHERE  vitrina_news.Categoryid = c.Category_common);""")
    cursor.execute("""WITH count AS (SELECT DISTINCT Category_common, COUNT(*) as co
                                FROM public.news n WHERE sourceid=3 GROUP BY 1)
                      UPDATE vitrina_news
                                SET Count_news_vedomosti= (SELECT co FROM count c 
                                WHERE  vitrina_news.Categoryid = c.Category_common);""")
    cursor.execute("""WITH count AS (SELECT DISTINCT Category_common, COUNT(*) as co
                                FROM public.news n WHERE EXTRACT(DAY FROM now()-date_new)<1
                                GROUP BY 1)
                      UPDATE vitrina_news
                                SET Count_news_all_sources_24_hours = (SELECT co FROM count c 
                                WHERE vitrina_news.Categoryid = c.Category_common);""")
    cursor.execute("""WITH count AS (SELECT DISTINCT Category_common, COUNT(*) as co
                                FROM public.news n WHERE EXTRACT(DAY FROM now()-date_new)<1 and sourceid=1 
                                GROUP BY 1)
                      UPDATE vitrina_news
                                SET Count_news_tass_24_hours = (SELECT co FROM count c 
                                WHERE  vitrina_news.Categoryid = c.Category_common);""")
    cursor.execute("""WITH count AS (SELECT DISTINCT Category_common, COUNT(*) as co
                                FROM public.news n WHERE EXTRACT(DAY FROM now()-date_new)<1 and sourceid=2 
                                GROUP BY 1)
                      UPDATE vitrina_news
                                SET Count_news_lenta_24_hours = (SELECT co FROM count c 
                                WHERE  vitrina_news.Categoryid = c.Category_common);""")
    cursor.execute("""WITH count AS (SELECT DISTINCT Category_common, COUNT(*) as co
                                FROM public.news n WHERE EXTRACT(DAY FROM now()-date_new)<1 and sourceid=3 
                                GROUP BY 1)
                      UPDATE vitrina_news
                                SET Count_news_vedomosti_24_hours = (SELECT co FROM count c 
                                WHERE  vitrina_news.Categoryid = c.Category_common);""")
    cursor.execute("""WITH count AS (SELECT category_common, date_new, COUNT(*) as co
                                FROM public.news n GROUP BY 1,2),
                           avg_count AS (SELECT category_common, avg(co) as avg_news
                                FROM count c GROUP BY 1)
                      UPDATE vitrina_news
                                SET Avg_count_news_per_day = (SELECT avg_news from avg_count a 
                                WHERE vitrina_news.Categoryid = a.Category_common);""")
    cursor.execute("""WITH counts AS (SELECT category_common, date_trunc('day', date_new) as day_new, count(*) as co_new
                                FROM public.news n GROUP BY 1,2),
                                max_counts AS ((SELECT category_common, max(co_new) as max_c
                                FROM counts c GROUP BY 1))
                      UPDATE vitrina_news
                                SET Date_with_max_count_news = (SELECT day_new FROM counts c
                                JOIN max_counts m on m.category_common=c.category_common
                                WHERE co_new=max_c AND
                                vitrina_news.Categoryid = m.Category_common LIMIT 1);""")
    cursor.execute("""UPDATE vitrina_news
                                SET Count_news_mon = (SELECT COUNT(*)
                                FROM public.news n
                                WHERE DATE_PART('dow', date_new)=1 AND
                                vitrina_news.Categoryid = n.Category_common);""")
    cursor.execute("""UPDATE vitrina_news
                                SET Count_news_tue = (SELECT COUNT(*)
                                FROM public.news n
                                WHERE DATE_PART('dow', date_new)=2 AND
                                vitrina_news.Categoryid = n.Category_common);""")
    cursor.execute("""UPDATE vitrina_news
                                SET Count_news_wed = (SELECT COUNT(*)
                                FROM public.news n
                                WHERE DATE_PART('dow', date_new)=3 AND
                                vitrina_news.Categoryid = n.Category_common);""")
    cursor.execute("""UPDATE vitrina_news
                                SET Count_news_thr = (SELECT COUNT(*)
                                FROM public.news n
                                WHERE DATE_PART('dow', date_new)=4 AND
                                vitrina_news.Categoryid = n.Category_common);""")
    cursor.execute("""UPDATE vitrina_news
                                SET Count_news_fr = (SELECT COUNT(*)
                                FROM public.news n
                                WHERE DATE_PART('dow', date_new)=5 AND
                                vitrina_news.Categoryid = n.Category_common);""")
    cursor.execute("""UPDATE vitrina_news
                                SET Count_news_sat = (SELECT COUNT(*)
                                FROM public.news n
                                WHERE DATE_PART('dow', date_new)=6 AND
                                vitrina_news.Categoryid = n.Category_common);""")
    cursor.execute("""UPDATE vitrina_news
                                SET Count_news_sun = (SELECT COUNT(*)
                                FROM public.news n
                                WHERE DATE_PART('dow', date_new)=7 AND
                                vitrina_news.Categoryid = n.Category_common);""")
    conn.commit()
    cursor.close()
    conn.close()



#определяем даг
with DAG(dag_id = "News_init", schedule_interval = None,
   default_args = default_args, catchup = False) as dag:
        

   import_lenta = PythonOperator(task_id = "import_news_lenta_init",
                                                python_callable = import_news_lenta)
   
   import_tass = PythonOperator(task_id = "import_news_tass_init",
                                                python_callable = import_news_tass)
   
   import_vedomosti = PythonOperator(task_id = "import_news_vedomosti_init",
                                                python_callable = import_news_vedomosti)
   
   news_core = PythonOperator(task_id = "news_make_core_init",
                                                python_callable = news_make_core)
   news_vitrina = PythonOperator(task_id = "news_make_vitrina_init",
                                                python_callable = news_make_vitrina)
    
    
#Определяем последовательность тасков    
import_lenta >> news_core >> news_vitrina
import_tass >> news_core >> news_vitrina
import_vedomosti >> news_core >> news_vitrina