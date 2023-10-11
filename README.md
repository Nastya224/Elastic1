Сравнение 2 вариантов: 

1 вариант) транзакционная Postgres + аналитическая Clickhouse (синхронизация через движки Clickhouse: PostgreSQL и MaterializedPostgreSQL)
2 вариант) транзакционная Postgres + аналитическая Elastic (синхронизация через сервис Pgsync)

Проведено тестирование по выводу данных из аналитической БД. 
Результаты:

![image](https://github.com/Nastya224/Elastic1/assets/94219446/d9ec904f-484c-4804-a94f-674603caf649)


Сама схема: 
![Концептуальная_схема](https://github.com/Nastya224/Elastic1/assets/94219446/6b8bf4b0-9b78-47fd-b6b6-e799a6e42b4b)
