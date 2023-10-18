## Инструкция по разворачиваю Docker-Compose
### Содержание
Данный docker-compose предназначен для разворачивания сервисов:
- сервисы Airflow
- СУБД Postgres
- СУБД Elastic
- kibana 
- redis 
- pgsync (сервис для синхронизации данных между Postgres и Elastic)

### Действия
1. Скачать все файлы из репо
3. В командой строке перейти в папку с файлами
4. Запустить команду (Docker engine должен быть запущен!)
```
docker-compose up -d
```
6. В результате запустятся все контейнеры. Контейнер с airflow-init за пару минут отработает и остановится.
![image](https://github.com/Nastya224/Elastic1/assets/94219446/d9c651bc-4f83-42ab-a2c3-d4a6aabe8792)
