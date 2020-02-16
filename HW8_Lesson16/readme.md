# HW8_Lesson16 - проектирование витрины в Vertica (BigQuery).

## Цель: 
Спроектировать схему данных + Построить витрину Использовать Vertica (Docker) или BigQuery 
Датасет: Захват данных из divolte (или GCP Public Datasets) 
Definition of Done:  DDL объектов  DML шагов преобразований 
Опционально: Тестирование на наличие ошибок в данных

## Решение:

**Т.к. не было чёткого требования к источнику данных, да и суть ДЗ - это именно поделать витринки в целом, а не из поработать с каким-то источником данных, было принято решение выцыганить данные продаж интернет магазина у друзей =) **


### Схема данных в источнике:
![ERD](https://raw.githubusercontent.com/adm-8/otus-de-andreevds-2019-11/master/HW8_Lesson16/_images/erd_source.jpg)

### Схема DWH по методологии Data Vault 
![ERD_DWH_DV2](https://raw.githubusercontent.com/adm-8/otus-de-andreevds-2019-11/master/HW8_Lesson16/_images/erd_DWH_dv2.jpg)


В моем случае саму Vertica я буду разворачивать на виртуалке с CentOS7, а конектиться к ней с виндовой машины. Погнали! Заходим на машине с CentOS7 в нужную папку куда будем клонировать репозиторий, в моем случае это:
```
 cd /home/git
```

Клонируем наш чудо-репозиторий:
```
git clone https://github.com/adm-8/otus-de-andreevds-2019-11.git
```
 
Захоим в папку с ДЗ:
```
cd /home/git/otus-de-andreevds-2019-11/HW8_Lesson16
```

Качаем докер с Vertica:
```
docker pull dataplatform/docker-vertica
```

Запускаем докер, подпихнув папку с данными:
```
docker run -p 5433:5433 -d -v /home/git/otus-de-andreevds-2019-11/HW8_Lesson16/data:/tmp/data dataplatform/docker-vertica
```

После того как запустился докер с вертикой, на виндовой (ну или любой другой где есть клиент vsql) машине соединяемся с базой. Соединение без пароля, указываем только имя пользователя dbadmin и IP адрес с вертикой, в моем случае получилось:
```
cmd /K chcp 65001

vsql -h192.168.247.131 -Udbadmin
```
![Connection_OK](https://raw.githubusercontent.com/adm-8/otus-de-andreevds-2019-11/master/HW8_Lesson16/_images/connection_ok.JPG)

Соединение есть, прекрасно! Теперь можно выполнять наш [!DDL & DML](https://github.com/adm-8/otus-de-andreevds-2019-11/blob/master/HW8_Lesson16/DDL_DML.sql)
