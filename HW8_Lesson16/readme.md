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

Поскольку в сущности "Заказы" (Orders) отсутствуют какие-либо полезныые данные кроме даты, эту дату мы прокинем в сущность "Состав заказа" (Purchases) и таким образом избавимся от необходимости в сущности "Заказы".

Допустим, что целевое хранилище должно быть спроектировано таким образом, чтобы можно было анализировать данные в разрезах:
* Миниммальное, масимальное и среднее кол-во заказанных единиц товаров по месяцам и годам
* Доход по основным категориям (Product Categories.Position = 0) или по родительским категориям основных категорий (категории имеют некую вложенность, но у всех у них есть первоначальный родитель, вот речь о нем), так же с возможностью отсекать\группировать данные по годам и месяцам.

*Имея такия требования, получаем следующую схему нашего DWH:*

![ERD_DWH_DV2](https://raw.githubusercontent.com/adm-8/otus-de-andreevds-2019-11/master/HW8_Lesson16/_images/erd_DWH_dv2.jpg)


### Разворачивание базы

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

Соединение есть, прекрасно! 

[Теперь можно выполнять наш DDL & DML](https://github.com/adm-8/otus-de-andreevds-2019-11/blob/master/HW8_Lesson16/DDL_DML.sql)

## Получение данных:
После того как мы создали все таблицы, вьюхи, проекции и залили данные, можно погонять [!запросики витрин](https://github.com/adm-8/otus-de-andreevds-2019-11/blob/master/HW8_Lesson16/DataMarts.sql) : 

```
-- Витрина "Самые покупаемые категрии товаров с группировкой по годам и месяцам"
-- explain
select c_year , c_month, category_name, count(category_name) as cnt
from sbx.link_purchases_products lpp
inner join sbx.sat_products spr on spr.hk_id = lpp.product_hk_id
inner join sbx.sat_purchases spu on spu.hk_id = lpp.purchase_hk_id
group by category_name, c_year, c_month
order by cnt desc
;
```
![DataMart_count_by_cat_name](https://raw.githubusercontent.com/adm-8/otus-de-andreevds-2019-11/master/HW8_Lesson16/_images/DataMart_count_by_cat_name.jpg)

```
-- Витрина "Самые дохоные товары с группировкой по годам и месяцам"
-- explain
select c_year, c_month, product_name, sum(amount) total_count , sum(total_sum) total_sum 
from sbx.link_purchases_products lpp
inner join sbx.sat_products spr on spr.hk_id = lpp.product_hk_id
inner join sbx.sat_purchases spu on spu.hk_id = lpp.purchase_hk_id
group by c_year, c_month, product_name 
order by total_sum desc
;
```
![DataMart_total_sum_by_product](https://raw.githubusercontent.com/adm-8/otus-de-andreevds-2019-11/master/HW8_Lesson16/_images/DataMart_total_sum_by_product.jpg)


*Если выполнить EXPLAIN последнего запроса, то можно увидеть, что проекуцию мы делали не зря =) и судя по explain'ам мы получаем 2.5-3х меньше Cost на наши запросы витрин*

![DataMart_total_sum_by_product](https://raw.githubusercontent.com/adm-8/otus-de-andreevds-2019-11/master/HW8_Lesson16/_images/DataMart_total_sum_by_product_explain.jpg)



