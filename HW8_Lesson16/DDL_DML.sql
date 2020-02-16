-- Создаем схему

CREATE SCHEMA SBX;

/*
drop table SBX.orders_external;
drop table SBX.purchases_external;
drop table SBX.products_external;
drop table SBX.product_categories_external;
drop table SBX.categories_external;
*/

-- 0. Создаем таблицы на основе внешних CSV файлов:

create external table if not exists SBX.orders_external (
    id int,
    created datetime,
    total_price FLOAT
) as copy from '/tmp/data/orders.csv' DELIMITER ',';


create external table if not exists SBX.purchases_external (
    id int,
    order_id int,
    product_id int,
    variant_id int,
    product_name varchar(500),
    variant_name varchar(500),
    price FLOAT,
	amount int,
	sku varchar(500)
) as copy from '/tmp/data/purchases.csv' DELIMITER ',';


create external table if not exists SBX.products_external (
    id int,
    brand_id int,
    name varchar(500),
    position_id int
) as copy from '/tmp/data/products.csv' DELIMITER ',';


create external table if not exists SBX.product_categories_external (
    product_id int,
    category_id int,
    position int
) as copy from '/tmp/data/product_categories.csv' DELIMITER ',';


create external table if not exists SBX.categories_external (
    id int,
    parent_id int,
    name varchar(500),
    position_id int
) as copy from '/tmp/data/categories.csv' DELIMITER ',';

-- 1.1 Создаем таблицу для хранения состава заказов

create table if not exists SBX.purchases_internal (
	load_dt  TIMESTAMP NOT NULL
	, created  TIMESTAMP NOT NULL
	, id  INTEGER NOT NULL
	, order_id INTEGER NOT NULL
	, product_id INTEGER NOT NULL
	, price FLOAT NOT NULL 
	, amount INTEGER NOT NULL
	, PRIMARY KEY (id)
)
	ORDER BY id
	SEGMENTED BY HASH(id) ALL NODES
;

-- select count(*) from SBX.purchases_internal
-- = 0

-- 1.2 Создаем вьюху для отрезания лишних данных для загрузки
create or replace view SBX.v_purchases_for_import as
select
	GETDATE() as load_dt
	, o.created
	, p.id
	, p.order_id
	, p.product_id
	, p.price
	, p.amount
from SBX.purchases_external p
inner join SBX.orders_external o on o.id = p.order_id
left outer join SBX.purchases_internal tgt on tgt.id = p.id
where tgt.load_dt is null
;

-- 1.3 Заливаем данные состава заказов
insert into SBX.purchases_internal (
	load_dt 
	, created 
	, id 
	, order_id 
	, product_id 
	, price  
	, amount 
) select 
	load_dt 
	, created 
	, id 
	, order_id 
	, product_id 
	, price  
	, amount 
	from SBX.v_purchases_for_import;

-- select count(*) from SBX.purchases_internal
-- = 23398

-- 2.1 Создаем таблицу для продуктов
select
	GETDATE() as load_dt
	, p.id
	, p.name as product_name
	, c.name as category_name
	, c.parent_id as category_parent_id
	, ifnull(par_c2.id, par_c1.id) as parent_category_id
	, ifnull(par_c2.name, par_c1.name) as parent_category_name
from SBX.products_external p
inner join SBX.product_categories_external pc on pc.product_id = p.id
inner join SBX.categories_external c on pc.category_id = c.id
left outer join SBX.categories_external par_c1 on par_c1.id = c.parent_id
left outer join SBX.categories_external par_c2 on par_c2.id = par_c1.parent_id
;
