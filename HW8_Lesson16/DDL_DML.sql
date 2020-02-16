-- Создаем схему

CREATE SCHEMA SBX;

/*
drop table SBX.orders_external;
drop table SBX.purchases_external;
drop table SBX.products_external;
drop table SBX.product_categories_external;
drop table SBX.categories_external;

drop table SBX.purchases_internal;
drop table SBX.products_internal;
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
DROP TABLE SBX.products_internal;
create table if not exists SBX.products_internal (
	load_dt  TIMESTAMP NOT NULL
	, id  INTEGER NOT NULL
	, product_name varchar(500)
	, category_id  INTEGER NOT NULL
	, category_name varchar(500)
	, parent_category_id  INTEGER
	, parent_category_name varchar(500)
	, product_hash varchar(32) not null
	, PRIMARY KEY (id)
)
	ORDER BY id
	SEGMENTED BY HASH(id) ALL NODES
;

-- select count(*) from SBX.products_internal;

-- 2.2 Заливаем в неё данные из внешней таблицы
-- Поскольку данные в продуктах могут изменяться, то подход с заливке данных будет немного отличаться от того, которым мы заливали данные по составам заказов. 
merge into SBX.products_internal tgt
using (
	select 
		load_dt
		, id
		, product_name
		, category_id
		, category_name
		, parent_category_id
		, parent_category_name
		, md5(id || product_name || category_id || category_name || ifnull(parent_category_id,0) || ifnull(parent_category_name, 'NULL')) as product_hash
	from (
		select
			GETDATE() as load_dt
			, p.id
			, p.name as product_name
			, c.id as category_id
			, c.name as category_name
			, ifnull(par_c2.id, par_c1.id) as parent_category_id
			, ifnull(par_c2.name, par_c1.name) as parent_category_name
			, ROW_NUMBER() OVER (PARTITION BY p.id ORDER BY pc.position) AS rn
		from SBX.products_external p
		inner join SBX.product_categories_external pc on pc.product_id = p.id
		inner join SBX.categories_external c on pc.category_id = c.id
		left outer join SBX.categories_external par_c1 on par_c1.id = c.parent_id
		left outer join SBX.categories_external par_c2 on par_c2.id = par_c1.parent_id
	) main where rn = 1 
) src on tgt.id = src.id

when not matched then 
	insert (load_dt, id, product_name, category_id, category_name, parent_category_id, parent_category_name, product_hash)
	values (src.load_dt, src.id, src.product_name, src.category_id, src.category_name, src.parent_category_id, src.parent_category_name, src.product_hash)

when matched and src.product_hash <> tgt.product_hash then 
	update set 
		load_dt = src.load_dt
		, id = src.id
		, product_name = src.product_name
		, category_id = src.category_id
		, category_name = src.category_name
		, parent_category_id = src.parent_category_id
		, parent_category_name = src.parent_category_name
		, product_hash = src.product_hash
;

