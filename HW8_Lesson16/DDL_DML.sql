-- Создаем схему

DROP PROJECTION IF EXISTS sbx.p_purchases_products_year_month_all_product_info;

drop table if exists sbx.orders_external;
drop table if exists sbx.purchases_external;
drop table if exists sbx.products_external;
drop table if exists sbx.product_categories_external;
drop table if exists sbx.categories_external;

drop table if exists sbx.purchases_internal;
drop table if exists sbx.products_internal;

drop table if exists sbx.link_purchases_products;

drop table if exists sbx.hub_products;
drop table if exists sbx.sat_products; 

drop table if exists sbx.hub_purchases;
drop table if exists sbx.sat_purchases; 


drop view if exists sbx.v_purchases_for_import;

DROP SCHEMA IF EXISTS sbx;
CREATE SCHEMA sbx;



-- 0. Создаем таблицы на основе внешних CSV файлов:

create external table if not exists sbx.orders_external (
    id int,
    created datetime,
    total_price FLOAT
) as copy from '/tmp/data/orders.csv' DELIMITER ',';


create external table if not exists sbx.purchases_external (
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


create external table if not exists sbx.products_external (
    id int,
    brand_id int,
    name varchar(500),
    position_id int
) as copy from '/tmp/data/products.csv' DELIMITER ',';


create external table if not exists sbx.product_categories_external (
    product_id int,
    category_id int,
    position int
) as copy from '/tmp/data/product_categories.csv' DELIMITER ',';


create external table if not exists sbx.categories_external (
    id int,
    parent_id int,
    name varchar(500),
    position_id int
) as copy from '/tmp/data/categories.csv' DELIMITER ',';

-- 1.1 Создаем таблицу для хранения состава заказов

create table if not exists sbx.purchases_internal (
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

-- select count(*) from sbx.purchases_internal
-- = 0

-- 1.2 Создаем вьюху для отрезания лишних данных для загрузки
create or replace view sbx.v_purchases_for_import as
select
	GETDATE() as load_dt
	, o.created
	, p.id
	, p.order_id
	, p.product_id
	, p.price
	, p.amount
from sbx.purchases_external p
inner join sbx.orders_external o on o.id = p.order_id
left outer join sbx.purchases_internal tgt on tgt.id = p.id
where tgt.load_dt is null
;

-- 1.3 Заливаем данные состава заказов
insert into sbx.purchases_internal (
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
	from sbx.v_purchases_for_import;

-- select count(*) from sbx.purchases_internal
-- = 23398

-- 2.1 Создаем таблицу для продуктов
create table if not exists sbx.products_internal (
	load_dt  TIMESTAMP NOT NULL
	, id  INTEGER NOT NULL
	, hk_id  INTEGER NOT NULL
	, product_name varchar(500)
	, category_id  INTEGER NOT NULL
	, category_name varchar(500)
	, parent_category_id  INTEGER
	, parent_category_name varchar(500)
	, product_hash varchar(32) not null
	, PRIMARY KEY (hk_id)
)
	ORDER BY hk_id
	SEGMENTED BY hk_id ALL NODES
;

-- select count(*) from sbx.products_internal;

-- 2.2 Заливаем в неё данные из внешней таблицы
-- Поскольку данные в продуктах могут изменяться, то подход с заливке данных будет немного отличаться от того, которым мы заливали данные по составам заказов. 
merge into sbx.products_internal tgt
using (
	select 
		load_dt
		, id
		, hash(id) as hk_id
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
		from sbx.products_external p
		inner join sbx.product_categories_external pc on pc.product_id = p.id
		inner join sbx.categories_external c on pc.category_id = c.id
		left outer join sbx.categories_external par_c1 on par_c1.id = c.parent_id
		left outer join sbx.categories_external par_c2 on par_c2.id = par_c1.parent_id
	) main where rn = 1 
) src on tgt.hk_id = src.hk_id

when not matched then 
	insert (load_dt, id, hk_id, product_name, category_id, category_name, parent_category_id, parent_category_name, product_hash)
	values (src.load_dt, src.id, src.hk_id, src.product_name, src.category_id, src.category_name, src.parent_category_id, src.parent_category_name, src.product_hash)

when matched and src.product_hash <> tgt.product_hash then 
	update set 
		load_dt = src.load_dt
		, product_name = src.product_name
		, category_id = src.category_id
		, category_name = src.category_name
		, parent_category_id = src.parent_category_id
		, parent_category_name = src.parent_category_name
		, product_hash = src.product_hash
;

-- 3.1 Создаем HUB-таблицу для Продуктов
create table if not exists sbx.hub_products (
	hk_id integer not null
	, load_dt timestamp not null
	, product_id integer not null
	, primary key (hk_id) enabled
)
	order by hk_id
	segmented by hk_id all nodes
;
-- 3.2 заливаем в неё данные
merge into sbx.hub_products tgt
using (
	select 
		hk_id 
		, getdate() as load_dt
		, p.id as product_id
	from sbx.products_internal p
) src on tgt.hk_id = src.hk_id
when not matched then insert (hk_id, load_dt, product_id) values (src.hk_id, src.load_dt, src.product_id);

-- 3.3 Создаем SAT-таблицу для Продуктов
create table if not exists sbx.sat_products (
	hk_id integer not null 
	, product_hash varchar(32) not null
	, load_dt timestamp not null
	, product_id integer not null
	, product_name varchar(500)
	, category_id  INTEGER NOT NULL
	, category_name varchar(500)
	, parent_category_id  INTEGER
	, parent_category_name varchar(500)
	, primary key (hk_id) enabled
)
	order by hk_id
	segmented by hk_id all nodes
;
-- 3.4 Заливаем в неё данные
merge into sbx.sat_products tgt
using (
select 
		GETDATE() as load_dt
		, hash(id) as hk_id
		, id as product_id
		, product_name
		, category_id
		, category_name
		, parent_category_id
		, parent_category_name
		, product_hash
	from sbx.products_internal
) src on tgt.hk_id = src.hk_id

when not matched then insert (
	load_dt
	, hk_id
	, product_id
	, product_name
	, category_id
	, category_name
	, parent_category_id
	, parent_category_name
	, product_hash
) values (
	src.load_dt
	, src.hk_id
	, src.product_id
	, src.product_name
	, src.category_id
	, src.category_name
	, src.parent_category_id
	, src.parent_category_name
	, src.product_hash

)

when matched and tgt.product_hash <> src.product_hash then update set
	load_dt = src.load_dt
	, product_name = src.product_name
	, category_id = src.category_id
	, category_name = src.category_name
	, parent_category_id = src.parent_category_id
	, parent_category_name = src.parent_category_name
	, product_hash = src.product_hash
;


-- 4.1 Создаем SAT-таблицу для Состава 
drop table sbx.hub_purchases ;
create table if not exists sbx.hub_purchases (
	hk_id integer not null
	, load_dt  TIMESTAMP NOT NULL
	, id  INTEGER NOT NULL
	, PRIMARY KEY (hk_id)
)
	order by hk_id
	segmented by hk_id all nodes
;
-- 4.2 И заливаем в неё данные 
merge into sbx.hub_purchases tgt 
using (
	select 
		hash(id) as  hk_id
		, getdate() as load_dt
		, id
	from sbx.purchases_internal
) src on src.hk_id = tgt.hk_id

when not matched then 
	insert (
		hk_id
		, load_dt
		, id
	) values (
		src.hk_id
		, src.load_dt
		, src.id
	)
;

-- 4.3 Создаем SAT-таблицу для Состава Заказа
create table if not exists sbx.sat_purchases (
	hk_id integer not null
	, load_dt  TIMESTAMP NOT NULL
	, id  INTEGER NOT NULL
	, created timestamp not null
	, c_year INT NOT NULL
	, c_month INT NOT NULL
	, order_id INTEGER NOT NULL
	, product_id INTEGER NOT NULL
	, price FLOAT NOT NULL 
	, amount INTEGER NOT NULL
	, total_sum FLOAT NOT NULL 
	, PRIMARY KEY (hk_id)
)
	order by hk_id
	segmented by hk_id all nodes
;
-- 4.4 И заливаем в неё данные 
merge into sbx.sat_purchases tgt 
using (
	select 
		hash(id) as  hk_id
		, getdate() as load_dt
		, id
		, created 
		, YEAR(created::DATE) as c_year
		, MONTH(created::DATE) AS c_month
		, order_id 
		, product_id 
		, price  
		, amount 
		, price * amount as total_sum
	from sbx.purchases_internal
) src on src.hk_id = tgt.hk_id

when not matched then 
	insert (
		hk_id
		, load_dt
		, id
		, created 
		, c_year
		, c_month
		, order_id 
		, product_id 
		, price  
		, amount 
		, total_sum
	) values (
		src.hk_id
		, src.load_dt
		, src.id
		, src.created 
		, src.c_year
		, src.c_month
		, src.order_id 
		, src.product_id 
		, src.price  
		, src.amount 
		, src.total_sum
	)
;

-- 5.1 Создаем таблицу с линкой между продуктами и составами заказов
create table if not exists sbx.link_purchases_products (
	product_hk_id integer not null
	, purchase_hk_id integer not null
	, primary key (product_hk_id, purchase_hk_id)
	, CONSTRAINT fk_link_pp_product_hk_id foreign key (product_hk_id) references sbx.sat_products (hk_id)
	, CONSTRAINT fk_link_pp_purchase_hk_id foreign key (purchase_hk_id) references sbx.sat_purchases (hk_id)
)
	order by product_hk_id, purchase_hk_id
;
-- и заливаем в неё связи
merge into sbx.link_purchases_products tgt 
using (
	select 
		hash(pur_i.id) as purchase_hk_id
		, hash(pur_i.product_id) as product_hk_id
	from sbx.purchases_internal pur_i
		inner join sbx.hub_purchases h_pur on h_pur.hk_id = hash(pur_i.id)
		inner join sbx.hub_products h_prod on h_prod.hk_id = hash(pur_i.product_id)
) src on tgt.product_hk_id = src.product_hk_id and tgt.purchase_hk_id = src.purchase_hk_id
when not matched then insert (product_hk_id, purchase_hk_id) values (src.product_hk_id, src.purchase_hk_id)
;
	
	
COMMIT;



-- 6.1 Создаем проекцию для витрин "Самые покупаемые категрии товаров с группировкой по годам и месяцам"

CREATE PROJECTION IF NOT EXISTS sbx.p_purchases_products_year_month_all_product_info AS  
select c_year, c_month, parent_category_name, category_name, product_name, amount, total_sum
from sbx.link_purchases_products lpp
inner join sbx.sat_products spr on spr.hk_id = lpp.product_hk_id
inner join sbx.sat_purchases spu on spu.hk_id = lpp.purchase_hk_id
order by c_year, c_month, parent_category_name, category_name, product_name;

-- обновляем данные для проекций
select start_refresh();

/*



-- Витрина "Самые покупаемые категрии товаров с группировкой по годам и месяцам"
--explain
select c_year , c_month, category_name, count(category_name) as cnt
from sbx.link_purchases_products lpp
inner join sbx.sat_products spr on spr.hk_id = lpp.product_hk_id
inner join sbx.sat_purchases spu on spu.hk_id = lpp.purchase_hk_id
group by category_name, c_year, c_month
order by cnt desc
;

-- Витрина "Самые дохоные товары с группировкой по годам и месяцам"
-- explain
select c_year, c_month, product_name, sum(amount) total_count , sum(total_sum) total_sum 
from sbx.link_purchases_products lpp
inner join sbx.sat_products spr on spr.hk_id = lpp.product_hk_id
inner join sbx.sat_purchases spu on spu.hk_id = lpp.purchase_hk_id
group by c_year, c_month, product_name 
order by total_sum desc
;


*/