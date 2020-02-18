-- Создаем схему

CREATE SCHEMA SBX;



drop table SBX.orders_external;
drop table SBX.purchases_external;
drop table SBX.products_external;
drop table SBX.product_categories_external;
drop table SBX.categories_external;

drop table SBX.purchases_internal;
drop table SBX.products_internal;

drop table SBX.hub_products;
drop table SBX.sat_products; 

drop table SBX.hub_purchases;
drop table SBX.sat_purchases; 



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
create table if not exists SBX.products_internal (
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

-- select count(*) from SBX.products_internal;

-- 2.2 Заливаем в неё данные из внешней таблицы
-- Поскольку данные в продуктах могут изменяться, то подход с заливке данных будет немного отличаться от того, которым мы заливали данные по составам заказов. 
merge into SBX.products_internal tgt
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
		from SBX.products_external p
		inner join SBX.product_categories_external pc on pc.product_id = p.id
		inner join SBX.categories_external c on pc.category_id = c.id
		left outer join SBX.categories_external par_c1 on par_c1.id = c.parent_id
		left outer join SBX.categories_external par_c2 on par_c2.id = par_c1.parent_id
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
create table if not exists SBX.hub_products (
	hk_id integer not null
	, load_dt timestamp not null
	, product_id integer not null
	, primary key (hk_id) enabled
)
	order by hk_id
	segmented by hk_id all nodes
;
-- 3.2 заливаем в неё данные
merge into SBX.hub_products tgt
using (
	select 
		hk_id 
		, getdate() as load_dt
		, p.id as product_id
	from SBX.products_internal p
) src on tgt.hk_id = src.hk_id
when not matched then insert (hk_id, load_dt, product_id) values (src.hk_id, src.load_dt, src.product_id);

-- 3.3 Создаем SAT-таблицу для Продуктов
create table if not exists SBX.sat_products (
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
merge into SBX.sat_products tgt
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
	from SBX.products_internal
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
drop table SBX.hub_purchases ;
create table if not exists SBX.hub_purchases (
	hk_id integer not null
	, load_dt  TIMESTAMP NOT NULL
	, id  INTEGER NOT NULL
	, PRIMARY KEY (hk_id)
)
	order by hk_id
	segmented by hk_id all nodes
;
-- 4.2 И заливаем в неё данные 
merge into SBX.hub_purchases tgt 
using (
	select 
		hash(id) as  hk_id
		, getdate() as load_dt
		, id
	from SBX.purchases_internal
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
create table if not exists SBX.sat_purchases (
	hk_id integer not null
	, load_dt  TIMESTAMP NOT NULL
	, id  INTEGER NOT NULL
	, created timestamp not null
	, order_id INTEGER NOT NULL
	, product_id INTEGER NOT NULL
	, price FLOAT NOT NULL 
	, amount INTEGER NOT NULL
	, total_sum FLOAT NOT NULL 
	, PRIMARY KEY (id)
)
	order by hk_id
	segmented by hk_id all nodes
;
-- 4.4 И заливаем в неё данные 
merge into SBX.sat_purchases tgt 
using (
	select 
		hash(id) as  hk_id
		, getdate() as load_dt
		, id
		, created 
		, order_id 
		, product_id 
		, price  
		, amount 
		, price * amount as total_sum
	from SBX.purchases_internal
) src on src.hk_id = tgt.hk_id

when not matched then 
	insert (
		hk_id
		, load_dt
		, id
		, created 
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
		, src.order_id 
		, src.product_id 
		, src.price  
		, src.amount 
		, src.total_sum
	)
;

-- 5.1 Создаем таблицу с линкой между продуктами и составами заказов
create table if not exists SBX.link_purchases_products (
	product_hk_id integer not null
	, purchase_hk_id integer not null
	, primary key (product_hk_id, purchase_hk_id)
)
	order by product_hk_id, purchase_hk_id
;
-- и заливаем в неё связи
merge into SBX.link_purchases_products tgt 
using (
	select 
		hash(pur_i.id) as purchase_hk_id
		, hash(pur_i.product_id) as product_hk_id
	from SBX.purchases_internal pur_i
		inner join SBX.hub_purchases h_pur on h_pur.hk_id = hash(pur_i.id)
		inner join SBX.hub_products h_prod on h_prod.hk_id = hash(pur_i.product_id)
) src on tgt.product_hk_id = src.product_hk_id and tgt.purchase_hk_id = src.purchase_hk_id
when not matched then insert (product_hk_id, purchase_hk_id) values (src.product_hk_id, src.purchase_hk_id)
;
	
	


/*



select * from SBX.link_purchases_products;



select count(*)
from SBX.hub_products hp 
inner join SBX.sat_products sp on sp.hk_id = hp.hk_id
;


select count(*)
from SBX.hub_purchases hp 
inner join SBX.sat_purchases sp on sp.hk_id = hp.hk_id
;

*/
