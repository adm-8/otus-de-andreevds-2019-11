-- Создаем таблицы на основе внешних CSV файлов:

create external table if not exists orders_external (
    id int,
    created datetime,
    total_price FLOAT
) as copy from '/tmp/data/orders.csv' DELIMITER ',';


create external table if not exists purchases_external (
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


create external table if not exists products_external (
    id int,
    brand_id int,
    name varchar(500),
    position_id int
) as copy from '/tmp/data/products.csv' DELIMITER ',';


create external table if not exists product_categories_external (
    product_id int,
    category_id int,
    position int
) as copy from '/tmp/data/product_categories.csv' DELIMITER ',';


create external table if not exists categories_external (
    id int,
    parent_id int,
    name varchar(500),
    position_id int
) as copy from '/tmp/data/categories.csv' DELIMITER ',';