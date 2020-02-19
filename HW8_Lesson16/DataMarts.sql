

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


