/*
📊 Advanced Sales & Customer Analytics (Gold Layer Analysis)

This section contains advanced analytical queries built on top of the Gold star schema to derive business KPIs and performance insights. The analysis covers financial metrics, growth trends, customer behavior, product performance, and profitability using fact–dimension joins.

Key insights generated include:

> Average Order Value (AOV)

> Year-over-Year (YoY) and Month-over-Month (MoM) growth

> Top-performing products by revenue and quantity

> Category and subcategory sales distribution

> Profit, profit margin, and country-wise performance

> Customer retention rate

> Customer Lifetime Value (CLV)

> Repeat purchase rate

> Revenue contribution of top products

> Age-group-based sales segmentation
*/

------------------------------------------
--Average Order Value (AOV)
------------------------------------------
select
sum(sales_amount) / count(distinct order_number) as average_order_value
from gold.fact_sales

------------------------------------------
--Number of orders per year
------------------------------------------
select
extract(year from order_date) as years,
count(distinct order_number) as orders
from gold.fact_sales
group by extract(year from order_date)
order by years

------------------------------------------
--Orders growth rate per year
------------------------------------------
with cte as(
select
extract(year from order_date) as years,
count(distinct order_number) as orders
from gold.fact_sales
group by extract(year from order_date)
order by years
)
,cte2 as(
select
*,
lag(orders) over(order by years) as previous_year_orders
from cte
)
select 
*,
case when previous_year_orders is null or previous_year_orders = 0
     then null
	 else concat(round((orders - previous_year_orders)*100 / previous_year_orders,2), '%')
	 end as order_growth_rate
from cte2

------------------------------------------
--YoY growth
------------------------------------------
with cte as(
select
extract(year from order_date) as years,
sum(sales_amount) as total_sales
from gold.fact_sales
group by years
)
,cte2 as(
select
*,
lag(total_sales) over(order by years) as previous_year_sales
from cte
)
select
*,
case when previous_year_sales is null or previous_year_sales = 0
     then null
	 else concat((total_sales - previous_year_sales)*100 / previous_year_sales,'%') 
	 end as YoY_growth 
from cte2

------------------------------------------
--MoM growth per year
------------------------------------------
with cte as(
select
extract(year from order_date) as years,
extract(month from order_date) as months,
sum(sales_amount) as total_sales
from gold.fact_sales
group by years, months
)
,cte2 as(
select
*,
lag(total_sales) over(partition by years order by months) as previous_month_sales
from cte
)
select
*,
case when previous_month_sales is null or previous_month_sales = 0
     then null
	 else concat((total_sales - previous_month_sales)*100 / previous_month_sales,'%') 
	 end as MoM_growth
from cte2

------------------------------------------
--Top 10 performing produts 
------------------------------------------
with cte as (
select
product_key,
sum(sales_amount) as total_sales
from gold.fact_sales
group by product_key
)
,cte2 as(
select
*,
row_number() over(order by total_sales desc) as ranking
from cte
)
select 
*
from cte2
where ranking <= 10

------------------------------------------
--Top 10 most sold products
------------------------------------------
with cte as(
select
s.product_key,
p.product_name,
sum(s.quantity) as total_quantity_sold
from gold.fact_sales s
left join gold.dim_products p
on p.product_key = s.product_key
group by s.product_key, p.product_name
)
,cte2 as(
select
*,
dense_rank() over(order by total_quantity_sold desc) as ranking
from cte
)
select 
*
from cte2
where ranking <= 10

------------------------------------------
--Category wise sales and distribution
------------------------------------------
with cte as(
select
s.product_key,
p.product_name,
p.category,
sum(s.sales_amount) as total_sales
from gold.fact_sales s
join gold.dim_products p
on p.product_key = s.product_key
group by s.product_key,
p.product_name,
p.category
)
,cte2 as(
select
category,
sum(total_sales) as category_sales
from cte
group by category
)
select
*,
(category_sales*100 / sum(category_sales) over()) as sales_distribution
from cte2 


------------------------------------------
--Sub-Category wise sales 
------------------------------------------
with cte as(
select
s.product_key,
p.product_name,
p.category,
p.subcategory,
sum(s.sales_amount) as total_sales
from gold.fact_sales s
join gold.dim_products p
on p.product_key = s.product_key
group by s.product_key,
p.product_name,
p.category,
p.subcategory
)
,cte2 as (
select
category,
subcategory,
sum(total_sales) as subcategory_sales
from cte
group by category, subcategory
order by category
)
select
*,
concat(round((subcategory_sales*100 / sum(subcategory_sales) over(partition by category)),2),'%') as sales_distribution
from cte2


------------------------------------------
--Profit of each order
------------------------------------------
select
s.order_number,
s.order_date,
s.sales_amount,
s.quantity,
s.price,
p.cost,
(sales_amount - (s.quantity * p.cost)) as profit
from gold.fact_sales s
left join gold.dim_products p
on s.product_key = p.product_key

------------------------------------------
--Total sales by age group
------------------------------------------
with cte as(
select
c.birthdate,
date_part('year', age(current_date, c.birthdate)) as age,
sum(s.sales_amount) as total_sales
from gold.fact_sales s
left join gold.dim_customers c
on s.customer_key = c.customer_key
group by c.birthdate
)
select
case when age between 0 and 1 then 'Infant'
     when age between 2 and 12 then 'Child'
     when age between 13 and 19 then 'Teenager'
	 when age between 20 and 24 then 'Young Adult'
	 else 'Adult'
     end as age_group,
sum(total_sales) as total_sales
from cte
group by age_group


--------------------------------------------------
--Country wise sales and profit and profit margin
--------------------------------------------------
with cte as(
select 
c.country,
s.sales_amount,
s.quantity,
s.price,
p.cost,
(sales_amount - (s.quantity * p.cost)) as profit
from gold.fact_sales s
join gold.dim_customers c
on c.customer_key = s.customer_key
join gold.dim_products p
on s.product_key = p.product_key
)
,cte2 as(
select 
country,
sum(sales_amount) as total_sales,
sum(profit) as total_profit
from cte
group by country
)
select
*,
concat((total_profit * 100/ total_sales),'%') as profit_margin
from cte2

--Second method:-
select
c.country,
sum(sales_amount) as total_sales,
sum((sales_amount - (s.quantity * p.cost))) as profit,
concat(sum((sales_amount - (s.quantity * p.cost))* 100) / nullif(sum(sales_amount),0),'%') as profit_margin
from gold.fact_sales s
join gold.dim_customers c
on c.customer_key = s.customer_key
join gold.dim_products p
on s.product_key = p.product_key
group by c.country

--------------------------------------------------
--Customer Retaintion 
--------------------------------------------------

with customers_in_2011 as(
select
distinct
customer_key
from gold.fact_sales
where extract(year from order_date) = '2011' 
)
,customers_in_2012 as(
select
distinct
customer_key
from gold.fact_sales
where extract(year from order_date) = '2012' 
)
select
count(distinct c11.customer_key) as total_customer_in_2011,
count(distinct c12.customer_key) as retained_customers_in_2012,
concat((count(distinct c12.customer_key) * 100 / nullif(count(distinct c11.customer_key),0)),'%') as retaintion_rate
from customers_in_2011 c11
left join customers_in_2012 c12
on c11.customer_key = c12.customer_key

--------------------------------------------------
--Customer Lifetime Value (CLV)
--------------------------------------------------
select
c.customer_id,
c.first_name,
c.last_name,
sum(s.sales_amount) as customer_lifetime_value
from gold.fact_sales s
left join gold.dim_customers c
on s.customer_key = c.customer_key
group by c.customer_id, c.first_name, c.last_name
order by customer_id

--------------------------------------------------
--Repeat Purchase Rate
--------------------------------------------------
with cte as(
select 
customer_key,
count(distinct order_number) as order_count
from gold.fact_sales
group by customer_key
)
select
count(case when order_count > 1 then 1 end) * 100 / count(*) as repeat_purchse_rate
from cte

--------------------------------------------------
--Contribution of Top 10 Products to Total Revenue
--------------------------------------------------
with cte as(
select
product_key,
sum(sales_amount) as product_total_sales
from gold.fact_sales
group by product_key
order by sum(sales_amount) desc limit 10
)
select
*,
concat((product_total_sales * 100 / (select sum(sales_amount) from gold.fact_sales)),'%') as contribution
from cte






