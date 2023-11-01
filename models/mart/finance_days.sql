select 
    date_date,
    count(orders_id) as nb_transactions,
    revenue,
    round(avg(revenue*quantity),2) as avg_basket,
    margin,
    round(sum(operational_margin),2) as operational_margin
from {{ref("int_orders_operational")}}
group by date_date, revenue, margin
order by date_date