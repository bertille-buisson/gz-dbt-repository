with
    subq as (
        select
            ship.orders_id,
            date_date,
            round(sum(cast(ship_cost as float64)), 2) as ship_cost,
            round(sum(shipping_fee), 2) as shipping_fee,
            round(sum(margin), 2) as margin,
            round(sum(logcost), 2) as logcost,
            round(sum(revenue),2) as revenue,
            round(sum(quantity),2) as quantity
        from {{ source("raw", "ship") }} as ship
        full join
            {{ ref("int_orders_margin") }} as ord on ship.orders_id = ord.orders_id
        group by ship.orders_id, date_date
    )
select
    orders_id,
    date_date,
    round(((margin + shipping_fee) - logcost) - ship_cost, 2) as operational_margin,
    revenue,
    quantity,
    margin
from subq
group by orders_id, date_date, margin, shipping_fee, logcost, ship_cost, revenue, quantity
order by date_date
