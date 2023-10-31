select
    orders_id,
    date_date,
    revenue,
    quantity,
    round(sum(quantity) * sum(pr_price), 2) as purchase_cost,
    round(sum(revenue) - sum(pr_price), 2) as margin
from {{ ref("int_orders_margin") }}
order by orders_id, date_date
