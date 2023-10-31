with sub as (
  select 
  date_date,
  cast(purchSE_PRICE as float64) as pr_price,
  orders_id,
  quantity,
  revenue,
  pdt_id
  from {{ source("raw", "product") }} as prod
  full join {{ source("raw","sales") }} as sal 
  on prod.products_id=sal.pdt_id
)
select
round(sum(quantity) * sum(pr_price),2) as purchase_cost,
round(sum(revenue) - sum(pr_price),2) as margin
from sub
group by quantity,pr_price,revenue,date_date
order by date_date
