with sub as (
    select 
        date_date,
        round(sum(operational_margin),2) as operational_margin,
        round(sum(revenue),2) as revenue,
        round(sum(quantity),2) as quantity,
        round(sum(margin),2) as margin
    from {{ref("int_orders_operational")}} as ordop
    group by date_date
)
select
    sub.date_date,
    sum(operational_margin - ads_cost) as ads_margin,
    round(sum(operational_margin),2) as operational_margin,
    round(sum(revenue),2) as revenue,
    round(sum(quantity),2) as quantity,
    round(sum(margin),2) as margin,
    round(sum(ads_impressions),2) as ads_impressions,
    round(sum(ads_clicks),2) as ads_clicks,
    round(sum(ads_cost),2) as ads_cost
from sub 
left join {{ref("int_campaigns_day")}} as icd 
on sub.date_date=icd.date_date
group by date_date
