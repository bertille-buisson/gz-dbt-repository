select
    date_trunc(date(date_date), month) as date_year_month,
    sum(operational_margin - ads_cost) as ads_margin,
    round(sum(operational_margin),2) as operational_margin,
    round(sum(revenue),2) as revenue,
    round(sum(quantity),2) as quantity,
    round(sum(margin),2) as margin,
    round(sum(ads_impressions),2) as ads_impressions,
    round(sum(ads_clicks),2) as ads_clicks
from {{ref("finance_campaigns_day")}}
group by date_year_month
