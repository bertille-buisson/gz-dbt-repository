select
    date_date,
    round(sum(cast(ads_cost as float64)),2) as ads_cost,
    round(sum(impression),2) as ads_impressions,
    round(sum(click),2) as ads_clicks
from {{ ref("int_campaign")}}
group by date_date
order by date_date