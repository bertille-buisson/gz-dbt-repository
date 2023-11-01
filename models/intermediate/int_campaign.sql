SELECT * FROM {{ source("raw", "adwords")}}
UNION ALL 
SELECT *
FROM    {{ source("raw", "bing")}}
UNION ALL 
SELECT *
FROM   {{ source("raw", "criteo")}}
UNION ALL 
SELECT *
FROM   {{ source("raw", "facebook")}}
