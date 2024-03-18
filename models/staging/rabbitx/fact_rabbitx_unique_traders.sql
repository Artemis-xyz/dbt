with
    latest_source_jsons as (
        select
            extraction_date,
            source_url,
            source_json,
            rank() over (
                partition by (source_url, date(extraction_date))
                order by extraction_date desc
            ) as rnk
        from {{ source("PROD_LANDING", "raw_rabbitx_unique_traders") }}
    )

select
    source_json::number as unique_traders,
    'rabbitx' as app,
    null as chain,
    'DeFi' as category,
    date(extraction_date) as date
from latest_source_jsons
where rnk = 1
