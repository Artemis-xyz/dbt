with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_kamino_revenue_and_fees") }}
    )
select
    value:day::date as date,
    value:KlendRevenueUSD::number as klend_revenue_usd,
    value:KlendFeesUSD::number as klend_fees_usd
from
    {{ source("PROD_LANDING", "raw_kamino_revenue_and_fees") }},
    lateral flatten(input => parse_json(source_json))
where extraction_date = (select max_date from max_extraction)