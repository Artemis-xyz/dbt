with
    latest_source_json as (
        select extraction_date, source_url, source_json
        from {{ source("PROD_LANDING", "raw_apex_trading_volume") }}
        order by extraction_date desc
        limit 1
    ),

    volume_by_date as (
        select
            to_date(value:"time"::string) as date,
            sum(
                ((value:"open"::float + value:"close"::float) / 2)
                * value:"volume"::float
            ) as trading_volume
        from latest_source_json, lateral flatten(input => parse_json(source_json))
        group by 1
        order by 1 asc
    )

select *, 'apex' as app, 'apex' as chain, 'DeFi' as category
from volume_by_date
