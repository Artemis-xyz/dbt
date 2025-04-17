with
    extracted_hypeStaked as (
        select
            value:snapshot_timestamp::int as snapshot_timestamp,
            value:num_holders::int as num_holders,
            value:total_staked::float as total_staked,
            'hyperliquid' as app,
            'hyperliquid' as chain,
            'DeFi' as category
        from {{ source("PROD_LANDING", "raw_hyperliquid_hype_staked") }},
            lateral flatten(input => parse_json(source_json))
)
select
    date(snapshot_timestamp) as date,
    num_holders as num_stakers,
    total_staked as hype_staked,
    app,
    chain,
    category
from extracted_hypeStaked