with latest_source_json as (
    select extraction_date, source_url, source_json
    from {{ source("PROD_LANDING", "raw_hyperliquid_fees") }}
    where extraction_date = (select max(extraction_date) from {{ source("PROD_LANDING", "raw_hyperliquid_fees") }})
),

extracted_fees as (
    select
        value:total_fees::number as total_fees,
        value:total_spot_fees::number as total_spot_fees,
        value:time as timestamp
    from latest_source_json, lateral flatten(input => parse_json(source_json))
)
, max_fees as (
    select
        timestamp,
        total_fees / 1e6 as max_trading_fees,
        total_spot_fees / 1e6 as max_spot_fees
    from extracted_fees
)
, fee_data as (
    select
        timestamp,
        max_trading_fees,
        max_spot_fees,
        max_trading_fees - coalesce(lag(max_trading_fees) over (order by timestamp asc), 0) as trading_fees,
        max_spot_fees - coalesce(lag(max_spot_fees) over (order by timestamp asc), 0) as spot_fees,
        (trading_fees - spot_fees) as perp_fees
    from max_fees
)

, reassigned_fees as (
    select
        case
            when date(timestamp) in ('2024-11-17', '2024-11-25') then date '2024-11-29'
            else date(timestamp)
        end as date,
        trading_fees,
        spot_fees,
        perp_fees
    from fee_data
)

, date_spine_fees as (
    select
        ds.date,
        rf.trading_fees,
        rf.spot_fees,
        rf.perp_fees
    from {{ ref('dim_date_spine') }} ds
    left join reassigned_fees rf on ds.date = rf.date
    where ds.date between (select min(date) from reassigned_fees)
                            and to_date(sysdate())
)

, numbered_fees as (
    select
        date,
        trading_fees,
        spot_fees,
        perp_fees,
        sum(case when trading_fees is not null then 1 else 0 end) 
            over (order by date desc rows between unbounded preceding and current row) as group_id
    from date_spine_fees
)

, group_sizes as (
    select
        group_id,
        count(*) as group_size
    from numbered_fees
    group by group_id
)

, group_values as (
    select
        group_id,
        max(trading_fees) as trading_fees,
        max(spot_fees) as spot_fees,
        max(perp_fees) as perp_fees
    from numbered_fees
    group by group_id
)

, distributed_fees as (
    select
        nf.date,
        gv.trading_fees / nullif(gs.group_size, 0) as trading_fees,
        gv.spot_fees / nullif(gs.group_size, 0) as spot_fees,
        gv.perp_fees / nullif(gs.group_size, 0) as perp_fees
    from numbered_fees nf
    left join group_sizes gs on nf.group_id = gs.group_id
    left join group_values gv on nf.group_id = gv.group_id
)

select
    date,
    'hyperliquid' AS app,
    'hyperliquid' AS chain,
    'DeFi' AS category,
    round(sum(trading_fees), 2) as trading_fees,
    round(sum(spot_fees), 2) as spot_fees,
    round(sum(perp_fees), 2) as perp_fees
from distributed_fees
group by 1