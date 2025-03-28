{{
    config(
        materialized="table",
        snowflake_warehouse="PUMPFUN",
        database="PUMPFUN",
        schema="raw",
        alias="fact_pumpswap_metrics",
    )
}}

with
    max_extraction as (
        select max(extraction_date) as max_date
        from {{ source("PROD_LANDING", "raw_pumpswap_metrics" ) }}
        
    ),
   latest_data as (
        select parse_json(source_json) as data
        from {{ source("PROD_LANDING", "raw_pumpswap_metrics") }}
        where extraction_date = (select max_date from max_extraction)
    ),
    flattened_data as (
        select
            f.value:"date"::date as date,
            f.value:"daily_lp_fees_sol"::number as daily_lp_fees_sol,
            f.value:"daily_protocol_fees_sol"::number as daily_protocol_fees_sol,
            f.value:"daily_volume_sol"::number as trading_volume_sol,
            f.value:"spot_dau"::number as spot_dau,
            f.value:"transactions"::number as spot_txns,
        from latest_data, lateral flatten(input => data) as f
    ),
    flattened_data_usd as (
        select
            date,
            daily_lp_fees_sol,
            daily_lp_fees_sol * p.price as daily_lp_fees_usd,
            daily_protocol_fees_sol,
            daily_protocol_fees_sol * p.price as daily_protocol_fees_usd,
            trading_volume_sol,
            trading_volume_sol * p.price as trading_volume_usd,
            spot_dau,
            spot_txns
        from flattened_data
        left join {{ source('SOLANA_FLIPSIDE_PRICE', 'ez_prices_hourly') }} p
            ON p.hour = date_trunc(hour, flattened_data.date)
            AND p.is_native
    )
select 
    date,
    daily_lp_fees_sol,
    daily_lp_fees_usd,
    daily_protocol_fees_sol,
    daily_protocol_fees_usd,
    trading_volume_sol,
    trading_volume_usd,
    spot_dau,
    spot_txns
from flattened_data_usd