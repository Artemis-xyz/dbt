{{
    config(
        materialized="table",
        snowflake_warehouse="IMMUTABLE_X",
        database="immutable_x",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    nft_metrics as ({{ get_nft_metrics("immutable_x") }})

select
    date,
    'immutable_x' as chain,
    nft_trading_volume
from nft_metrics
