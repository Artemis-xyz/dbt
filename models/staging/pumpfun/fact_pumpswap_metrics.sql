{{
    config(
        materialized="table",
        snowflake_warehouse="PUMPFUN",
        database="PUMPFUN",
        schema="raw",
        alias="fact_pumpswap_metrics",
    )
}}

with fees as (
    select
        date,
        daily_lp_fees_usd,
        daily_protocol_fees_usd,
        daily_lp_fees_usd + daily_protocol_fees_usd as spot_fees
    from {{ ref('fact_pumpswap_fees') }}
)
, volume as (
    select
        date,
        daily_volume_usd as spot_volume
    from {{ ref('fact_pumpswap_volume') }}
)
, dau as (
    select
        date,
        spot_dau
    from {{ ref('fact_pumpswap_dau') }}
)
, txns as (
    select
        date,
        daily_txns as spot_txns
    from {{ ref('fact_pumpswap_txns') }}
)
    select
        date,
        spot_dau,
        spot_txns,
        spot_volume,
        spot_fees
    from dau
    left join txns using(date)
    left join volume using(date)
    left join fees using(date)