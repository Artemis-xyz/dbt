{{
    config(
        materialized="table",
        snowflake_warehouse="STRIDE",
        database="stride",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    -- Alternative fundamental source from BigQuery, preferred when possible over Snowflake data
    fundamental_data as (select * EXCLUDE date, TO_TIMESTAMP_NTZ(date) AS date from {{ source('PROD_LANDING', 'ez_stride_metrics') }})

select
    fundamental_data.date,
    fundamental_data.chain,
    fundamental_data.txns,
    fundamental_data.dau,
    fundamental_data.wau,
    fundamental_data.mau,
    fundamental_data.fees_native,
    fundamental_data.fees_usd AS fees,
    fundamental_data.avg_txn_fee,
    fundamental_data.returning_users,
    fundamental_data.new_users,
    fundamental_data.low_sleep_users,
    fundamental_data.high_sleep_users,
    fundamental_data.sybil_users,
    fundamental_data.non_sybil_users,
    fundamental_data.total_staking_yield_usd,
    fundamental_data.total_supply_side_revenue_usd,
    fundamental_data.protocol_revenue_usd,
    fundamental_data.operating_expenses_usd,
    fundamental_data.protocol_earnings_usd
from fundamental_data