{{
    config(
        materialized="table",
        snowflake_warehouse="AKASH",
        database="akash",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

SELECT
    date
    , 'akash' AS chain
    , active_leases
    , active_providers
    , new_leases
    , compute_fees
    , gas_fees
    , gross_protocol_revenue
    , validator_cash_flow
    , treasury_cash_flow
    , service_cash_flow
    , revenue
    , burns_native
    , mints_native
    , price
    , market_cap
    , fdmc
    , token_turnover_fdv
    , token_volume
FROM
    {{ ref("ez_akash_metrics") }}
