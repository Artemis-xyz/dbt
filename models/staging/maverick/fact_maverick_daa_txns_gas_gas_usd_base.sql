{{
    config(
        materialized="table",
        snowflake_warehouse="FUNDAMENTAL_METRICS_WAREHOUSE_SM",
    )
}}

with
    dim_protocol_addresses as (
        select address
        from {{ ref("dim_maverick_contracts_gold") }}
        where chain = 'base'
    )

    {{
        fact_protocol_daa_txns_gas_gas_usd(
            "base", "maverick_protocol", "DeFi", "dim_protocol_addresses"
        )
    }}
