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
        where chain = 'ethereum'
    )

    {{
        fact_protocol_daa_txns_gas_gas_usd(
            "ethereum", "maverick_protocol", "Maverick Protocol", "DeFi", "dim_protocol_addresses"
        )
    }}
