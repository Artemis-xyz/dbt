{{
    config(
        materialized="view",
        unique_key=["tx_hash", "event_index"],
        warehouse="GMX",
        database="gmx",
        schema="raw",
        alias="fact_perps_token_prices"
    )
}}

SELECT * FROM {{ ref('fact_gmx_all_versions_trades') }}