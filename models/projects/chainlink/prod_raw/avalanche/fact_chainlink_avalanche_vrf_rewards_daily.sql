{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_avalanche_vrf_rewards_daily",
    )
}}
{{ chainlink_vrf_rewards_daily('avalanche') }}