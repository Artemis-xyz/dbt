{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_optimism_vrf_rewards_daily",
    )
}}
{{ chainlink_vrf_rewards_daily('optimism') }}