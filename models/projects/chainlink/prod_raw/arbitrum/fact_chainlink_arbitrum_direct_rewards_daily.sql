{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_arbitrum_direct_rewards_daily"
    )
}}

{{ chainlink_direct_operator_rewards_daily('arbitrum')}}