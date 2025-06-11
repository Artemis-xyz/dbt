{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_avalanche_ccip_reward_daily"
    )
}}

{{ chainlink_ccip_rewards_daily('avalanche')}}