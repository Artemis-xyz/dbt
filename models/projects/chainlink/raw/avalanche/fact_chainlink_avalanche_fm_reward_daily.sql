{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_avalanche_fm_reward_daily"
    )
}}

{{ chainlink_fm_rewards_daily('avalanche')}}