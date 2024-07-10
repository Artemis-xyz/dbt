{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_bsc_automation_reward_daily",
    )
}}


{{chainlink_automation_rewards_daily ('bsc')}} 