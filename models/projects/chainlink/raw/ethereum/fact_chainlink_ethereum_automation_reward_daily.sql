{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_ethereum_automation_reward_daily",
    )
}}


{{chainlink_automation_rewards_daily ('ethereum')}} 