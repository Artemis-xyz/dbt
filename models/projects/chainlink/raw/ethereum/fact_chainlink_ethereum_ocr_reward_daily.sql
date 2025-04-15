{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_ethereum_ocr_reward_daily",
    )
}}
    
 {{ chainlink_ocr_rewards_daily('ethereum') }}