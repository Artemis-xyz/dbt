{{
    config(
        materialized="incremental",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_arbitrum_ocr_reward_transmission_logs",
    )
}}

{{ chainlink_logs('arbitrum', ('0xd0d9486a2c673e2a4b57fc82e4c8a556b3e2b82dd5db07e2c04a920ca0f469b6'))}}