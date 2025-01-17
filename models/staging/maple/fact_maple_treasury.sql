{{
    config(
        materialized='table',
        snowflake_warehouse='MAPLE'
    )
}}

{{ get_treasury_balance(
        chain='ethereum',
        addresses=[
            '0xa9466eabd096449d650d5aeb0dd3da6f52fd0b19',
            '0xd6d4Bcde6c816F17889f1Dd3000aF0261B03a196'
        ],
        earliest_date='2020-09-12'
    )
}}