{{
    config(
        materialized='table',
        snowflake_warehouse='BALANCER',
        database='BALANCER',
        schema='raw',
        alias='fact_balancer_treasury_by_token'
    )
}}

{{ get_treasury_balance(
        chain='ethereum',
        addresses=[
            '0xce88686553686da562ce7cea497ce749da109f9f',
            '0x10a19e7ee7d7f8a52822f6817de8ea18204f2e4f',
            '0xb618f903ad1d00d6f7b92f5b0954dcdc056fc533',
            '0x0efccbb9e2c09ea29551879bd9da32362b32fc89'
        ],
        earliest_date='2020-06-23'
    )
}}