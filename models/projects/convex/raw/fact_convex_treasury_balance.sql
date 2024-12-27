{{
    config(
        materialized='table',
        snowflake_warehouse='CONVEX',
        database='CONVEX',
        schema='raw',
        alias='fact_convex_treasury_balance'
    )
}}

{{ get_treasury_balance(
        chain='ethereum',
        addresses=[
            '0x1389388d01708118b497f59521f6943Be2541bb7',
            '0xe98984aD858075813AdA4261aF47e68A64E28fCC'
        ],
        earliest_date='2021-05-12'
    )
}}