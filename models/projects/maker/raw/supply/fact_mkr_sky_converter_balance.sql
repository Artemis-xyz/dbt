{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
    )
}}
    
    {{forward_filled_balance_for_address('ethereum', '0xA1Ea1bA18E88C381C724a75F23a130420C403f9a')}}