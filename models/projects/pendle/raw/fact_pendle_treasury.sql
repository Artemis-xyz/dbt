{{
    config(
        materialized="table",
        snowflake_warehouse="PENDLE",
        database="pendle",
        schema="raw",
        alias="fact_pendle_treasury",
    )
}}

{{ get_treasury_balance('ethereum', '0x8270400d528c34e1596EF367eeDEc99080A1b592') }}