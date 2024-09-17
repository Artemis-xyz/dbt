{{
    config(
        materialized="table",
        snowflake_warehouse="PENDLE",
        database="pendle",
        schema="raw",
        alias="fact_token_holders",
    )
}}

{{token_holders('ethereum', '0x808507121B80c02388fAd14726482e061B8da827', '2021-04-27')}}