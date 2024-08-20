{{
    config(
        materialized="table",
        snowflake_warehouse="LIDO",
        database="lido",
        schema="raw",
        alias="fact_ldo_tokenholder_count",
    )
}}

{{ token_holders('ethereum', '0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32', '2020-12-17')}}
