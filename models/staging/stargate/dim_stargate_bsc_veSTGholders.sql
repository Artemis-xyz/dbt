{{config(materialized="table", snowflake_warehouse='STARGATE_MD')}}
{{
    stargate_veSTGholders(
        'bsc'
        , '0xB0D502E938ed5f4df2E681fE6E419ff29631d62b'
    )
}}
