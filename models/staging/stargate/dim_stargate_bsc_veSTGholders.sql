{{config(materialized="table", snowflake_warehouse='STARGATE_MD')}}
{{
    stargate_veSTGholders(
        'bsc'
    )
}}
