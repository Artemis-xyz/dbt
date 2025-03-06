{{config(materialized="table", snowflake_warehouse='STARGATE')}}
{{
    stargate_veSTGholders(
        'bsc'
        , '0xB0D502E938ed5f4df2E681fE6E419ff29631d62b'
        , '0xD4888870C8686c748232719051b677791dBDa26D'
    )
}}
