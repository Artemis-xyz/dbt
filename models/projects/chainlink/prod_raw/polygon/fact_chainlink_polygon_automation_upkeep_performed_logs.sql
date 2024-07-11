{{
    config(
        materialized="incremental",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_polygon_automation_upkeep_performed_logs"
    )
}}

{{ chainlink_logs('polygon', ('0xcaacad83e47cc45c280d487ec84184eee2fa3b54ebaa393bda7549f13da228f6', '0xad8cc9579b21dfe2c2f6ea35ba15b656e46b4f5b0cb424f52739b8ce5cac9c5b'))}} 

