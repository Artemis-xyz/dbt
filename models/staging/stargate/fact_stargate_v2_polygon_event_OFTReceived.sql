{{config(materialized="table", snowflake_warehouse='ANALYTICS_XL')}}

{{stargate_OFTReceived('polygon')}}
