{{config(materialized="table", snowflake_warehouse='STARGATE')}}
{{
    stargate_fees(
        'ethereum'
        , '0xbB2Ea70C9E858123480642Cf96acbcCE1372dCe1'
    )
}}