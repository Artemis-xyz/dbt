{{ 
    config(
        partition_by='date', 
        materialized='incremental',
        schema="raw",
        alias="paritioned_dbt_assets_2"
    ) 
}}

select *
from ethereum_flipside.core.ez_token_transfers
