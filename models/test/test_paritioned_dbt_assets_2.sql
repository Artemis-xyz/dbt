{{ 
    config(
        partition_by='date', 
        materialized='incremental',
        schema="raw",
        alias="paritioned_dbt_assets_2"
    ) 
}}

{% set partition_date = var('partition_date') %}

{% do log("Current partition key: " ~ partition_date, info=true) %}

select *
from ethereum_flipside.core.ez_token_transfers
where block_timestamp::date = '{{ partition_date }}' 
