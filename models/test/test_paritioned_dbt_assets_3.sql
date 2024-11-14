{{ 
    config(
        partition_by='date', 
        materialized='incremental',
    ) 
}}

{% set partition_date = var('partition_date') %}

{% do log("Current partition key: " ~ partition_date, info=true) %}

select *
from ethereum_flipside.core.ez_token_transfers
where block_timestamp::date = '{{ partition_date }}' 
