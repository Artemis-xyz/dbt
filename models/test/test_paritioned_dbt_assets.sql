{{ 
    config(
        partition_by='date', 
        materialized='incremental',
) }}

select *
from base_flipside.core.ez_token_transfers