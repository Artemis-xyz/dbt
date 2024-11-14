{{ 
    config(
        partition_by='date', 
        materialized='incremental',
    ) 
}}

select *
from ethereum_flipside.core.ez_token_transfers
