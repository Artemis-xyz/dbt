{{
    config(
        materialized='incremental',
        unique_key='date',
        snowflake_warehouse='PYTH',
    )
}}

--  This data model is only for the v2 of the pyth protocol (on multiple chains)

select 
    block_timestamp::date as date, 
    count(distinct signers) as dau, 
    count(*) as txns
from solana_flipside.core.fact_events_inner
where program_id = 'rec5EKMGg6MxZYaMdyBfgwp4d5rB9T1VQH5pJv5LtFJ'
{% if is_incremental() %}
    and block_timestamp > (select max(date) from {{ this }})
{% else %}
    and block_timestamp > '2024-03-18'
{% endif %}
group by 1