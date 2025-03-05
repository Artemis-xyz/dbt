{{ config(
    materialized="table",
    snowflake_warehouse="MARINADE"
) }}

select 
    date(block_timestamp) as date, 
    count(distinct signers[0]) as dau,
    count(*) as txns
from solana_flipside.core.fact_decoded_instructions
where program_id = 'MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD'
group by 1
order by 1 desc