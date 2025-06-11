{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="RAYDIUM",
    )
}}

select date_trunc('day', block_timestamp) as date
    , mint as token_mint_address
    , sum(amount) as amount_raw -- RAY amount 
from SOLANA_FLIPSIDE.CORE.FACT_TRANSFERS
where 1=1
    and mint = '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R' -- RAY
    and tx_to = 'DdHDoz94o2WJmD9myRobHCwtx1bESpHTd4SSPe6VEZaz'

{% if is_incremental() %}
    AND block_timestamp::date >= (select dateadd('day', -3, max(date)) from {{ this }})
{% else %}
    AND block_timestamp::date >= date('2022-04-22') 
{% endif %}

group by 1,2
