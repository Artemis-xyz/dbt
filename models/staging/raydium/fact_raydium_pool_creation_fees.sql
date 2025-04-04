{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="RAYDIUM",
    )
}}

select date_trunc('day', block_timestamp) as date
    , 'So11111111111111111111111111111111111111112' as token_mint_address -- replace native SOL for WSOL mint address to match price later
    , sum(amount) as amount_raw -- SOL amount 
from SOLANA_FLIPSIDE.CORE.FACT_TRANSFERS
where 1=1
    and mint = 'So11111111111111111111111111111111111111111' -- SOL
    and tx_to in (
        'DNXgeM9EiiaAbaWvwjHj9fQQLAX5ZsfHyvmYUNRAdNC8' -- CPMM (0.15SOL)
        , '7YttLkHDoNj9wyDur5pM1ejNaAvT9X4eqaYcHQqtj2G5' -- AMMv4  (0.4SOL)
        -- CLMM has no pool creation fee to Raydium
    )
{% if is_incremental() %}
    AND block_timestamp::date >= (select dateadd('day', -3, max(date)) from {{ this }})
{% else %}
    AND block_timestamp::date >= date('2022-04-22') 
{% endif %}

group by 1,2