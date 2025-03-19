{{
    config(
        materialized='table',
        snowflake_warehouse='SYNTHETIX',
        database='synthetix',
        schema='raw',
        alias='fact_synthetix_token_holders'
    )
}}

with ethereum_token_holders as (
    {{ token_holders('ethereum', '0xC011a73ee8576Fb46F5E1c5751cA3B9Fe0af2a6F', '2024-02-23') }}
)
, optimism_token_holders as (
    {{ token_holders('optimism', '0x8700dAec35aF8Ff88c16BdF0418774CB3D7599B4', '2024-07-21') }}
)
, base_token_holders as (
    {{ token_holders('base', '0x22e6966B799c4D5B13BE962E1D117b56327FDa66', '2023-12-19') }}
)
, union_all_token_holders as (
    select * from ethereum_token_holders
    union all
    select * from optimism_token_holders
    union all
    select * from base_token_holders
)
select
    date,
    sum(token_holder_count) as token_holder_count
from union_all_token_holders
group by date
order by date desc


