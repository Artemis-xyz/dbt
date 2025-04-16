{{ config(materialized="table", snowflake_warehouse="SYNTHETIX") }}


with ethereum_token_holders as (
    {{ token_holders('ethereum', '0xC011a73ee8576Fb46F5E1c5751cA3B9Fe0af2a6F', '2024-02-23') }}
), 

optimism_token_holders as (
    {{ token_holders('optimism', '0x8700dAec35aF8Ff88c16BdF0418774CB3D7599B4', '2024-07-21') }}
)

select * from ethereum_token_holders
union all
select * from optimism_token_holders

