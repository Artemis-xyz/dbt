{{ config(materialized="incremental", unique_key=["date"]) }}

select
  date,
  sum(stablecoin_supply) as tvl
from {{ ref("fact_ethereum_stablecoin_balances") }}
where lower(address) = lower('0xc8CF6D7991f15525488b2A83Df53468D682Ba4B0')
  and lower(contract_address) = lower('0xfa2b947eec368f42195f24f36d2af29f7c24cec2')
  {% if is_incremental() %}
    and date >= dateadd(day, -3, current_date())
  {% endif %}
group by date