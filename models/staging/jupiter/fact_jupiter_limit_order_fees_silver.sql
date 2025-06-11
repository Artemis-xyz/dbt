{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="JUPITER",
    )
}}

with all_transfers as(
    SELECT
      tr.block_timestamp,
      tr.tx_id,
      tr.amount,
      tr.mint,
      coalesce(p.price, sol.price) as price,
    FROM
      solana_flipside.core.fact_transfers tr
    left join solana_flipside.price.ez_prices_hourly p on (
        p.token_address = tr.mint 
        and p.hour = date_trunc('hour',tr.block_timestamp)
    )
    left join solana_flipside.price.ez_prices_hourly sol on (
        sol.is_native 
        and tr.mint = 'So11111111111111111111111111111111111111111' 
        and sol.hour = date_trunc('hour',tr.block_timestamp)
    )
    where tx_to in ('H3vkQqNVWySTD4c1Y91wtoT5iwxKSVtVLfC2rD8SgwTN', 'GNSHYrJmjwYXnWLy3esF5VjWa1AKMhzAru1pTeQDY8w3')
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run 
        and block_timestamp::date >= (select DATEADD('day', -3, max(date)) from {{ this }})
    {% else %}
        and block_timestamp::date > '2022-10-14'
    {% endif %}
)
select
  date(a.block_timestamp) as date,
  sum(amount*price) as fees,
  -- we can back into volume since Jupiter Limit Order do charge a platform fees of 0.1% on taker. 
  -- https://station.jup.ag/guides/limit-order/limit-order
  sum(amount*price) / 0.001 as volume,
  count(distinct tx_id) as txns
from all_transfers a
WHERE
  ((amount*price) > 100 and a.mint in (SELECT mint FROM pc_dbt_db.prod.dim_solana_top_500_tokens_by_transfer_count limit 100))
  or (amount*price) < 100
group by 1
order by 1 desc