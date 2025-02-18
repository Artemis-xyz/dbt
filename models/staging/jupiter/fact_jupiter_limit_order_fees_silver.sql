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
      p.price,
    FROM
      solana_flipside.core.fact_transfers tr
    left join solana_flipside.price.ez_prices_hourly p -- missing PUPS data
      on p.token_address = tr.mint
      and p.hour = date_trunc('hour',tr.block_timestamp)
    where tx_to = 'H3vkQqNVWySTD4c1Y91wtoT5iwxKSVtVLfC2rD8SgwTN'
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run 
        and block_timestamp::date >= (select dateadd('day', -7, max(date)) from {{ this }})
    {% else %}
        and block_timestamp::date > '2022-10-14'
    {% endif %}
    and p.price is not null
)
select
  date(a.block_timestamp) as date,
  sum(amount*price) as fees,
  sum(amount*price) / 0.02 as volume, -- we can back into volume since Jupiter Limit Order do charge a platform fees of 0.2% on taker.
  count(distinct tx_id) as txns
from all_transfers a
WHERE
  ((amount*price) > 100 and a.mint in (SELECT mint FROM pc_dbt_db.prod.dim_solana_top_500_tokens_by_transfer_count limit 100))
  or (amount*price) < 100
group by 1
order by 1 desc