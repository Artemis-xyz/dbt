{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="JUPITER",
    )
}}

with hex_cte as(
    SELECT
      date(block_timestamp) as date,
      {{ base58_to_hex("f.value:data") }} as hex_data,
      f.value:data as base58_data,
      tx_id
    FROM
      solana_flipside.core.fact_events,
    LATERAL FLATTEN(input => get_path(inner_instruction, 'instructions')) AS f
    where program_id = 'DCA265Vj8a9CEuX1eb1LWRnDT7uK6q1xMipnNyatn23M'
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run 
        and block_timestamp::date >= (select DATEADD('day', -3, max(date)) from {{ this }})
    {% else %}
        and block_timestamp::date > '2023-06-12'
    {% endif %}
    and f.value:data is not null
    and succeeded = 1
),
processed_ic as( -- processed hex data
    SELECT
        date,
        {{ big_endian_hex_to_decimal("SUBSTR(hex_data,224+1,16)") }} as amount, -- fee amount in token units
        {{ hex_to_base58("SUBSTR(hex_data, 160+1, 64)") }} as mint,
        hex_data,
        tx_id
    FROM hex_cte
    WHERE SUBSTRING(hex_data,16+1,16) = '2a88d874b5d16db5' -- CollectedFee
)
SELECT
  date,
  SUM(p.price*ic.amount/pow(10, p.decimals)) as fees,
  -- we can back into volume since Jupiter DCA do charge a platform fees of 0.1% on order completion
  -- https://station.jup.ag/guides/dca/how-to-dca
  SUM(p.price*ic.amount/pow(10, p.decimals)) / 0.001 as volume, 
  COUNT(distinct ic.tx_id) as txns
FROM
  processed_ic ic
left join solana_flipside.price.ez_prices_hourly p -- missing PUPS data
      on p.token_address = ic.mint
      and p.hour = date_trunc('hour',ic.date)
where p.decimals is not null
and p.token_address in ( SELECT mint from pc_dbt_db.prod.dim_solana_top_500_tokens_by_transfer_count limit 300)
GROUP BY 1
order by 1 desc