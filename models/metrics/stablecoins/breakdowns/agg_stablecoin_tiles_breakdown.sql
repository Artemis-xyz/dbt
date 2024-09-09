{{ config(materialized="table", snowflake_warehouse="STABLECOIN_V2_LG_2") }}


{% set chains = dbt_utils.get_column_values(table=ref('agg_daily_stablecoin_breakdown'), column='chain') %}

{% set symbols = dbt_utils.get_column_values(table=ref('agg_daily_stablecoin_breakdown'), column='symbol') %}



{% set all_chain_combinations = generate_combinations(chains) %}

{% set all_symbol_combinations = generate_combinations(symbols) %}

{% set all_combinations = all_chain_combinations + all_symbol_combinations %}

with
stablecoin_data as (
    select *
    from {{ ref("agg_daily_stablecoin_breakdown") }}
    where date > dateadd(day, -31, to_date(sysdate()))
)

{% for combination in all_combinations %}

select
    '({{combination | map("string") | sort | join("-")}})' as breakdown 
    , sum(stablecoin_transfer_volume) as stablecoin_transfer_volume
    , sum(stablecoin_daily_txns) as stablecoin_daily_txns
    , count(distinct case when stablecoin_transfer_volume > 0 then from_address end) as stablecoin_dau
    , sum(case when date = dateadd(day, -1, to_date(sysdate())) then stablecoin_supply end) as stablecoin_supply
from stablecoin_data
where chain in ({{ "'" + combination | map("string") | join("', '") + "'" }})

{% if not loop.last %}
union all
{% endif %}

{% endfor %}