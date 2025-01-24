{{
    config(
        materialized="table",
        unique_key=["unique_id"],
    )
}}
with
    stablecoin_transfers as (
        select
            block_timestamp
            , date
            , block_number
            , epoch
            , tx_hash
            , from_address
            , to_address
            , is_burn
            , is_mint
            , amount
            , inflow
            , transfer_volume
            , contract_address
            , symbol
            , unique_id
        from {{ ref("fact_sui_stablecoin_transfers") }}
        where from_address != to_address 
            and from_address is not null
            and to_address is not null
        {% if is_incremental() %} 
            and block_timestamp >= (
                select dateadd('day', -3, max(block_timestamp))
                from {{ this }}
            )
        {% endif %}
    )
    , stablecoin_transfers_with_prices as (
        select
            t1.block_timestamp,
            t1.block_number,
            t1.tx_hash,
            t1.contract_address as token_address,
            unique_id,
            t1.from_address,
            t1.to_address,
            t1.amount,
            transfer_volume * coalesce(
                p.shifted_token_price_usd, 
                case 
                    when c.coingecko_id = 'euro-coin' then ({{ avg_l7d_coingecko_price('euro-coin') }})
                    when c.coingecko_id = 'celo-euro' then ({{ avg_l7d_coingecko_price('celo-euro') }})
                    when c.coingecko_id = 'celo-real-creal' then ({{ avg_l7d_coingecko_price('celo-real-creal') }})
                    when c.coingecko_id = 'celo-kenyan-shilling' then ({{ avg_l7d_coingecko_price('celo-kenyan-shilling') }})
                    else 1
                end
            ) as amount_usd
        from stablecoin_transfers t1
        join {{ ref("fact_sui_stablecoin_contracts") }} c
            on lower(t1.contract_address) = lower(c.contract_address)
        left join {{ ref("fact_coingecko_token_date_adjusted_gold") }} p
            on lower(c.coingecko_id) = lower(p.coingecko_id)
            and t1.date = p.date
    )
     , cex_contracts as (
        select address, artemis_application_id AS app, artemis_sub_category_id AS sub_category from {{ ref("dim_all_addresses_labeled_gold")}} where chain = '{{ chain }}' and lower(artemis_sub_category_id) in ('cex', 'market maker')
    )
    , cex_filter as (
        select distinct tx_hash 
        from stablecoin_transfers_with_prices
        left join cex_contracts t1 on lower(from_address) = lower(t1.address)
        left join cex_contracts t2 on lower(to_address) = lower(t2.address)
        where t1.app = t2.app
            and lower(t1.sub_category) in ('cex', 'market maker') 
    )
select
    block_timestamp,
    block_number,
    tx_hash,
    token_address,
    from_address,
    to_address,
    amount,
    amount_usd,
    unique_id
from stablecoin_transfers_with_prices
where lower(tx_hash) not in (select lower(tx_hash) from cex_filter)