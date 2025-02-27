{{ config(materialized="table", snowflake_warehouse="BRIDGE_MD") }}

with
    prices as (
        select hour, token_address, symbol, avg(price) as price
        from
            (
                select *
                from ethereum_flipside.price.ez_prices_hourly
                union
                select *
                from arbitrum_flipside.price.ez_prices_hourly
                union
                select *
                from optimism_flipside.price.ez_prices_hourly
                union
                select *
                from avalanche_flipside.price.ez_prices_hourly
                union
                select *
                from base_flipside.price.ez_prices_hourly
                union
                select *
                from polygon_flipside.price.ez_prices_hourly
                union
                select *
                from bsc_flipside.price.ez_prices_hourly
            )
        group by 1, 2, 3
    ),

    synapse_transfers as (
        select
            origin_block_timestamp,
            case
                when origin_token_symbol = 'LINK'
                then 'chainlink'
                when origin_token_symbol = 'AVAX'
                then 'avalanche-2'
                when origin_token_symbol = 'BTCB'
                then 'bitcoin-bep2'
                when origin_token_symbol = 'DAI'
                then 'dai'
                when origin_token_symbol = 'BUSD'
                then 'binance-usd'
                when origin_token_symbol = 'WBTC'
                then 'wrapped-bitcoin'
                when origin_token_symbol = 'crvUSD'
                then 'crvusd'
                when origin_token_symbol = 'FTM'
                then 'fantom'
                when origin_token_symbol = 'PLS'
                then 'plutusdao'
                when origin_token_symbol = 'KLAY'
                then 'klay-token'
                when origin_token_symbol = 'xJEWEL'
                then 'xjewel'
                when origin_token_symbol = 'SYN'
                then 'synapse-2'
                when origin_token_symbol = 'VSTA'
                then 'vesta-finance'
                when origin_token_symbol = 'UST'
                then 'terrausd'
                when origin_token_symbol = 'PEPE'
                then 'pepe'
                when origin_token_symbol = 'GMX'
                then 'gmx'
                when origin_token_symbol = 'USDT'
                then 'tether'
                when origin_token_symbol = 'DOG'
                then 'the-doge-nft'
                when origin_token_symbol = 'SDT'
                then 'stake-dao'
                when origin_token_symbol = 'SFI'
                then 'saffron-finance'
                when origin_token_symbol = 'MOVR'
                then 'moonriver'
                when origin_token_symbol = 'synFRAX'
                then 'frax'
                when origin_token_symbol = 'NFD'
                then 'feisty-doge-nft'
                when origin_token_symbol = 'H2O'
                then 'h2o-dao'
                when origin_token_symbol = 'UNIDX'
                then 'unidex'
                when origin_token_symbol = 'NEWO'
                then 'new-order'
                when origin_token_symbol = 'HIGH'
                then 'highstreet'
                when origin_token_symbol = 'gOHM'
                then 'governance-ohm'
                when origin_token_symbol = 'agEUR'
                then 'ageur'
                when origin_token_symbol = 'JEWEL'
                then 'defi-kingdoms'
                when origin_token_symbol = 'MATIC'
                then 'matic-network'
                when origin_token_symbol = 'USDC'
                then 'usd-coin'
                else ''
            end as origin_gecko_id,
            case
                when destination_token_symbol = 'LINK'
                then 'chainlink'
                when destination_token_symbol = 'AVAX'
                then 'avalanche-2'
                when destination_token_symbol = 'BTCB'
                then 'bitcoin-bep2'
                when destination_token_symbol = 'DAI'
                then 'dai'
                when destination_token_symbol = 'BUSD'
                then 'binance-usd'
                when destination_token_symbol = 'WBTC'
                then 'wrapped-bitcoin'
                when destination_token_symbol = 'crvUSD'
                then 'crvusd'
                when destination_token_symbol = 'FTM'
                then 'fantom'
                when destination_token_symbol = 'PLS'
                then 'plutusdao'
                when destination_token_symbol = 'KLAY'
                then 'klay-token'
                when destination_token_symbol = 'xJEWEL'
                then 'xjewel'
                when destination_token_symbol = 'SYN'
                then 'synapse-2'
                when destination_token_symbol = 'VSTA'
                then 'vesta-finance'
                when destination_token_symbol = 'UST'
                then 'terrausd'
                when destination_token_symbol = 'PEPE'
                then 'pepe'
                when destination_token_symbol = 'GMX'
                then 'gmx'
                when destination_token_symbol = 'USDT'
                then 'tether'
                when destination_token_symbol = 'DOG'
                then 'the-doge-nft'
                when destination_token_symbol = 'SDT'
                then 'stake-dao'
                when destination_token_symbol = 'SFI'
                then 'saffron-finance'
                when destination_token_symbol = 'MOVR'
                then 'moonriver'
                when destination_token_symbol = 'synFRAX'
                then 'frax'
                when destination_token_symbol = 'NFD'
                then 'feisty-doge-nft'
                when destination_token_symbol = 'H2O'
                then 'h2o-dao'
                when destination_token_symbol = 'UNIDX'
                then 'unidex'
                when destination_token_symbol = 'NEWO'
                then 'new-order'
                when destination_token_symbol = 'HIGH'
                then 'highstreet'
                when destination_token_symbol = 'gOHM'
                then 'governance-ohm'
                when destination_token_symbol = 'agEUR'
                then 'ageur'
                when destination_token_symbol = 'JEWEL'
                then 'defi-kingdoms'
                when destination_token_symbol = 'MATIC'
                then 'matic-network'
                when destination_token_symbol = 'USDC'
                then 'usd-coin'
                else ''
            end as destination_gecko_id,
            case
                when origin_token_symbol = 'nETH'
                then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                when origin_token_symbol = 'AVAX'
                then '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7'
                else origin_token_address
            end as origin_token_address,
            origin_token_symbol,
            origin_token_amount,
            destination_block_timestamp,
            case
                when destination_token_symbol = 'nETH'
                then '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                when destination_token_symbol = 'AVAX'
                then '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7'
                else destination_token_address
            end as destination_token_address,
            destination_token_symbol,
            destination_token_amount,
            destination_chain_id,
            origin_chain_id,
            synapse_tx_hash
        from {{ ref("fact_synapse_transfers") }}
    ),

    destination_values as (
        select
            synapse_tx_hash,
            case
                when destination_token_symbol = 'nUSD'
                then destination_token_amount
                else destination_token_amount * price
            end as destination_value
        from synapse_transfers t
        left join
            prices p
            on p.hour = date_trunc('hour', destination_block_timestamp)
            and p.token_address = lower(destination_token_address)
    ),

    origin_values as (
        select
            synapse_tx_hash,
            case
                when origin_token_symbol = 'nUSD'
                then origin_token_amount
                else origin_token_amount * price
            end as origin_value
        from synapse_transfers t
        left join
            prices p
            on p.hour = date_trunc('hour', origin_block_timestamp)
            and p.token_address = lower(origin_token_address)
    ),

    combined as (
        select
            t1.*,
            t2.destination_value,
            t3.origin_value,
            case
                when origin_value > 1000000000
                then destination_value
                when destination_value > 1000000000
                then origin_value
                when origin_value is null
                then destination_value
                when destination_value is null
                then origin_value
                else (origin_value + destination_value) / 2
            end as usd_value
        from synapse_transfers t1
        left join destination_values t2 using (synapse_tx_hash)
        left join origin_values t3 using (synapse_tx_hash)
    ),

    null_combined as (select * from combined where usd_value is null),

    null_destination_values as (
        select
            synapse_tx_hash,
            destination_token_amount
            * shifted_token_price_usd as destination_gecko_value
        from null_combined t1
        left join
            {{ ref("fact_coingecko_token_date_adjusted") }} t2
            on date_trunc('day', destination_block_timestamp) = t2.date
            and destination_gecko_id = coingecko_id
    ),

    null_origin_values as (
        select
            synapse_tx_hash,
            origin_token_amount * shifted_token_price_usd as origin_gecko_value
        from null_combined t1
        left join
            {{ ref("fact_coingecko_token_date_adjusted") }} t2
            on date_trunc('day', origin_block_timestamp) = t2.date
            and origin_gecko_id = coingecko_id
    ),

    gecko_prices as (
        select
            synapse_tx_hash,
            coalesce(origin_gecko_value, destination_gecko_value) as gecko_usd_value
        from null_origin_values ov
        left join null_destination_values dv using (synapse_tx_hash)
    ),

    final_combined as (
        select *, coalesce(usd_value, gecko_usd_value, 0) as final_usd_value
        from combined
        left join gecko_prices using (synapse_tx_hash)
    ),

    dim_contracts as (
        select distinct address, chain, category
        from {{ ref("dim_contracts_gold") }} 
        where category is not null and chain is not null
    ),

    flows_by_chain_id as (
        select
            date_trunc('day', origin_block_timestamp) as date,
            t2.chain as source_chain,
            t3.chain as destination_chain,
            coalesce(t4.category, 'Not Categorized') as category,
            final_usd_value as amount_usd
        from final_combined t1
        left join {{ ref("dim_chain_ids")}} t2 on t1.origin_chain_id = t2.id
        left join {{ ref("dim_chain_ids")}} t3 on t1.destination_chain_id = t3.id
        left join dim_contracts t4 on lower(destination_token_address) = lower(t4.address) and t3.chain = t4.chain
        -- Filter due to synapse data quality issues
        where final_usd_value < 100000000
    )

select
    date,
    'synapse' as app,
    source_chain,
    destination_chain,
    category,
    sum(amount_usd) as amount_usd,
    null as fee_usd
from flows_by_chain_id
group by 1, 2, 3, 4, 5
order by date desc, source_chain asc
