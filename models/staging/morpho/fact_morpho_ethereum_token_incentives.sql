{{ config(materialized='table', snowflake_warehouse='MORPHO') }}

with
    legacy_token_incentives_txns as (
        select
            date(block_timestamp) as date
            , tx_hash
            , cast(raw_amount_precise as string)::float / 1e18 as amount_native
            , contract_address
        from {{ source('ethereum_flipside', 'ez_token_transfers') }}
        where 1=1
            and lower(from_address) in (lower('0x3B14E5C73e0A56D607A8688098326fD4b4292135'),
                                        lower('0x60345417a227ad7E312eAa1B5EC5CD1Fe5E2Cdc6'))
            and lower(contract_address) = lower('0x9994E35Db50125E0DF82e4c2dde62496CE330999')
    )
    
    , morpho_prices as (
        {{  get_coingecko_metrics('morpho') }}
    )

    , morpho_migration_date_price as (
        select
            date
            , price
            , market_cap
            , fdmc
            , token_turnover_circulating
            , token_turnover_fdv
            , token_volume
        from morpho_prices
        -- when people transfer their Legacy Morpho Tokens to Morpho Tokens
        where date = '2024-11-21'
    )

    , agg_old_morpho_token_incentives as (
        select
            ti.date as date
            , sum(amount_native) as amount_native
            , sum(amount_native) * max(price) as amount_usd
        from legacy_token_incentives_txns ti
        left join morpho_migration_date_price on true
        group by 1
    )

    , legacy_morpho_token_incentives as (
        select
            block_timestamp
            , tx_hash
            , from_address
            , to_address
            , contract_address
            , cast(raw_amount_precise as string)::float / 1e18 as amount_native
        from {{ source('ethereum_flipside', 'ez_token_transfers') }}
        where 1=1
            and lower(from_address) = lower('0x330eefa8a787552DC5cAd3C3cA644844B1E61Ddb')
            and lower(contract_address) = lower('0x58D97B57BB95320F9a05dC918Aef65434969c2B2')            
    )

    , agg_morpho_token_incentives as (
        select
            date(old.block_timestamp) as date,
            sum(old.amount_native) as amount_native,
            sum(old.amount_native) * max(mm.price) as amount_usd
        from legacy_morpho_token_incentives old
        left join morpho_prices mm
            on date(old.block_timestamp) = mm.date
        group by 1
    )

    , morpho_ethereum_token_incentives as (
        select
            coalesce(new_morpho.date, old_morpho.date) as date
            , coalesce(new_morpho.amount_native, 0) + coalesce(old_morpho.amount_native, 0) as amount_native
            , coalesce(new_morpho.amount_usd, 0) + coalesce(old_morpho.amount_usd, 0) as amount_usd
        from agg_morpho_token_incentives new_morpho
        full outer join agg_old_morpho_token_incentives old_morpho
            on new_morpho.date = old_morpho.date
    )

select
    date
    , 'ethereum' as chain
    , amount_native
    , amount_usd
from morpho_ethereum_token_incentives