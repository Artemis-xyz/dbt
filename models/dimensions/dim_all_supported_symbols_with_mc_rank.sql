{{ config(materialized="table") }}

with
    tokens_supported as (
        select
            coalesce(meta.coingecko_token_id, dim.coingecko_id) as coingecko_token_id,
            coalesce(meta.token_symbol, dim.symbol) as token_symbol,
            coalesce(meta.token_name, dim.name) as token_name,
            case
                when meta.token_image_small is not null
                then meta.token_image_small
                else null
            end as token_image_small,
            meta.token_market_cap_rank,
            meta.token_categories
        from {{ ref("dim_chain") }} dim
        left join
            {{ ref("dim_coingecko_tokens") }} meta on dim.symbol = meta.token_symbol

        union

        select
            coalesce(meta.coingecko_token_id, dim.coingecko_id) as coingecko_token_id,
            coalesce(meta.token_symbol, dim.symbol) as token_symbol,
            coalesce(meta.token_name, dim.name) as token_name,
            case
                when meta.token_image_small is not null
                then meta.token_image_small
                else null
            end as token_image_small,
            meta.token_market_cap_rank,
            meta.token_categories
        from pc_dbt_db.prod.dim_chains dim
        right join
            {{ ref("dim_coingecko_tokens") }} meta on dim.symbol = meta.token_symbol
        where dim.symbol is null
        order by token_market_cap_rank asc
    )
    , coingecko_data as (
        select
            coingecko_token_id, 
            token_symbol, 
            token_name,
            token_market_cap_rank,
            token_image_small,
            case 
            when 
                array_contains('Stablecoins'::variant, token_categories) or 
                array_contains('Bridged Stablecoins'::variant, token_categories) 
            then 'Stablecoin'
            when 
                array_contains('Liquid Staking'::variant, token_categories) or 
                array_contains('Wrapped-Tokens'::variant, token_categories) or
                array_contains('Restaking'::variant, token_categories) or
                array_contains('Bridged-Tokens'::variant, token_categories) or
                array_contains('Binance-Peg Tokens'::variant, token_categories)
            then 'Staked, Bridged, or Wrapped Asset'
            end as category
        from tokens_supported
    )

select
    coingecko_data.coingecko_token_id,
    coingecko_data.token_symbol,
    coingecko_data.token_name,
    token_image_small,
    token_market_cap_rank,
    coalesce(lower(sigma.category), lower(coingecko_data.category)) as category,
    lower(subcategory) as subcategory,
    lower(tag_1) as tag_1,
    lower(tag_2) as tag_2
from coingecko_data
left join
    {{ source("SIGMA", "coingecko_classification") }} as sigma
    on coingecko_data.coingecko_token_id = sigma.coingecko_id
