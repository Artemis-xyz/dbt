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
            meta.token_market_cap_rank
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
            meta.token_market_cap_rank
        from pc_dbt_db.prod.dim_chains dim
        right join
            {{ ref("dim_coingecko_tokens") }} meta on dim.symbol = meta.token_symbol
        where dim.symbol is null
        order by token_market_cap_rank asc
    )

select
    tokens_supported.coingecko_token_id,
    tokens_supported.token_symbol,
    tokens_supported.token_name,
    token_image_small,
    token_market_cap_rank,
    category,
    subcategory,
    tag_1,
    tag_2
from tokens_supported
left join
    {{ source("SIGMA", "coingecko_classification") }} as sigma
    on tokens_supported.coingecko_token_id = sigma.coingecko_id
