{{
    config(
        materialized="table",
        snowflake_warehouse="BRIDGE_MD",
    )
}}
with
    distinct_tokens as (
        select distinct destination_token as token_address
        from {{ ref("fact_across_transfers") }}
        union all
        select distinct input_token as token_address
        from {{ ref("fact_across_transfers") }}
    ),

    prices as (
        select *
        from ethereum_flipside.price.ez_hourly_token_prices
        where token_address in (select * from distinct_tokens)
        union
        select *
        from optimism_flipside.price.ez_hourly_token_prices
        where token_address in (select * from distinct_tokens)
        union
        select *
        from arbitrum_flipside.price.ez_hourly_token_prices
        where token_address in (select * from distinct_tokens)
        union
        select *
        from polygon_flipside.price.ez_hourly_token_prices
        where token_address in (select * from distinct_tokens)
        union
        select *
        from base_flipside.price.ez_hourly_token_prices
        where token_address in (select * from distinct_tokens)
    ),

    zksync_transfers_v2 as (
        select *
        from {{ ref("fact_across_transfers") }}
        where destination_token_symbol is not null and version = 'v2'
    ),

    zksync_transfers_v3 as (
        select *
        from {{ ref("fact_across_transfers") }}
        where destination_token_symbol is not null and version = 'v3'
    ),

    zksync_tokens as (
        select distinct destination_token as token, destination_token_symbol as symbol
        from zksync_transfers_v2
        union
        select distinct destination_token as token, destination_token_symbol as symbol
        from zksync_transfers_v3
    ),

    zksync_prices as (
        select hour, symbol, decimals, avg(price) as price
        from ethereum_flipside.price.ez_hourly_token_prices
        where
            symbol in (
                select distinct destination_token_symbol
                from zksync_transfers_v2
                union
                select distinct destination_token_symbol
                from zksync_transfers_v3
                where symbol is not null
            )
        group by 1, 2, 3
    ),

    chain_ids as (
        select 'ethereum' as chain, 1 as id
        union
        select 'optimism' as chain, 10 as id
        union
        select 'polygon' as chain, 137 as id
        union
        select 'boba' as chain, 288 as id
        union
        select 'arbitrum' as chain, 42161 as id
        union
        select 'base' as chain, 8453 as id
        union
        select 'zksync' as chain, 324 as id
    ),

    zksync_volume_and_fees_by_chain_and_symbol_v2 as (
        with
            temp as (
                select
                    date_trunc('hour', block_timestamp) as hour,
                    origin_chain_id,
                    destination_chain_id,
                    destination_token,
                    sum(
                        (coalesce(amount, 0) / power(10, p.decimals)) * price
                    ) as amount_usd,
                    sum(
                        ((coalesce(amount, 0) / power(10, p.decimals)) * price)
                        * (relayer_fee_pct + realized_lp_fee_pct)
                    ) as fee_usd
                from zksync_transfers_v2 t
                left join
                    zksync_prices p
                    on date_trunc('hour', t.block_timestamp) = p.hour
                    and t.destination_token_symbol = p.symbol
                group by 1, 2, 3, 4
            )

        select
            hour,
            c1.chain as source_chain,
            c2.chain as destination_chain,
            destination_token as token,
            coalesce(amount_usd, 0) as amount_usd,
            fee_usd
        from temp t
        left join chain_ids c1 on t.origin_chain_id = c1.id
        left join chain_ids c2 on t.destination_chain_id = c2.id
    ),
    zksync_volume_and_fees_by_chain_and_symbol_v3 as (
        with
            temp as (
                select
                    date_trunc('hour', block_timestamp) as hour,
                    origin_chain_id,
                    destination_chain_id,
                    destination_token,
                    sum(
                        (coalesce(amount, 0) / power(10, p1.decimals)) * p1.price
                    ) as amount_usd,

                    sum(
                        (coalesce(input_amount, 0) / power(10, p2.decimals)) * p2.price
                    ) as input_amount_usd,

                    input_amount_usd - amount_usd as fee_usd
                from zksync_transfers_v3 t
                left join
                    zksync_prices p1
                    on date_trunc('hour', t.block_timestamp) = p1.hour
                    and t.destination_token_symbol = p1.symbol

                left join
                    prices p2
                    on date_trunc('hour', t.block_timestamp) = p2.hour
                    and lower(t.input_token) = lower(p2.token_address)

                group by 1, 2, 3, 4
            )

        select
            hour,
            c1.chain as source_chain,
            c2.chain as destination_chain,
            destination_token as token,
            coalesce(amount_usd, 0) as amount_usd,
            fee_usd
        from temp t
        left join chain_ids c1 on t.origin_chain_id = c1.id
        left join chain_ids c2 on t.destination_chain_id = c2.id
    ),

    non_zksync_volume_and_fees_by_chain_and_symbol_v1_v2_uba as (
        with
            temp as (
                select
                    date_trunc('hour', block_timestamp) as hour,
                    origin_chain_id,
                    destination_chain_id,
                    destination_token,
                    sum(
                        (coalesce(amount, 0) / power(10, p.decimals)) * price
                    ) as amount_usd,
                    sum(
                        ((coalesce(amount, 0) / power(10, p.decimals)) * price)
                        * (relayer_fee_pct + realized_lp_fee_pct)
                    ) as fee_usd
                from {{ ref("fact_across_transfers") }} t
                left join
                    prices p
                    on date_trunc('hour', t.block_timestamp) = p.hour
                    and t.destination_token = p.token_address
                where
                    t.destination_token_symbol is null
                    and version in ('v1', 'uba', 'v2')
                group by 1, 2, 3, 4
            )

        select
            hour,
            c1.chain as source_chain,
            c2.chain as destination_chain,
            destination_token as token,
            coalesce(amount_usd, 0) as amount_usd,
            fee_usd
        from temp t
        left join chain_ids c1 on t.origin_chain_id = c1.id
        left join chain_ids c2 on t.destination_chain_id = c2.id
    ),

    zksync_source_chain_volume_and_fees_v3 as (
        with
            temp as (
                select
                    date_trunc('hour', block_timestamp) as hour,
                    origin_chain_id,
                    destination_chain_id,
                    destination_token,
                    sum(
                        (coalesce(amount, 0) / power(10, p1.decimals)) * p1.price
                    ) as amount_usd,

                    sum(
                        (coalesce(input_amount, 0) / power(10, p2.decimals)) * p2.price
                    ) as amount_in_usd,

                    amount_in_usd - amount_usd as fee_usd
                from {{ ref("fact_across_transfers") }} t
                left join
                    prices p1
                    on date_trunc('hour', t.block_timestamp) = p1.hour
                    and t.destination_token = p1.token_address

                left join zksync_tokens zt on lower(t.input_token) = lower(zt.token)

                left join
                    zksync_prices p2
                    on date_trunc('hour', t.block_timestamp) = p2.hour
                    and zt.symbol = p2.symbol

                where
                    t.destination_token_symbol is null
                    and version = 'v3'
                    and origin_chain_id = 324
                group by 1, 2, 3, 4
            )

        select
            hour,
            c1.chain as source_chain,
            c2.chain as destination_chain,
            destination_token as token,
            coalesce(amount_usd, 0) as amount_usd,  -- I am here 
            fee_usd
        from temp t
        left join chain_ids c1 on t.origin_chain_id = c1.id
        left join chain_ids c2 on t.destination_chain_id = c2.id
    ),

    non_zksync_volume_and_fees_by_chain_and_symbol_v3 as (
        with
            temp as (
                select
                    date_trunc('hour', block_timestamp) as hour,
                    origin_chain_id,
                    destination_chain_id,
                    destination_token,
                    sum(
                        (coalesce(amount, 0) / power(10, p1.decimals)) * p1.price
                    ) as amount_usd,

                    sum(
                        (coalesce(input_amount, 0) / power(10, p2.decimals)) * p2.price
                    ) as amount_in_usd,

                    amount_in_usd - amount_usd as fee_usd
                from {{ ref("fact_across_transfers") }} t
                left join
                    prices p1
                    on date_trunc('hour', t.block_timestamp) = p1.hour
                    and t.destination_token = p1.token_address

                left join
                    prices p2
                    on date_trunc('hour', t.block_timestamp) = p2.hour
                    and t.input_token = p2.token_address

                where
                    t.destination_token_symbol is null
                    and version = 'v3'
                    and origin_chain_id != 324
                group by 1, 2, 3, 4
            )

        select
            hour,
            c1.chain as source_chain,
            c2.chain as destination_chain,
            destination_token as token,
            coalesce(amount_usd, 0) as amount_usd,
            fee_usd
        from temp t
        left join chain_ids c1 on t.origin_chain_id = c1.id
        left join chain_ids c2 on t.destination_chain_id = c2.id
    ),
    flows_by_token as (
        select
            date_trunc('day', hour) as date,
            'across' as app,
            source_chain,
            destination_chain,
            token,
            sum(amount_usd) as amount_usd,
            sum(fee_usd) as fee_usd
        from
            (
                select *
                from non_zksync_volume_and_fees_by_chain_and_symbol_v1_v2_uba

                union

                select *
                from zksync_volume_and_fees_by_chain_and_symbol_v2

                union

                select *
                from zksync_volume_and_fees_by_chain_and_symbol_v3

                union

                select *
                from non_zksync_volume_and_fees_by_chain_and_symbol_v3

                union

                select *
                from zksync_source_chain_volume_and_fees_v3
            ) t
        group by 1, 2, 3, 4, 5
    )
select
    t1.date,
    t1.app,
    t1.source_chain,
    t1.destination_chain,
    t2.category,
    sum(t1.amount_usd) as amount_usd,
    sum(t1.fee_usd) as fee_usd
from flows_by_token t1
left join {{ ref("dim_contracts_gold") }} t2 on lower(token) = lower(address)
group by date, t1.app, source_chain, destination_chain, category
order by date desc, source_chain asc
