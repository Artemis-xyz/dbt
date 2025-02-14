{{
    config(
        materialized='table',
        snowflake_warehouse='LIQUITY',
        database='LIQUITY',
        schema='raw',
        alias='fact_liquity_v2_tvl'
    )
}}

-- original query: https://dune.com/queries/4611479

with
    constants as (
        select
            '2025-01-14' as bold_deployment_date
    ),
    token as (
        SELECT symbol, address FROM (
            
            values
                (
                    'BOLD',
                    '0xb01dd87b29d187f3e3a4bf6cdaebfb97f3d9ab98'
                ),
                (
                    'WETH',
                    '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                ),
                (
                    'wstETH',
                    '0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0'
                ),
                (
                    'rETH',
                    '0xae78736cd615f374d3085123a210448e74fc6393'
                )
            ) as token (symbol, address)
    ),
    pool as (
        SELECT * FROM (
            values
                -- WETH
                ('0xacece9a6ff7fea9b9e1cdfeee61ca2b45cc4627b'), -- active
                ('0x075e0c707097c4c071056687b0b87cd9392c7bbd'), -- default
                ('0xf69eb8c0d95d4094c16686769460f678727393cf'), -- stability
                -- wstETH
                ('0x2fcf4e86594aadd744f82fd80d5da9b72ab50d7c'), -- active
                ('0x4fd6b1d48900e41710db9f219e153bb56727192b'), -- default
                ('0xcf46dab575c364a8b91bda147720ff4361f4627f'), -- stability
                -- rETH
                ('0x4b0739071d85444121b17b7d0ee23672825d7cff'), -- active
                ('0x26b1d8571560c7942e6dd79377721be81ae817a4'), -- default
                ('0xc4463b26be1a6064000558a84ef9b6a58abe4f7a') --  stability
        )  as pool (address)
    ),
    range as (
        select
            distinct
            date,
            token.symbol as token_symbol,
            token.address as token_address
        from
            pc_dbt_db.prod.dim_date_spine,
            token,
            constants
        WHERE date between bold_deployment_date and to_date(sysdate())
    ),
    xfer as (
        select
            block_timestamp as evt_block_time,
            tx_hash,
            contract_address,
            to_address,
            from_address,
            amount as value
        from
            ethereum_flipside.core.ez_token_transfers,
            constants
        where
            block_timestamp >= bold_deployment_date
    )
    , balance as (
        select
            evt_block_time as time,
            tx_hash,
            token.symbol as token_symbol,
            value as change
        from
            xfer
            join token on xfer.contract_address = token.address
            join pool on xfer.to_address = pool.address
        union all
        select
            evt_block_time,
            tx_hash,
            token.symbol,
            - value
        from
            xfer
            join token on xfer.contract_address = token.address
            join pool on xfer.from_address = pool.address -- pretty good up to here
    ) 
    , daily as (
        select
            range.*,
            coalesce(sum(balance.change), 0) as change
        from
            range
            left join balance on (
                range.date = date_trunc('day', balance.time)
                and range.token_symbol = balance.token_symbol
            )
        group by
            range.date,
            range.token_symbol,
            range.token_address
    ),
    cume as (
        select
            date,
            token_symbol,
            token_address,
            sum(change) over (
                partition by
                    token_symbol
                order by
                    date
            ) as balance
        from
            daily
    )
    select
        cume.date,
        'ethereum' as chain,
        'v2' as version,
        'Liquity' as app,
        token_symbol as token,
        balance as tvl_native,
        balance * case token_symbol
            when 'BOLD' then coalesce(price, 1) -- no BOLD price in the beginning
            else price
        end as tvl_usd
    from
        cume
        left join ethereum_flipside.price.ez_prices_hourly price on (
            cume.token_address = price.token_address
            and cume.date = price.hour
        )