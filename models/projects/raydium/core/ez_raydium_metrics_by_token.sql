{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="RAYDIUM",
        database="raydium",
        schema="core",
        alias="ez_metrics_by_token",
    )
}}


WITH buyback_fee as (
    select t.block_timestamp
        , t.block_id
        , t.tx_id 
        , t.amount as amout_raw 
        , t.mint as mint_address
        /** Can't use the approach of first row <> last row combo to get tokens and RAY price, because the ordering is occasionally messed up https://solscan.io/tx/5S4CXkijWmnbkHA6TY5DFPkhfWxACUJuTLC2bLUDdEW56TgFwbXCfJipX6eySGQMAkpPLj4apjhxYaPCfVfb76RE
        -- , ROW_NUMBER() OVER (PARTITION BY t.block_id, t.tx_id ORDER BY t.index::float ASC) as rn_asc
        -- , ROW_NUMBER() OVER (PARTITION BY t.block_id, t.tx_id ORDER BY t.index::float DESC) as rn_desc
        */
        , COUNT(*) OVER (PARTITION BY t.block_id, t.tx_id, t.mint) AS t_count
    from SOLANA_FLIPSIDE.CORE.FACT_TRANSFERS t
    
    where 1=1
        and exists ( -- filter for txns which includes RAY conversion, so we can grab the first transfer 
            select 1
            FROM SOLANA_FLIPSIDE.CORE.FACT_TRANSFERS tr
            WHERE tr.tx_to in (
                'projjosVCPQH49d5em7VYS7fJZzaqKixqKtus7yk416' -- buyback fee collector for CLMM
                , 'PNLCQcVCD26aC7ZWgRyr5ptfaR7bBrWdTFgRWwu2tvF' -- buyback fee collector for AMMv4
                , 'ProCXqRcXJjoUd1RNoo28bSizAA6EEqt9wURZYPDc5u' -- buyback fee collector for CPMM
                )
                and tr.mint = '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R' -- RAY
                and t.block_id = tr.block_id and t.tx_id = tr.tx_id
                {% if is_incremental() %}
                    AND tr.block_timestamp::date >= (select dateadd('day', -2, max(date)) from {{ this }})
                {% else %}
                    AND tr.block_timestamp::date >= date('2022-04-22') 
                {% endif %}

        )
        and not exists ( -- filter out collectProtocolFee in case of xxx<>RAY pool
            select 1
            FROM SOLANA_FLIPSIDE.CORE.EZ_EVENTS_DECODED ed
            WHERE ed.program_id in (
                    'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK' -- CLMM program
                    , '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8' -- AMM program
                ) 
                and ed.event_type in ('collectProtocolFee', 'withdrawPnl')
                and t.block_id = ed.block_id and t.tx_id = ed.tx_id
                {% if is_incremental() %}
                    AND ed.block_timestamp::date >= (select dateadd('day', -2, max(date)) from {{ this }})
                {% else %}
                    AND ed.block_timestamp::date >= date('2022-04-22') 
                {% endif %}
        )
        and not exists ( -- filter out CPMM collectProtocolFee in case of xxx<>RAY pool (Flipside decoded table broken for CPMM, so doing this werid way)
            select 1
            from SOLANA_FLIPSIDE.CORE.FACT_TRANSFERS t2 
            left join SOLANA_FLIPSIDE.CORE.FACT_EVENTS e on e.block_id = t2.block_id and e.tx_id = t2.tx_id
            where t2.mint = '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R' -- RAY
                and t2.tx_to ='ProCXqRcXJjoUd1RNoo28bSizAA6EEqt9wURZYPDc5u' -- buyback fee collector for CPMM 
                and e.program_id = 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C' --CPMM program
                and t.block_id = e.block_id and t.tx_id = e.tx_id
                {% if is_incremental() %}
                    AND t2.block_timestamp::date >= (select dateadd('day', -2, max(date)) from {{ this }})
                {% else %}
                    AND t2.block_timestamp::date >= date('2022-04-22') 
                {% endif %}
        )
        {% if is_incremental() %}
            AND t.block_timestamp::date >= (select dateadd('day', -2, max(date)) from {{ this }})
        {% else %}
            AND t.block_timestamp::date >= date('2022-04-22') 
        {% endif %}
)

, buyback as (

    select day
        , token_mint_address
        , token_symbol
        , sum(buyback_fee_raw) as buyback_fee_raw
        , sum(buyback_fee_usd) as buyback_fee_usd
    from (
        select date_trunc('day', f.block_timestamp) as day
            , f.mint_address as token_mint_address
            , case when f.mint_address = 'So11111111111111111111111111111111111111112' then 'WSOL' else coalesce(m.symbol, f.mint_address) end as token_symbol
            , sum(l.amout_raw) as buyback_fee_raw
            , sum(l.amout_raw * coalesce(p.price, 0)) as buyback_fee_usd
        from buyback_fee f 
        left join SOLANA_FLIPSIDE.PRICE.EZ_ASSET_METADATA m on m.token_address = f.mint_address
        inner join buyback_fee l on f.block_id = l.block_id and f.tx_id = l.tx_id 
            and l.t_count =1 and l.mint_address = '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R' -- RAY
        left join SOLANA_FLIPSIDE.PRICE.EZ_PRICES_HOURLY p on p.token_address = '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R'
            and p.hour = date_trunc('hour', l.block_timestamp) and p.blockchain = 'solana'
        
        where f.t_count = 1 and f.mint_address != '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R' -- original token swapped for RAY
        group by 1,2,3
    
        UNION ALL 
    
        select date_trunc('day', t.block_timestamp) as day
            , t.mint as token_mint_address
            , 'RAY' as token_symbol
            , sum(t.amount) as buyback_fee_raw 
            , sum(t.amount * coalesce(p.price, 0)) as buyback_fee_usd
        from SOLANA_FLIPSIDE.CORE.FACT_TRANSFERS t
        left join SOLANA_FLIPSIDE.PRICE.EZ_PRICES_HOURLY p on p.token_address = '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R'
            and p.hour = date_trunc('hour', t.block_timestamp) and p.blockchain = 'solana'
            
        where 1=1 
            -- filter for RAY directly transferred to projjosVCPQH49d5em7VYS7fJZzaqKixqKtus7yk416 in case of xxx<>RAY pool
            and t.mint = '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R' -- filter for RAY only, the other tokens are already accounted for
            and exists ( 
                select 1
                FROM SOLANA_FLIPSIDE.CORE.EZ_EVENTS_DECODED ed
                WHERE ed.program_id in (
                        'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK' -- CLMM program id
                        , '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8' -- AMM program id
                    ) 
                    and ed.event_type in ('collectProtocolFee', 'withdrawPnl')
                    and t.block_id = ed.block_id and t.tx_id = ed.tx_id
                    {% if is_incremental() %}
                        AND ed.block_timestamp::date >= (select dateadd('day', -2, max(date)) from {{ this }})
                    {% else %}
                        AND ed.block_timestamp::date >= date('2022-04-22') 
                    {% endif %}
            )
            and exists ( -- filter for CPMM collectProtocolFee in case of xxx<>RAY pool (Flipside decoded table broken for CPMM, so doing this werid way)
                select 1
                from SOLANA_FLIPSIDE.CORE.FACT_TRANSFERS t2 
                left join SOLANA_FLIPSIDE.CORE.FACT_EVENTS e on e.block_id = t2.block_id and e.tx_id = t2.tx_id
                where t2.mint = '4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R' -- RAY
                    and t2.tx_to ='ProCXqRcXJjoUd1RNoo28bSizAA6EEqt9wURZYPDc5u' -- buyback fee collector for CPMM 
                    and e.program_id = 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C' --CPMM program
                    {% if is_incremental() %}
                        AND t2.block_timestamp::date >= (select dateadd('day', -2, max(date)) from {{ this }})
                    {% else %}
                        AND t2.block_timestamp::date >= date('2022-04-22') 
                    {% endif %}
                    and t.block_id = e.block_id and t.tx_id = e.tx_id
            )
            {% if is_incremental() %}
                AND t.block_timestamp::date >= (select dateadd('day', -2, max(date)) from {{ this }})
            {% else %}
                AND t.block_timestamp::date >= date('2022-04-22') 
            {% endif %}
        group by 1,2,3
    ) 
    group by 1,2,3
)

, treasury_fee as (
    select t.block_timestamp
        , t.block_id
        , t.tx_id 
        , t.amount as amout_raw 
        , t.mint as mint_address
        /*
        Can't use the approach of first row <> last row combo to get tokens and USDC price, because the ordering is occasionally messed up
        */
        -- , ROW_NUMBER() OVER (PARTITION BY t.block_id, t.tx_id ORDER BY t.index::float ASC) as rn_asc
        -- , ROW_NUMBER() OVER (PARTITION BY t.block_id, t.tx_id ORDER BY t.index::float DESC) as rn_desc
        , COUNT(*) OVER (PARTITION BY t.block_id, t.tx_id, t.mint) AS t_count
    from SOLANA_FLIPSIDE.CORE.FACT_TRANSFERS t
    
    where 1=1
        and exists ( -- filter for txns which includes USDC conversion, so we can grab the first transfer 
            select 1
            FROM SOLANA_FLIPSIDE.CORE.FACT_TRANSFERS tr
            WHERE tr.tx_to in (
                    'FundHfY8oo8J9KYGyfXFFuQCHe7Z1VBNmsj84eMcdYs4' -- treasury fee collector for CLMM
                    , 'FUNDduJTA7XcckKHKfAoEnnhuSud2JUCUZv6opWEjrBU' -- treasury fee collector for CPMM 
                )
                and tr.mint = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v' -- USDC
                and t.block_id = tr.block_id and t.tx_id = tr.tx_id
                {% if is_incremental() %}
                    AND tr.block_timestamp::date >= (select dateadd('day', -2, max(date)) from {{ this }})
                {% else %}
                    AND tr.block_timestamp::date >= date('2022-04-22') 
                {% endif %}
        )
        and not exists ( -- filter out collectFundFee in case of xxx<>USDC pool
            select 1
            FROM SOLANA_FLIPSIDE.CORE.EZ_EVENTS_DECODED ed
            WHERE ed.program_id = 'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK' -- CLMM program id 
                and ed.event_type = 'collectFundFee' 
                and t.block_id = ed.block_id and t.tx_id = ed.tx_id
                {% if is_incremental() %}
                    AND ed.block_timestamp::date >= (select dateadd('day', -2, max(date)) from {{ this }})
                {% else %}
                    AND ed.block_timestamp::date >= date('2022-04-22') 
                {% endif %}
        )
        and not exists ( -- filter out CPMM collectFundFee in case of xxx<>USDC pool, flispide decoded event table broken for CPMM
            select 1
            from SOLANA_FLIPSIDE.CORE.FACT_TRANSFERS t2 
            left join SOLANA_FLIPSIDE.CORE.FACT_EVENTS e on e.block_id = t2.block_id and e.tx_id = t2.tx_id
            where t2.mint = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v' -- USDC
                and t2.tx_to ='FUNDduJTA7XcckKHKfAoEnnhuSud2JUCUZv6opWEjrBU' -- treasury fee collector for CPMM 
                and e.program_id = 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C' --CPMM program
                {% if is_incremental() %}
                    AND t2.block_timestamp::date >= (select dateadd('day', -2, max(date)) from {{ this }})
                {% else %}
                    AND t2.block_timestamp::date >= date('2022-04-22') 
                {% endif %}
                and t.block_id = e.block_id and t.tx_id = e.tx_id
        )
        {% if is_incremental() %}
            AND t.block_timestamp::date >= (select dateadd('day', -2, max(date)) from {{ this }})
        {% else %}
            AND t.block_timestamp::date >= date('2022-04-22') 
        {% endif %}
)

, treasury as (

    select day
        , token_mint_address
        , token_symbol
        , sum(treasury_fee_raw) as treasury_fee_raw
        , sum(treasury_fee_usd) as treasury_fee_usd
    from (
        select date_trunc('day', f.block_timestamp) as day
            , f.mint_address as token_mint_address
            , case when f.mint_address = 'So11111111111111111111111111111111111111112' then 'WSOL' else coalesce(m.symbol, f.mint_address) end as token_symbol
            , sum(l.amout_raw) as treasury_fee_raw
            , sum(l.amout_raw * coalesce(p.price, 0)) as treasury_fee_usd
        from treasury_fee f 
        left join SOLANA_FLIPSIDE.PRICE.EZ_ASSET_METADATA m on m.token_address = f.mint_address
        inner join treasury_fee l on f.block_id = l.block_id and f.tx_id = l.tx_id 
            and l.t_count =1 and l.mint_address = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v' -- USDC
        left join SOLANA_FLIPSIDE.PRICE.EZ_PRICES_HOURLY p on p.token_address = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
            and p.hour = date_trunc('hour', l.block_timestamp) and p.blockchain = 'solana'
        
        where f.t_count = 1 and f.mint_address != 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v' -- original token swapped for USDC
        group by 1,2,3
    
        UNION ALL 
    
        select date_trunc('day', t.block_timestamp) as day
            , t.mint as token_mint_address
            , 'USDC' as token_symbol
            , sum(t.amount) as treasury_fee_raw 
            , sum(t.amount * coalesce(p.price, 0)) as treasury_fee_usd
        from SOLANA_FLIPSIDE.CORE.FACT_TRANSFERS t
        left join SOLANA_FLIPSIDE.PRICE.EZ_PRICES_HOURLY p on p.token_address = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
            and p.hour = date_trunc('hour', t.block_timestamp) and p.blockchain = 'solana'
            
        where 1=1 
            -- filter for USDC directly transferred to FundHfY8oo8J9KYGyfXFFuQCHe7Z1VBNmsj84eMcdYs4 in case of xxx<>USDC pool
            and t.mint = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v' -- filter for USDC only, the other tokens are already accounted for
            and exists ( 
                select 1
                FROM SOLANA_FLIPSIDE.CORE.EZ_EVENTS_DECODED ed
                WHERE ed.program_id = 'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK' -- raydium concentrated liquidity
                    and ed.event_type = 'collectFundFee' 
                    and t.block_id = ed.block_id and t.tx_id = ed.tx_id
                    {% if is_incremental() %}
                        AND ed.block_timestamp::date >= (select dateadd('day', -2, max(date)) from {{ this }})
                    {% else %}
                        AND ed.block_timestamp::date >= date('2022-04-22') 
                    {% endif %}
            )
            and exists ( -- filter for CPMM collectFundFee in case of xxx<>USDC pool, flispide decoded event table broken for CPMM
                select 1
                from SOLANA_FLIPSIDE.CORE.FACT_TRANSFERS t2 
                left join SOLANA_FLIPSIDE.CORE.FACT_EVENTS e on e.block_id = t2.block_id and e.tx_id = t2.tx_id
                where t2.mint = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v' -- USDC
                    and t2.tx_to ='FUNDduJTA7XcckKHKfAoEnnhuSud2JUCUZv6opWEjrBU' -- treasury fee collector for CPMM 
                    and e.program_id = 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C' --CPMM program
                    {% if is_incremental() %}
                        AND t2.block_timestamp::date >= (select dateadd('day', -2, max(date)) from {{ this }})
                    {% else %}
                        AND t2.block_timestamp::date >= date('2022-04-22') 
                    {% endif %}
                    and t.block_id = e.block_id and t.tx_id = e.tx_id
            )
            {% if is_incremental() %}
                AND t.block_timestamp::date >= (select dateadd('day', -2, max(date)) from {{ this }})
            {% else %}
                AND t.block_timestamp::date >= date('2022-04-22') 
            {% endif %}
        group by 1,2,3
    ) 
    group by 1,2,3
)

, trading_volume as (
    select day
        , token_mint_address
        , coalesce(token_symbol, m.symbol) as token_symbol
        , sum(swap_volume_raw) as swap_volume_raw
        , sum(swap_volume_usd) as swap_volume_usd
    from (
    
        select date_trunc('day', block_timestamp) as day
            , swap_from_mint as token_mint_address
            , swap_from_symbol as token_symbol
            , sum(swap_from_amount) as swap_volume_raw 
            , sum(swap_from_amount_usd) as swap_volume_usd
        from SOLANA_FLIPSIDE.DEFI.EZ_DEX_SWAPS dex 
        where lower(swap_program) like '%raydium%'
            and (swap_from_amount_usd is not null and swap_to_amount_usd is not null) 
            and (swap_from_amount_usd > 0 and swap_to_amount_usd > 0)
            and abs(
                ln(coalesce(nullif(swap_from_amount_usd, 0), 1)) / ln(10)
                - ln(coalesce(nullif(swap_to_amount_usd, 0), 1)) / ln(10)
            ) < 1
            {% if is_incremental() %}
                AND dex.block_timestamp::date >= (select dateadd('day', -2, max(date)) from {{ this }})
            {% else %}
                AND dex.block_timestamp::date >= date('2022-04-22') 
            {% endif %}
        group by 1,2,3
        
        
        UNION ALL 
        
        select date_trunc('day', block_timestamp) as day
            , swap_to_mint as token_mint_address
            , swap_to_symbol as token_symbol
            , sum(swap_to_amount) as swap_volume_raw 
            , sum(swap_to_amount_usd) as swap_volume_usd
        from SOLANA_FLIPSIDE.DEFI.EZ_DEX_SWAPS dex 
        where lower(swap_program) like '%raydium%'
            and (swap_from_amount_usd is not null and swap_to_amount_usd is not null) 
            and (swap_from_amount_usd > 0 and swap_to_amount_usd > 0)
            and abs(
                ln(coalesce(nullif(swap_from_amount_usd, 0), 1)) / ln(10)
                - ln(coalesce(nullif(swap_to_amount_usd, 0), 1)) / ln(10)
            ) < 1
            {% if is_incremental() %}
                AND dex.block_timestamp::date >= (select dateadd('day', -2, max(date)) from {{ this }})
            {% else %}
                AND dex.block_timestamp::date >= date('2022-04-22') 
            {% endif %}
        group by 1,2,3
    ) x 
    left join SOLANA_FLIPSIDE.PRICE.EZ_ASSET_METADATA m on m.token_address = x.token_mint_address and m.blockchain = 'solana'
    group by 1,2,3
)

, pool_creation as (
    select date_trunc('day', x.block_timestamp) as day 
        , token_mint_address
        , m.symbol as token_symbol
        , sum(amount_raw) as pool_creation_fee_raw 
        , sum(amount_raw * p.price) as pool_creation_fee_usd 
    from (
        select t.block_timestamp
            , t.mint as token_mint_address
            -- , case when t2.tx_to = 'DNXgeM9EiiaAbaWvwjHj9fQQLAX5ZsfHyvmYUNRAdNC8' then 0.15 / 2 -- CPMM
            --     when t2.tx_to = '7YttLkHDoNj9wyDur5pM1ejNaAvT9X4eqaYcHQqtj2G5' and t.block_timestamp >= '2024-02-16' then 0.4/2 -- AMMv4
            --     else 0.68 / 2 -- older AMMv4 charges 0.68 SOL
            -- end as amount_raw
            , case when t2.tx_to = 'DNXgeM9EiiaAbaWvwjHj9fQQLAX5ZsfHyvmYUNRAdNC8' then t2.amount / 2 -- CPMM
                else t2.amount / 2 -- AMMv4
            end as amount_raw 
        from SOLANA_FLIPSIDE.CORE.FACT_TRANSFERS t 
        inner join SOLANA_FLIPSIDE.CORE.FACT_TRANSFERS t2 on t2.block_id = t.block_id and t2.tx_id = t.tx_id
            and t2.tx_to in (
                    'DNXgeM9EiiaAbaWvwjHj9fQQLAX5ZsfHyvmYUNRAdNC8' -- CPMM (0.15SOL)
                    , '7YttLkHDoNj9wyDur5pM1ejNaAvT9X4eqaYcHQqtj2G5' -- AMMv4 (0.4, 0.68)
                )
            and t2.mint = 'So11111111111111111111111111111111111111111' -- SOL
        where t.mint != 'So11111111111111111111111111111111111111111' -- excluding SOL
            {% if is_incremental() %}
                AND t.block_timestamp::date >= (select dateadd('day', -2, max(date)) from {{ this }})
            {% else %}
                AND t.block_timestamp::date >= date('2022-04-22') 
            {% endif %}
            -- and t2.tx_id = '5ts75HwS7dCUum8yA5388AJ4G1qXxcvYR3xbPLeTxNv1dSuy4GWPAzbkwRXhsTEScjQVgpjWKJBDFdG16p622qp3' and t2.block_id = 288816886
    ) x 
    
    left join SOLANA_FLIPSIDE.PRICE.EZ_ASSET_METADATA m on m.token_address = x.token_mint_address
    left join SOLANA_FLIPSIDE.PRICE.EZ_PRICES_HOURLY p on p.token_address = 'So11111111111111111111111111111111111111112' 
                and p.hour = date_trunc('hour', x.block_timestamp) and p.blockchain = 'solana'
    group by 1,2,3
)

, labeling as (
    select day, token_symbol
        , case 
            when token_symbol in ('SOL', 'WSOL') then 'Native'
            when POSITION('Stablecoin' IN array_to_string(token_categories, ',')) > 0 then 'Stablecoins'
            when POSITION('Meme' IN array_to_string(token_categories, ',')) > 0 then 'Meme'
            when POSITION('Pump.fun' IN array_to_string(token_categories, ',')) > 0 then 'Meme'
            when POSITION('Cat-Themed' IN array_to_string(token_categories, ',')) > 0 then 'Meme'
            when POSITION('Dog-Themed' IN array_to_string(token_categories, ',')) > 0 then 'Meme'
            
            when POSITION('Liquid Staking' IN array_to_string(token_categories, ',')) > 0 then 'LST'
            when POSITION('Liquid Staked ETH' IN array_to_string(token_categories, ',')) > 0 then 'LST'
            when POSITION('Restaking' IN array_to_string(token_categories, ',')) > 0 then 'Restaking'
    
            when POSITION('Artificial Intelligence' IN array_to_string(token_categories, ',')) > 0 then 'AI'
            when POSITION('DeFi' IN array_to_string(token_categories, ',')) > 0 then 'DeFi'
            when POSITION('Gaming' IN array_to_string(token_categories, ',')) > 0 then 'Gaming'
            when POSITION('DePIN' IN array_to_string(token_categories, ',')) > 0 then 'DePIN'
            when POSITION('RWA' IN array_to_string(token_categories, ',')) > 0 then 'RWA'
            when POSITION('Governance' IN array_to_string(token_categories, ',')) > 0 then 'Governance'
            when POSITION('Protocol' IN array_to_string(token_categories, ',')) > 0 then 'Other Protocol'
            when POSITION('Infrastructure' IN array_to_string(token_categories, ',')) > 0 then 'Other Protocol'
            
            else 'Unlabeled'
        end as category
    from (
        select b.day 
            , b.token_symbol
            , array_agg(t.token_categories) as token_categories
        from buyback b 
        left join PC_DBT_DB.PROD.DIM_COINGECKO_TOKENS t on t.token_symbol = lower(b.token_symbol)
        group by 1,2
    )

)

select b.day as date
    , b.token_mint_address
    , b.token_symbol
    , l.category
    
    , v.swap_volume_raw as trading_volume_native -- native = token_mint_address
    , v.swap_volume_usd as trading_volume 
    
    , b.buyback_fee_usd / 0.12 + coalesce(c.pool_creation_fee_usd, 0) as fees -- trading fee + pool creation 
    , b.buyback_fee_usd / 0.12 as trading_fees -- total_trading_fee = revenue (12%) + treasury (4%) + LP(84%)
    
    , coalesce(b.buyback_fee_usd, 0) + coalesce(t.treasury_fee_usd, 0) as revenue -- revenue = buyback + treasury 
    , b.buyback_fee_usd as buyback
    , b.buyback_fee_raw as buyback_native -- native = RAY
    , t.treasury_fee_usd as treasury_fees
    , t.treasury_fee_raw as treasury_fees_native -- native = USDC
    , c.pool_creation_fee_usd as pool_creation_fees 
    , c.pool_creation_fee_raw as pool_creation_fees_native -- raw = SOL

from buyback b
left join treasury t on t.day = b.day and b.token_mint_address = t.token_mint_address
left join trading_volume v on b.day = v.day and b.token_mint_address = v.token_mint_address
left join pool_creation c on c.day = b.day and c.token_mint_address = b.token_mint_address

left join labeling l on b.day = l.day and b.token_symbol = l.token_symbol 

where coalesce(v.day, b.day) < to_date(sysdate())
order by 1 desc, 6 desc
