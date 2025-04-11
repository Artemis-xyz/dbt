{{
    config(
        materialized="incremental",
        unique_key=["date", "token_pair", "program"],
        snowflake_warehouse="RAYDIUM"
    )
}}

with amm_pool_creation AS ( -- special treatment for AMM v4 as the buyback event doesn't emit token mint addresses needed
    select distinct pool_address, token0_mint_address, token1_mint_address
    from (
        SELECT block_timestamp, block_id, tx_id,
            MAX(CASE WHEN account_name = 'amm' THEN pubkey END) AS pool_address,
            MAX(CASE WHEN account_name = 'coinMint' THEN pubkey END) AS token0_mint_address,
            MAX(CASE WHEN account_name = 'pcMint' THEN pubkey END) AS token1_mint_address
        FROM (
            SELECT 
                d.block_timestamp,
                d.block_id,
                d.tx_id,
                d.event_type,
                CAST(f.value:pubkey AS VARCHAR) AS pubkey, -- Cast to VARCHAR to remove quotes
                f.value:name AS account_name
            FROM (
                SELECT 
                    ed.block_timestamp,
                    ed.block_id,
                    ed.tx_id,
                    ed.event_type,
                    ed.decoded_instruction
                FROM SOLANA_FLIPSIDE.CORE.EZ_EVENTS_DECODED ed 
                WHERE 1=1
                    and ed.program_id = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8' -- Raydium Liquidity Pool V4
                    AND ed.event_type = 'initialize2' 
            ) d,
            LATERAL FLATTEN(input => parse_json(d.decoded_instruction):accounts) f 
        
        )
        group by 1,2,3
    )
)

-- select * from amm_pool_creation where pool_address = '8mARaeykopFHkmv6eX2rH2wTcCf2SLoLMaKzgHyUYQj3'

, buyback_event as (
    select b.block_timestamp
        , b.block_id
        , b.tx_id 
        , case when m0.symbol > m1.symbol then coalesce(m0.symbol, b.token0_mint_address) || '-' || coalesce(m1.symbol, b.token1_mint_address)
            else coalesce(m1.symbol, b.token1_mint_address) || '-' || coalesce(m0.symbol, b.token0_mint_address)
        end as token_pair 
        , b.pool_address
        , b.program
    from (
        select i.block_timestamp, i.block_id, i.tx_id, i.pool_address
            , coalesce(i.token0_mint_address, apc.token0_mint_address) as token0_mint_address
            , coalesce(i.token1_mint_address, apc.token1_mint_address) as token1_mint_address
            , CASE WHEN event_type = 'collectProtocolFee' then 'CLMM/CPMM'
                when event_type = 'withdrawPnl' then 'AMMv4'
            end as program
        from (
            SELECT block_timestamp
                , block_id
                , tx_id
                , event_type
                
                -- Using MAX and CASE WHEN to ensure one row per transaction and each field as VARCHAR
                , MAX(CASE WHEN account_name = 'poolState' or account_name = 'amm' THEN pubkey END) AS pool_address
                , MAX(CASE WHEN account_name = 'vault0Mint' THEN pubkey END) AS token0_mint_address
                , MAX(CASE WHEN account_name = 'vault1Mint' THEN pubkey END) AS token1_mint_address
            FROM (
                SELECT 
                    d.block_timestamp,
                    d.block_id,
                    d.tx_id,
                    d.event_type,
                    CAST(f.value:pubkey AS VARCHAR) AS pubkey, -- Cast to VARCHAR to remove quotes
                    f.value:name AS account_name
                FROM (
                    SELECT 
                        ed.block_timestamp,
                        ed.block_id,
                        ed.tx_id,
                        ed.event_type,
                        ed.decoded_instruction
                    FROM SOLANA_FLIPSIDE.CORE.EZ_EVENTS_DECODED ed 
                    WHERE 1=1
                        and (
            (ed.program_id = 'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK' and event_type = 'collectProtocolFee') -- CLMM
            or (ed.program_id = 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C' and event_type = 'collectProtocolFee') -- CPMM
            or (ed.program_id = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8' and event_type = 'withdrawPnl') -- AMMv4 
                        )
                        {% if is_incremental() %}
                            AND ed.block_timestamp::date >= (select dateadd('day', -2, max(date)) from {{ this }})
                        {% else %}
                            AND ed.block_timestamp::date >= date('2022-04-22') 
                        {% endif %}
                ) d,
                LATERAL FLATTEN(input => parse_json(d.decoded_instruction):accounts) f 
            
            ) 
            GROUP BY 1,2,3,4
        ) i 
        left join amm_pool_creation apc on apc.pool_address = i.pool_address
    
    ) b
    left join SOLANA_FLIPSIDE.PRICE.EZ_ASSET_METADATA m0 on m0.token_address = b.token0_mint_address
    left join SOLANA_FLIPSIDE.PRICE.EZ_ASSET_METADATA m1 on m1.token_address = b.token1_mint_address
)

, prices_data as (
    select date_trunc('day', hour) as day
        , TOKEN_ADDRESS
        , MEDIAN(price) as price
    from SOLANA_FLIPSIDE.PRICE.EZ_PRICES_HOURLY
    where blockchain = 'solana' 
        {% if is_incremental() %}
            AND hour::date >= (select dateadd('day', -2, max(date)) from {{ this }})
        {% else %}
            AND hour::date >= date('2022-04-22') 
        {% endif %}
    group by 1,2

)

select date_trunc('day', b.block_timestamp) as date 
    , b.token_pair
    , b.program
    , sum(t.amount * p.price) as buyback-- amount_usd
from buyback_event b 
left join SOLANA_FLIPSIDE.CORE.FACT_TRANSFERS t on t.block_id = b.block_id and t.tx_id = b.tx_id
left join SOLANA_FLIPSIDE.PRICE.EZ_ASSET_METADATA m on m.token_address = t.mint
left join prices_data p on p.token_address = t.mint
        and p.day = date_trunc('day', t.block_timestamp)
where date_trunc('day', b.block_timestamp) < to_date(sysdate())
group by 1,2,3
order by 1 desc 