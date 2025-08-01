{{
    config(
        materialized="table",
        snowflake_warehouse="RENDER",
    )
}}

with daily_net_change as (
    SELECT 
        block_timestamp::date as date,
        sum(case when from_address = '0x0000000000000000000000000000000000000000'
            then amount
            when to_address in (
                '0x0000000000000000000000000000000000000000'
                , lower('0x3ee18B2214AFF97000D974cf647E7C347E8fa585') -- Wormhole Bridge
                , lower('0x40ec5B33f54e0E8A33A975908C5BA1c14e5BbbDf') -- Polygon ERC20 Bridge
            )
            then -1 * amount
        end )as net_change
    FROM {{ source("ETHEREUM_FLIPSIDE", "ez_token_transfers") }}
    WHERE TRUE
    AND lower(contract_address) in(
        lower('0x6De037ef9aD2725EB40118Bb1702EBb27e4Aeb24'),
        lower('0xa5DBABb3171Eccd5B06f4F13FfBd2763Fba920EB')
        )
    AND block_timestamp is not null
    GROUP BY 1
)
, date_spine as (
    SELECT date FROM dim_date_spine
    WHERE date between (SELECT MIN(date) FROM daily_net_change) and to_date(sysdate())
)
SELECT
    ds.date,
    'ethereum' as chain,
    net_change,
    SUM(net_change) OVER (ORDER BY ds.date ASC) as supply_native
FROM date_spine ds
LEFT JOIN daily_net_change USING(date)
