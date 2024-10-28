{{
    config(
        materialized="incremental",
        snowflake_warehouse="MAPLE",
    )
}}

-- Works but off by 8 txs

with pools as (
    SELECT
        distinct('0x' || SUBSTR(topics[1], 24+3, 40)::string) as pool_address
    FROM
    {{source('ETHEREUM_FLIPSIDE', 'fact_event_logs')}}
    where topics[0] = lower('0xf55841bdafd5af17a3183b609d4042325203ab6eb4747e435c6a044b6eb27b05')

    UNION ALL

    SELECT
        distinct('0x' || SUBSTR(data, 24+3, 40)::string) as pool_address
    FROM
        {{source('ETHEREUM_FLIPSIDE', 'ez_decoded_event_logs')}}
    WHERE topics[0] = '0x0f91882b50d9330af0b1d4998e6af7f2eaee90ce7e77ea54fea089af166d021d'
    AND contract_address in (
        lower('0x1Bb73D6384ae73DA2101a4556a42eaB82803Ef3d')
        , lower('0x2c630CC5F1988e840C5D7F3bD5a43844CcdCf363')
        , lower('0x1146691782c089bCF0B19aCb8620943a35eebD12')
        , lower('0x8228719eA6dCc79b77d663F13af98684a637d3A0')
        , lower('0x7F0d63e2250bC99f48985B183AF0c9a66BbC8ac3')
    )
)
SELECT 
    block_timestamp
    , tx_hash
    , block_number as block
    , contract_address
    , decoded_log:assets_::number as assets_
    , decoded_log:caller_::string as caller_
    , decoded_log:owner_::string as owner_
    , decoded_log:shares_::number as shares_
FROM
    {{source('ETHEREUM_FLIPSIDE', 'ez_decoded_event_logs')}} l
join pools p on lower(p.pool_address) = lower(l.contract_address)
where event_name = 'Deposit'
{% if is_incremental() %}
    AND block_timestamp > (select dateadd('day', -1, max(block_timestamp)) from {{ this }})
{% endif %}