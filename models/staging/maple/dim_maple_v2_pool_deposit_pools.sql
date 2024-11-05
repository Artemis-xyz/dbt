{{
    config(
        materialized = 'incremental',
        unique_key = ['pool_address'],
        snowflake_warehouse = 'MAPLE',
    )
}}

SELECT
    sysdate() as last_updated,
    distinct({{ hex_string_to_evm_address("SUBSTR(data, 27, 40)::string") }}) as pool_address
FROM
{{source('ETHEREUM_FLIPSIDE', 'fact_event_logs')}}
where topics[0] = lower('0xf55841bdafd5af17a3183b609d4042325203ab6eb4747e435c6a044b6eb27b05')
{% if is_incremental() %}
    and block_timestamp > (select max(last_updated) from {{this}})
{% endif %}

UNION ALL

SELECT
    sysdate() as last_updated,
    distinct({{ hex_string_to_evm_address("SUBSTR(data, 27, 40)::string") }}) as pool_address
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
{% if is_incremental() %}
    and block_timestamp > (select max(last_updated) from {{this}})
{% endif %}