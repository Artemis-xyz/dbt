{% macro get_bananagun_contracts(chain) %} 
WITH all_contracts AS (
    SELECT
        '{{ chain }}' AS source,
        TO_ADDRESS AS to_address,
        sysdate() as last_updated_timestamp
    FROM
        {{ chain }}_FLIPSIDE.CORE.FACT_TRACES
    WHERE
        TYPE = 'CREATE'
        AND tx_succeeded
        AND (
            lower(FROM_ADDRESS) = lower('0x37aAb97476bA8dC785476611006fD5dDA4eed66B') 
            {% if chain == 'ethereum' %}
            OR (
                BLOCK_NUMBER >= 17345515
                AND lower(FROM_ADDRESS) = lower('0xf414d478934c29d9a80244a3626c681a71e53bb2')
                AND lower(TO_ADDRESS) != lower('0x461EFe0100BE0682545972EBfC8B4a13253bD602') 
            )
            {% endif %}
        )
    {% if is_incremental() %}
        AND block_timestamp > (SELECT dateadd('day', -3, MAX(last_updated_timestamp)) FROM {{ this }})
    {% endif %}
)
SELECT
    source,
    to_address as contract_address,
    last_updated_timestamp
FROM
    all_contracts 
    
{% endmacro %}