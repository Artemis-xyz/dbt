{{
    config(
        materialized="table",
        snowflake_warehouse="OPTIMISM",
        database="optimism",
        schema="raw",
        alias="fact_optimism_base_grants_emissions",
    )
}}

SELECT
    block_timestamp::date as date,
    'Base Grant' as event_type,
    sum(amount_precise) as amount,
    'https://optimism.mirror.xyz/Luegue9qIbTO_NZlNVOsj25O1k4NBNKkNadp2d0MsTI' as source_url
FROM
    optimism_flipside.core.ez_token_transfers
WHERE 1=1 
    AND contract_address = lower('0x4200000000000000000000000000000000000042') AND amount_precise = 4473924
    AND from_address in (
        lower('0x19793c7824Be70ec58BB673CA42D2779d12581BE')
        , lower('0x2501c477d0a35545a387aa4a3eee4292a9a8b3f0')
        )
GROUP BY
    1