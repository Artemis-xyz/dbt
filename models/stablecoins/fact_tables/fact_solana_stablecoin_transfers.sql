{{
    config(
        materialized="incremental",
        unique_key="fact_transfers_id",
        snowflake_warehouse="STABLECOIN_LG",
    )
}}

{% if not is_incremental() %}
    select
        tx_id,
        index,
        fact_transfers_id,
        block_timestamp,
        tx_from,
        tx_to,
        amount,
        mint as contract_address
    from solana_flipside.core.fact_transfers
    where
        to_date(block_timestamp) < to_date(sysdate())
        and mint
        in (select contract_address from {{ ref("fact_solana_stablecoin_contracts") }})
        and tx_from not in (
            '3emsAVdmGKERbHjmGfQ6oZ1e35dkf5iYcS6U4CPKFVaa',
            '7VHUFJHWu2CuExkJcJrzhQPJ2oygupTWkL2A2For4BmE'
        )
        and tx_from not in (
            select distinct tx_from
            from solana_flipside.core.fact_transfers
            where
                to_date(block_timestamp) >= '2021-10-07'
                and to_date(block_timestamp) <= '2022-11-15'
                and mint in (
                    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
                    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
                )
                and amount >= 3 * pow(10, 6)
        )
        and tx_to not in (
            '3emsAVdmGKERbHjmGfQ6oZ1e35dkf5iYcS6U4CPKFVaa',
            '7VHUFJHWu2CuExkJcJrzhQPJ2oygupTWkL2A2For4BmE'
        )
        and tx_to not in (
            select distinct tx_to
            from solana_flipside.core.fact_transfers
            where
                to_date(block_timestamp) >= '2021-10-07'
                and to_date(block_timestamp) <= '2022-11-15'
                and mint in (
                    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
                    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
                )
                and amount >= 3 * pow(10, 6)
        )
        and to_date(block_timestamp) >= '2021-10-07'
        and to_date(block_timestamp) <= '2022-11-15'
    union all
{% endif %}
select
    tx_id,
    index,
    fact_transfers_id,
    block_timestamp,
    tx_from,
    tx_to,
    amount,
    mint as contract_address
from solana_flipside.core.fact_transfers
where
    to_date(block_timestamp) < to_date(sysdate())
    and mint
    in (select contract_address from {{ ref("fact_solana_stablecoin_contracts") }})
    {% if is_incremental() %}
        and block_timestamp
        >= (select dateadd('day', -3, max(block_timestamp)) from {{ this }})
    {% else %} and to_date(block_timestamp) > '2022-11-15'
    {% endif %}
    and tx_from not in (
        '3emsAVdmGKERbHjmGfQ6oZ1e35dkf5iYcS6U4CPKFVaa',
        '7VHUFJHWu2CuExkJcJrzhQPJ2oygupTWkL2A2For4BmE'
    )
    and tx_to not in (
        '3emsAVdmGKERbHjmGfQ6oZ1e35dkf5iYcS6U4CPKFVaa',
        '7VHUFJHWu2CuExkJcJrzhQPJ2oygupTWkL2A2For4BmE'
    )
