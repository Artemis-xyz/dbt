-- Assumes the following:
-- 1. The table `fact_{{ chain }}_stablecoin_contracts` exists
-- 2. The table `{{ chain }}_flipside.core.ez_decoded_event_logs` exists and the
-- contracts in `fact_{{ chain }}_stablecoin_contracts` are decoded
-- 3. The table `{{ chain }}_flipside.core.fact_transactions` exists
{% macro agg_chain_stablecoin_transfers(chain, new_stablecoin_address) %}
    {% if chain in ("ripple") %}
        select
            block_timestamp,
            block_timestamp::date as date,
            block_number,
            event_index as index,
            transaction_hash as tx_hash,
            from_address,
            to_address,
            -- Mint and burn address work differently on XRP
            from_address = t1.contract_address AND to_address != t1.contract_address AS is_mint,
            to_address = t1.contract_address AND from_address != t1.contract_address AS is_burn,
            coalesce(amount_raw / pow(10, num_decimals), 0) as amount,
            case
                when is_mint then amount_raw / pow(10, num_decimals)
                when is_burn then -1 * amount_raw / pow(10, num_decimals)
                else 0
            end as inflow,
            case
                when not is_mint and not is_burn then coalesce(amount_raw / pow(10, num_decimals), 0)
            end as transfer_volume,
            t1.contract_address,
            t2.symbol
        from {{ ref("fact_ripple_token_transfers") }} t1
        inner join {{ ref("fact_ripple_stablecoin_contracts") }} t2
            on lower(t1.contract_address) = lower(t2.contract_address)
        where
            lower(t1.contract_address) in (
                select lower(contract_address)
                from {{ ref("fact_ripple_stablecoin_contracts") }}
            )
    {% elif chain in ("stellar") %}
        select
            block_timestamp,
            block_timestamp::date as date,
            block_number,
            event_index as index,
            transaction_hash as tx_hash,
            from_address,
            to_address,
            lower(event_type) = 'mint' AS is_mint,
            lower(event_type) = 'burn' AS is_burn,
            coalesce(amount_raw / pow(10, num_decimals), 0) as amount,
            case
                when is_mint then amount_raw / pow(10, num_decimals)
                when is_burn then -1 * amount_raw / pow(10, num_decimals)
                else 0
            end as inflow,
            case
                when not is_mint and not is_burn then coalesce(amount_raw / pow(10, num_decimals), 0)
            end as transfer_volume,
            t1.contract_address,
            t2.symbol
        from {{ ref("fact_stellar_token_transfers") }} t1
        inner join {{ ref("fact_stellar_stablecoin_contracts") }} t2
            on lower(t1.contract_address) = lower(t2.contract_address)
        where
            lower(t1.contract_address) in (
                select lower(contract_address)
                from {{ ref("fact_stellar_stablecoin_contracts") }}
            )
    {% elif chain in ("tron") %}
        select
            block_timestamp,
            block_timestamp::date as date,
            block_number,
            event_index as index,
            transaction_hash as tx_hash,
            from_address,
            to_address,
            -- NULL address on TRON is different
            from_address = 'T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb' 
            or lower(from_address) in (
                select distinct (lower(premint_address))
                from {{ ref("fact_"~chain~"_stablecoin_premint_addresses") }}
            ) as is_mint,
            to_address = 'T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb' 
            or lower(to_address) in (
                select distinct (lower(premint_address))
                from {{ ref("fact_"~chain~"_stablecoin_premint_addresses") }}
            )
            as is_burn,
            lower(to_address) in (
                select distinct (lower(premint_address))
                from {{ ref("fact_tron_stablecoin_bridge_addresses") }}
            ) as is_bridge_burn,
            lower(from_address) in (
                select distinct (lower(premint_address))
                from {{ ref("fact_tron_stablecoin_bridge_addresses") }}
            ) as is_bridge_mint,
            coalesce(amount_raw/ pow(10, num_decimals), 0) as amount,
            case
                when is_mint or is_bridge_mint then coalesce(amount_raw/ pow(10, num_decimals), 0) 
                when is_burn or is_bridge_burn then -1 * coalesce(amount_raw/ pow(10, num_decimals), 0) 
                else 0
            end as inflow,
            case
                when not is_mint and not is_burn then coalesce(amount_raw/ pow(10, num_decimals), 0) 
            end as transfer_volume,
            stablecoin_transfers.contract_address,
            contracts.symbol
        from {{ ref("fact_tron_token_transfers") }} stablecoin_transfers
        inner join {{ ref("fact_tron_stablecoin_contracts") }} contracts
            on lower(stablecoin_transfers.contract_address) = lower(contracts.contract_address)
        where
            lower(stablecoin_transfers.contract_address) in (
                select lower(contract_address)
                from {{ ref("fact_tron_stablecoin_contracts") }}
            )
    -- TODO: Refactor to support native currencies. Currently assumes everything is $1
    -- b/c of perf issues when joining
    {% elif chain in ("ton") %}
        select
            block_timestamp
            , trunc(block_timestamp, 'day') as date
            , null as block_number
            , event_index as index
            , trace_id
            , tx_hash
            , from_address
            , to_address
            , type = 'mint' or lower(from_address) in (
                select distinct (lower(premint_address))
                from {{ ref("fact_ton_stablecoin_premint_addresses") }}
            ) as is_mint
            , type = 'burn' or lower(to_address) in (
                select distinct (lower(premint_address))
                from {{ ref("fact_ton_stablecoin_premint_addresses") }}
            ) as is_burn
            , coalesce(amount / POWER(10, num_decimals), 0) as amount
            , case
                when is_mint then amount / POWER(10, num_decimals) when is_burn then -1 * amount / POWER(10, num_decimals) else 0
            end as inflow
            , case
                when
                    not is_mint
                    and not is_burn
                then amount / POWER(10, num_decimals)
                else 0
            end as transfer_volume
            , contracts.symbol
            , transfers.contract_address
        from {{ ref('fact_ton_token_transfers') }} transfers
        left join {{ ref('fact_ton_stablecoin_contracts') }} contracts
            on lower(transfers.contract_address) = lower(contracts.contract_address)
        where lower(transfers.contract_address) in (
                select lower(contract_address)
                from {{ ref('fact_ton_stablecoin_contracts') }} t1
            )
            and tx_status = 'TRUE'
    {% elif chain in ("solana") %}
    -- CASE 1: Mints into non-premint addresses
        select
            block_timestamp,
            trunc(block_timestamp, 'day') as date,
            tx_id as tx_hash,
            block_id as block_number,
            FACT_TOKEN_MINT_ACTIONS_ID as index,
            '1nc1nerator11111111111111111111111111111111' as from_address,
            token_account as to_address,
            TRUE as is_mint,
            FALSE as is_burn,
            coalesce((mint_amount/ POW(10, decimal)), 0) as amount,
            case
                when is_mint then amount when is_burn then -1 * amount else 0
            end as inflow,
            0 as transfer_volume,
            mint as contract_address,
            fact_solana_stablecoin_contracts.symbol
        from solana_flipside.defi.fact_token_mint_actions
        join
            fact_solana_stablecoin_contracts
            on lower(solana_flipside.defi.fact_token_mint_actions.mint)
            = lower(fact_solana_stablecoin_contracts.contract_address)
        where
            mint
            in (select distinct contract_address from fact_solana_stablecoin_contracts)
            and lower(token_account) not in (
                select distinct (lower(premint_address))
                    from fact_solana_stablecoin_premint_addresses
            )
            and succeeded = 'TRUE'
        UNION 
    -- CASE 2: Burns into non-premint addresses
        select
            block_timestamp,
            trunc(block_timestamp, 'day') as date,
            tx_id as tx_hash,
            block_id as block_number,
            FACT_TOKEN_BURN_ACTIONS_ID as index,
            token_account as from_address,
            '1nc1nerator11111111111111111111111111111111' as to_address,
            FALSE as is_mint,
            TRUE as is_burn,
            coalesce((burn_amount/ POW(10, decimal)), 0) as amount,
            case
                when is_mint then amount when is_burn then -1 * amount else 0
            end as inflow,
            0 as transfer_volume,
            mint as contract_address,
            fact_solana_stablecoin_contracts.symbol
        from solana_flipside.defi.fact_token_burn_actions
        join
            fact_solana_stablecoin_contracts
            on lower(solana_flipside.defi.fact_token_burn_actions.mint)
            = lower(fact_solana_stablecoin_contracts.contract_address)
        where
            mint
            in (select distinct contract_address from fact_solana_stablecoin_contracts)
            and lower(token_account) not in (
                select distinct (lower(premint_address))
                    from fact_solana_stablecoin_premint_addresses
            )
            and succeeded = 'TRUE'
        UNION
    -- CASE 3: Transfers between pre-mint and non-premint addresses (quasi-mint/burns)
        select
            block_timestamp,
            trunc(block_timestamp, 'day') as date,
            tx_id as tx_hash,
            block_id as block_number,
            index,
            tx_from as from_address,
            tx_to as to_address,
            -- OUTSIDE OF EMPIRICAL MINT / BURNS 
            -- Mint: From: Premint, To: Contract
            tx_from in (
                select distinct (premint_address)
                    from fact_solana_stablecoin_premint_addresses
                ) 
                and tx_to not in (
                select distinct (premint_address)
                    from fact_solana_stablecoin_premint_addresses
                )
            as is_mint,
            -- BURN: From: Contract, To: Premint
            tx_from not in (
                select distinct (premint_address)
                    from fact_solana_stablecoin_premint_addresses
                ) 
                and tx_to in (
                select distinct (premint_address)
                    from fact_solana_stablecoin_premint_addresses
                )
            as is_burn,
            coalesce(amount, 0) as amount,
            case
                when is_mint then amount when is_burn then -1 * amount else 0
            end as inflow,
            case
                -- Prior to 2023, volumes data not high fidelity enough to report. 
                -- Continuing to do analysis on this data. 
                when
                    not is_mint
                    and not is_burn
                    and trunc(block_timestamp, 'day') > '2022-12-31'
                then amount
                else 0
            end as transfer_volume,
            mint as contract_address,
            t1.symbol
        from solana_flipside.core.fact_transfers
        join
            fact_{{ chain }}_stablecoin_contracts t1
            on lower(solana_flipside.core.fact_transfers.mint)
            = lower(t1.contract_address)
        where
            mint
            in (select distinct contract_address from fact_solana_stablecoin_contracts)
    {% elif chain in ("near") %}
        select
            block_timestamp,
            trunc(block_timestamp, 'day') as date,
            block_id as block_number,
            tx_hash,
            ez_token_transfers_id as index,
            from_address,
            to_address,
            from_address = 'system' or from_address is null as is_mint,
            to_address = 'system' or to_address is null as is_burn,
            amount_raw_precise / pow(10, num_decimals) as amount,
            case
                when is_mint then 1 * amount
                when is_burn then -1 * amount
                else 0
            end as inflow,
            case
                when not is_burn and not is_burn then amount else 0
            end as transfer_volume,
            t1.contract_address,
            fact_{{ chain }}_stablecoin_contracts.symbol
        from near_flipside.core.ez_token_transfers t1
        join
            fact_{{ chain }}_stablecoin_contracts
            on lower(t1.contract_address)
            = lower(fact_{{ chain }}_stablecoin_contracts.contract_address)
        where
            lower(t1.contract_address) in (
                select lower(contract_address)
                from fact_{{ chain }}_stablecoin_contracts
            )
            and transfer_type = 'nep141'

    {% elif chain in ("celo", "kaia", "aptos", 'sei') %}
        select
            block_timestamp,
            block_timestamp::date as date,
            block_number,
            event_index as index,
            transaction_hash as tx_hash,
            from_address,
            to_address,
            from_address = '0x0000000000000000000000000000000000000000'
                or lower(from_address) in (
                    select distinct (lower(premint_address))
                    from {{ ref("fact_"~chain~"_stablecoin_premint_addresses") }}
                )
            as is_mint,
            to_address = '0x0000000000000000000000000000000000000000'
                or lower(to_address) in (
                    select distinct (lower(premint_address))
                    from {{ ref("fact_"~chain~"_stablecoin_premint_addresses") }}
                )
            as is_burn,
            amount_raw / pow(10, num_decimals) as amount,
            case
                when is_mint then amount_raw / pow(10, num_decimals) when is_burn then -1 * amount_raw / pow(10, num_decimals) else 0
            end as inflow,
            case
                when not is_mint and not is_burn then amount_raw / pow(10, num_decimals) else 0
            end as transfer_volume,
            t1.contract_address,
            contracts.symbol
        from {{ref("fact_" ~ chain ~ "_token_transfers")}} t1 
        join {{ref("fact_" ~chain~ "_stablecoin_contracts")}} contracts
            on lower(t1.contract_address) = lower(contracts.contract_address)
        where lower(t1.contract_address) in (
                select lower(contract_address)
                from {{ref("fact_" ~chain~ "_stablecoin_contracts")}}
            )
    {% elif chain in ("mantle", 'sonic') %}
        select
            block_timestamp
            , block_timestamp::date as date
            , block_number
            , event_index as index
            , transaction_hash as tx_hash
            , from_address
            , to_address
            , from_address = '0x0000000000000000000000000000000000000000' as is_mint
            , to_address = '0x0000000000000000000000000000000000000000' as is_burn
            , amount_raw / pow(10, num_decimals) as amount
            , case
                when is_mint then amount_raw / pow(10, num_decimals) when is_burn then -1 * amount_raw / pow(10, num_decimals) else 0
            end as inflow
            , case
                when not is_mint and not is_burn then amount_raw / pow(10, num_decimals) else 0
            end as transfer_volume
            , t1.contract_address
            , contracts.symbol
        from {{ref("fact_" ~ chain ~ "_token_transfers")}} t1 
        inner join {{ref("fact_" ~chain~ "_stablecoin_contracts")}} contracts
            on lower(t1.contract_address) = lower(contracts.contract_address)
        where
            1=1
    {% else %}
        select
            block_timestamp,
            trunc(block_timestamp, 'day') as date,
            block_number,
            event_index as index,
            tx_hash,
            -- Notably, we do NOT use the Mint / Burn events here because the
            -- basic IERC20 interface does not require them to be implemented
            coalesce(
                decoded_log:from,
                decoded_log:_from,
                -- DAI on ETH Mainnet does not follow the IERC20 interface
                decoded_log:src,
                decoded_log:sender
            ) as from_address,
            coalesce(
                decoded_log:to,
                decoded_log:_to,
                decoded_log:dst,
                decoded_log:receiver
            ) as to_address,
            from_address = '0x0000000000000000000000000000000000000000'
                or event_name = 'Issue' 
                {% if chain in ("ethereum") %}
                or lower(from_address) in (
                    select distinct (lower(premint_address))
                    from {{ ref("fact_"~chain~"_stablecoin_premint_addresses") }}
                )
                {% endif %}
            as is_mint,
            to_address = '0x0000000000000000000000000000000000000000'
                or event_name = 'Redeem' 
                {% if chain in ("ethereum") %}
                or lower(to_address) in (
                    select distinct (lower(premint_address))
                    from {{ ref("fact_"~chain~"_stablecoin_premint_addresses") }}
                )
                {% endif %}
            as is_burn,
            {% if chain in ("ethereum") %}
                lower(from_address) in (
                    select distinct (lower(premint_address))
                    from {{ ref("fact_ethereum_stablecoin_bridge_addresses") }}
                ) as is_bridge_mint,
                lower(to_address) in (
                    select distinct (lower(premint_address))
                    from {{ ref("fact_ethereum_stablecoin_bridge_addresses") }}
                ) as is_bridge_burn,
            {% endif %}
            coalesce(
                decoded_log:value::float / pow(10, num_decimals),
                decoded_log:_value::float / pow(10, num_decimals),
                decoded_log:wad::float / pow(10, num_decimals),
                -- USDT on ETH does not follow the IERC20 interface
                decoded_log:amount::float / pow(10, num_decimals),
                decoded_log:_amount::float / pow(10, num_decimals),
                0
            ) as amount,
            case
                {% if chain in ("ethereum") %}
                    when is_mint or is_bridge_mint then amount when is_burn or is_bridge_burn then -1 * amount else 0
                {% else %}
                    when is_mint then amount when is_burn then -1 * amount else 0
                {% endif %}
            end as inflow,
            case
                when not is_mint and not is_burn then amount else 0
            end as transfer_volume,
            t1.contract_address,
            fact_{{ chain }}_stablecoin_contracts.symbol
        from {{ chain }}_flipside.core.ez_decoded_event_logs t1
        join
            fact_{{ chain }}_stablecoin_contracts
            on lower(t1.contract_address)
            = lower(fact_{{ chain }}_stablecoin_contracts.contract_address)
        where
            lower(t1.contract_address) in (
                select lower(contract_address)
                from fact_{{ chain }}_stablecoin_contracts
            )
            -- DO NOT include mint / burn events here - they will be duped
            and event_name in ('Transfer', 'Issue', 'Redeem')
            and tx_succeeded = TRUE
    {% endif %}
    {% if is_incremental() and new_stablecoin_address == '' %} 
        and block_timestamp >= (
            select dateadd('day', -3, max(block_timestamp))
            from {{ this }}
        )
    {% endif %}
    {% if new_stablecoin_address != '' %}
        and lower(t1.contract_address) = lower('{{ new_stablecoin_address }}')
    {% endif %}

{% endmacro %}
