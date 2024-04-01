{{ config(materialized="incremental", unique_key="address") }}

with
    contract_standard as (
        select distinct
            contract_address address,
            case
                when
                    lower(topics[0])
                    = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'
                then 'ERC_1155'
                when
                    (
                        lower(topics[0])
                        = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
                        and array_size(topics) = 4
                    )
                then 'NFT'
                else 'Token'
            end as token_standard
        from ethereum_flipside.core.fact_event_logs
        where
            lower(topics[0]) in (
                -- ERC20 and ERC721 Transfer Topic
                '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
                -- ERC1155 TransferSingle Topic       
                '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'
            )
            -- this filter will only be applied on an incremental run
            {% if is_incremental() %}
                and block_timestamp >= dateadd(day, -1, to_date(sysdate()))
            {% endif %}
    ),
    flipside_labels as (
        select
            coalesce(a.address, l.address, c.address, null) address,
            coalesce(l.address_name, c.name, null) name,
            coalesce(l.label, c.symbol, null) namespace,
            coalesce(a.token_standard, null) category
        from contract_standard a
        left join
            ethereum_flipside.core.dim_contracts c
            on lower(a.address) = lower(c.address)
        left join
            ethereum_flipside.core.dim_labels l on lower(a.address) = lower(l.address)
    )
select address, max(name) name, max(namespace) namespace, max(category) category
from flipside_labels
group by address
