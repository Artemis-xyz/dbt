{% macro fact_protocol_daa_txns_gas_gas_usd(
    chain, app, category, dim_protocol_addresses
) %}
    select
        date,
        '{{ chain }}' as chain,
        '{{ app }}' as app,
        '{{ category }}' as category,
        count(distinct tx_hash) as txns,
        count(distinct from_address) as daa,
        sum(tx_fee) as gas,
        sum(gas_usd) as gas_usd
    from
        (
            select distinct
                trunc(t1.block_timestamp, 'day') as date,
                t1.tx_hash,
                t1.from_address,
                t1.tx_fee,
                coalesce(t4.price, 0) as price,
                t1.tx_fee * price as gas_usd
            from {{ chain }}_flipside.core.fact_transactions t1
            inner join
                (select address from {{ dim_protocol_addresses }}) t2
                on lower(t1.to_address) = lower(t2.address)
            left join
                (
                    {% if chain == "bsc" %}
                        {{ get_coingecko_price_with_latest("binancecoin") }}
                    {% else %}{{ get_coingecko_price_with_latest("ethereum") }}
                    {% endif %}
                ) t4
                on trunc(t1.block_timestamp, 'day') = t4.date
            where
                t1.block_timestamp < to_date(sysdate())
                {% if is_incremental() %}
                    and t1.block_timestamp
                    >= (select max(date) + interval '1 DAY' from {{ this }})
                {% endif %}
        )
    group by date
    order by date desc
{% endmacro %}
