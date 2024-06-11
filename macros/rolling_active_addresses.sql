{% macro rolling_active_addresses(chain) %}
    with
    {% if chain == 'solana' %}
            distinct_dates as (
                select distinct 
                    raw_date
                from {{ ref("ez_" ~ chain ~ "_transactions") }}
                where succeeded = 'TRUE'
                {% if is_incremental() %}
                    and raw_date > (select dateadd('day', -1, max(date)) from {{ this }})
                {% endif %}
            ),
            distinct_dates_for_rolling_active_address as (
                select distinct 
                    raw_date,
                    value as from_address 
                from {{ ref("ez_" ~ chain ~ "_transactions") }}, lateral flatten(input => signers)
                where succeeded = 'TRUE'
            ),
    {% elif chain == 'sui' %}
        distinct_dates as (
            select distinct
                raw_date
            from {{ ref("ez_" ~ chain ~ "_transactions") }}
            {% if is_incremental() %}
                where raw_date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
        ),
        distinct_dates_for_rolling_active_address as (
            select distinct
                raw_date,
                sender as from_address
            from {{ ref("ez_" ~ chain ~ "_transactions") }}
        ),
    {% elif chain == 'zksync' %}
        distinct_dates as (
            select distinct
                block_date as raw_date
            from zksync_dune.zksync.transactions
            {% if is_incremental() %}
                where raw_date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
        ),
        distinct_dates_for_rolling_active_address as (
            select distinct
                block_date as raw_date,
                "FROM" as from_address
            from zksync_dune.zksync.transactions
        ),
    {% elif chain == 'acala' %}
        distinct_dates as (
            select distinct
                date as raw_date
            from {{ ref("fact_acala_uniq_daily_signers") }}
            {% if is_incremental() %}
                where raw_date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
        ),
        distinct_dates_for_rolling_active_address as (
            select distinct
                date as raw_date,
                signer as from_address
            from {{ ref("fact_acala_uniq_daily_signers") }}
        ),
    {% elif chain == 'fantom' %}
        distinct_dates as (
            select distinct
                date as raw_date
            from {{ ref("fact_fantom_uniq_daily_addresses") }}
            {% if is_incremental() %}
                where raw_date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
        ),
        distinct_dates_for_rolling_active_address as (
            select distinct
                date as raw_date,
                from_address as from_address
            from {{ ref("fact_fantom_uniq_daily_addresses") }}
        ),
    {% elif chain == 'polkadot' %}
        distinct_dates as (
            select distinct
                date as raw_date
            from {{ ref("fact_polkadot_uniq_daily_signers") }}
            {% if is_incremental() %}
                where raw_date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
        ),
        distinct_dates_for_rolling_active_address as (
            select distinct
                date as raw_date,
                signer_pub_key as from_address
            from {{ ref("fact_polkadot_uniq_daily_signers") }}
        ),
    {% elif chain == 'stride' %}
        distinct_dates as (
            select distinct
                date as raw_date
            from {{ ref("fact_stride_uniq_daily_senders") }}
            {% if is_incremental() %}
                where raw_date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
        ),
        distinct_dates_for_rolling_active_address as (
            select distinct
                date as raw_date,
                sender as from_address
            from {{ ref("fact_stride_uniq_daily_senders") }}
        ),
    {% else %}
        distinct_dates as (
            select distinct 
                raw_date
            from {{ ref("ez_" ~ chain ~ "_transactions") }}
            {% if is_incremental() %}
                where raw_date > (select dateadd('day', -1, max(date)) from {{ this }})
            {% endif %}
        ),
        distinct_dates_for_rolling_active_address as (
            select distinct 
                raw_date,
                from_address
            from {{ ref("ez_" ~ chain ~ "_transactions") }}
        ),
    {% endif %}


    rolling_mau as (
        select 
            t1.raw_date,
            count(distinct t2.from_address) as mau
        from distinct_dates t1
        join distinct_dates_for_rolling_active_address t2 on t2.raw_date between dateadd(DAY, -29, t1.raw_date) and t1.raw_date
        group by t1.raw_date
    ),
    rolling_wau as (
        select 
            t1.raw_date,
            count(distinct t2.from_address) as wau
        from distinct_dates t1
        join distinct_dates_for_rolling_active_address t2 on t2.raw_date between dateadd(DAY, -6, t1.raw_date) and t1.raw_date
        group by t1.raw_date
    )
select 
    rolling_mau.raw_date as date,
    '{{ chain }}' as chain,
    mau,
    wau
from rolling_mau
left join rolling_wau using(raw_date)
where rolling_mau.raw_date < to_date(sysdate())
order by date
{% endmacro %}