{{ config(materialized="table") }}

{% set protocol_list = ['raydium', 'kamino', 'save', 'orca', 'drift', 'vaults_fyi', 'pendle', 'morpho', 'susdf'
] %}

{% for protocol in protocol_list %}
    select
        timestamp,
        id,
        name,
        apy,
        tvl,
        symbol,
        protocol,
        type,
        chain,
        link,
        tvl_score,
        daily_avg_apy_l7d
    from {{ ref("fact_" ~ protocol ~ "_stablecoin_apy") }}
    {% if not loop.last %}union all{% endif %}
{% endfor %}
