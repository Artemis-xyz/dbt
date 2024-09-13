{% macro get_p2p_metrics(chain) %}
    select 
        date
        , p2p_native_transfer_volume
        , p2p_token_transfer_volume
        , p2p_stablecoin_transfer_volume
        , p2p_transfer_volume
    from {{ ref("fact_" ~ chain ~ "_p2p_transfer_volume") }}
{% endmacro %}
