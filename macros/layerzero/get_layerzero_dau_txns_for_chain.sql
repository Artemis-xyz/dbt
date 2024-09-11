{% macro get_layerzero_dau_txns_for_chain(chain) %}
    select
        date(block_timestamp) as date
        , count(distinct origin_from_address) as DAU
        , count(*) as txns
        , '{{chain}}' as chain
    from
        {{chain}}_flipside.core.fact_event_logs
    where
        topics [0] in (
            '0xe8d23d927749ec8e512eb885679c2977d57068839d8cca1a85685dbbea0648f6',
            '0xe9bded5f24a4168e4f3bf44e00298c993b22376aad8c58c7dda9718a54cbea82'
        )
    group by 1 
{% endmacro %}