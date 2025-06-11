{{
    config(
        materialized="table",
        table_format="iceberg",
        database="ARTEMIS_ICEBERG",
        schema="LABELS",
        external_volume="ICEBERG_EXTERNAL_VOLUME_INTERNAL",
        alias="all_chains_gas_dau_txns_by_contract_v2",
        partition_by=["chain", "date", "namespace"],
        post_hook = "{{ merge_tags_dict({
            'duckdb': 'true',
            'order_by': 'date, namespace, contract_address',
            'partitioned_order_by': 'chain',
            'partitioned_order_by_breakdown': 'discrete'
        }) }}"
    )
}}

SELECT
    * EXCLUDE (date),
    date::DATE AS date
FROM PC_DBT_DB.PROD.ALL_CHAINS_GAS_DAU_TXNS_BY_CONTRACT_V2
ORDER BY chain, date, namespace, contract_address