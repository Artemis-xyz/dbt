-- depends_on: {{ ref('fact_celo_transactions') }}
{{
    config(
        materialized="incremental",
        unique_key=["transaction_hash", "event_index"],
        snowflake_warehouse="CELO",
    )
}}
{{
    unpack_logs_json(
        "celo",
        log_column_map=[
            ("transactionHash", to_string, "transaction_hash"),
            ("logIndex", hex_to_number, "event_index"),
            ("address", to_address, "contract_address"),
            ("topics", to_json, "topics"),
            ("data", to_string, "data"),
            ("removed", to_string, "removed"),
        ],
    )
}}
