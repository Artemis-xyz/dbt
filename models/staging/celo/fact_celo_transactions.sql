-- depends_on: {{ ref('fact_celo_blocks') }}
{{
    config(
        materialized="incremental",
        unique_key="transaction_hash",
        snowflake_warehouse="CELO",
    )
}}
{{
    unpack_transactions_json(
        "celo",
        transaction_column_map=[
            ("blockNumber", hex_to_number, "block_number"),
            ("hash", to_string, "transaction_hash"),
            ("from", to_address, "from_address"),
            ("to", to_address, "to_address"),
            ("nonce", hex_to_number, "nonce"),
            ("value", hex_to_number, "msg_value"),
            ("input", to_string, "input_data"),
            ("transactionIndex", hex_to_number, "transaction_index"),
            ("feeCurrency", to_string, "fee_currency"),
            ("gas", hex_to_number, "gas"),
            ("gasPrice", hex_to_number, "gas_price"),
            ("gatewayFee", hex_to_number, "gateway_fee"),
            ("gatewayFeeRecipient", to_string, "gateway_fee_recipient"),
            ("v", to_string, "v"),
            ("r", to_string, "r"),
            ("s", to_string, "s"),
            ("ethCompatible", to_string, "eth_compatible"),
            ("type", hex_to_number, "type"),
        ],
        receipts_column_map=[
            ("transactionHash", to_string, "transaction_hash"),
            ("transactionIndex", hex_to_number, "transaction_index"),
            ("contractAddress", to_address, "contract_address"),
            ("gasUsed", hex_to_number, "gas_used"),
            ("cumulativeGasUsed", hex_to_number, "cumulative_gas_used"),
            ("effectiveGasPrice", hex_to_number, "effective_gas_price"),
            ("status", hex_to_number, "status"),
            ("logsBloom", to_string, "logs_bloom"),
            ("logs", to_string, "logs"),
        ],
    )
}}
