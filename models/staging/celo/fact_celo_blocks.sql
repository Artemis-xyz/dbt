{{
    config(
        materialized="incremental",
        unique_key="block_hash",
    )
}}
{{
    unpack_blocks_json_quicknode_streams(
        "celo",
        block_column_map=[
            ("timestamp", hex_to_timestamp, "block_timestamp"),
            ("hash", to_string, "block_hash"),
            ("miner", to_address, "miner"),
            ("gasUsed", hex_to_number, "gas_used"),
            ("gasLimit", hex_to_number, "gas_limit"),
            ("baseFeePerGas", hex_to_number, "base_fee_per_gas"),
            ("size", hex_to_number, "size"),
            ("parentHash", to_string, "parent_hash"),
            ("receiptsRoot", to_string, "receipts_root"),
            ("stateRoot", to_string, "state_root"),
            ("transactionsRoot", to_string, "transactions_root"),
            ("randomness", to_string, "randomness"),
            ("difficulty", to_string, "difficulty"),
            ("totalDifficulty", to_string, "total_difficulty"),
            ("transactions", to_json, "transactions"),
            ("extraData", to_string, "extra_data"),
            ("epochSnarkData", to_string, "epoch_snark_data"),
        ],
    )
}}
