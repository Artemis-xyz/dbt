{{
    config(
        materialized="incremental",
        unique_key=["tx_hash", "index"],
        snowflake_warehouse="SOLANA",
        database="solana",
        schema="raw",
        alias="ez_token_transfers",
    )
}}

{{ 
    get_token_transfer_filtered(
        'solana'
        , limit_number=200
        , blacklist=(
            "o1Mw5Y3n68o8TakZFuGKLZMGjm72qv4JeoZvGiCLEvK", 
            "SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt",
            "GiBrdw1tF8nuJxWuhTp83ULEMY9uJkYUHQUBzwfEnw5R",
            "B584xmGMbrJz2FewT6fxNx64vQ74zrbQijHHijszSa7K",
            "NLBLjgKvSVtiporRdVEcyssTyr5oPRmjeajVJ4EGW9B"
        )
        , include_full_history="TRUE"
    ) 
}}