{{
    config(
        materialized="table",
        snowflake_warehouse="SOLANA",
    )
}}

{{ filter_p2p_token_transfers(
    "solana", 
    2000, 
    blacklist=(
        "o1Mw5Y3n68o8TakZFuGKLZMGjm72qv4JeoZvGiCLEvK", 
        "SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt",
        "GiBrdw1tF8nuJxWuhTp83ULEMY9uJkYUHQUBzwfEnw5R"
    )
)}}