{{
    config(
        materialized="table",
        
    )
}}

{{ filter_p2p_token_transfers(
    "solana", 
    200,
    blacklist=(
        "o1Mw5Y3n68o8TakZFuGKLZMGjm72qv4JeoZvGiCLEvK", 
        "SRMuApVNdxXokk5GT7XD5cUUgXMBCoAz2LHeuAoKWRt",
        "GiBrdw1tF8nuJxWuhTp83ULEMY9uJkYUHQUBzwfEnw5R",
        "B584xmGMbrJz2FewT6fxNx64vQ74zrbQijHHijszSa7K",
        "NLBLjgKvSVtiporRdVEcyssTyr5oPRmjeajVJ4EGW9B"
    )
)}}