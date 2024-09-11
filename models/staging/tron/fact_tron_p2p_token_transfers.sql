{{
    config(
        materialized="table",
    )
}}

{{ filter_p2p_token_transfers("tron", blacklist=('TAFjULxiVgT4qWk6UZwjqwZXTSaGaqnVp4', 'TSSMHYeV2uE9qYH95DqyoCuNCzEL1NvU3S', 'TR95WcjR78Y4puyv8BkbFuxaf4WcvmmPpd')) }}