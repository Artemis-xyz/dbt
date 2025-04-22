    {{
        config(
            materialized="table",
            snowflake_warehouse="ORCA",
        )
    }}

SELECT
    *
FROM values
    ('EXfMuea3TGiKJCvbhySsX6UPMue6wA6NTPDPBWBUyujy', NULL, 'orca', 'solana', NULL, NULL, 'treasury', SYSDATE()::TIMESTAMP_NTZ),
    ('3qPbC7P9baPCXxz2Duqk2Qmbj21ap8pRRbRY8sfobKje', NULL, 'orca', 'solana', NULL, NULL, 'treasury', SYSDATE()::TIMESTAMP_NTZ),
    ('9pvzg4ocGHZgGsoviHFjTvAR9oEjRe4bbyjMWXzjn3U1', NULL, 'orca', 'solana', NULL, NULL, 'treasury', SYSDATE()::TIMESTAMP_NTZ)
AS orca_treasury_accounts(address, name, artemis_application_id, chain, is_token, is_fungible, type, last_updated)