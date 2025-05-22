{{ config(materialized="table") }}

with community_market_pairs as (
  SELECT
    block_timestamp,
    decoded_log:index::NUMBER as pair_index,
    HEX_TO_UTF8(decoded_log:from::STRING) as from_symbol,
    HEX_TO_UTF8(decoded_log:to::STRING) as to_symbol,
    from_symbol || ' - ' || to_symbol as market_pair
  from
    arbitrum_flipside.core.ez_decoded_event_logs
  WHERE
    1 = 1
    AND contract_address = lower('0x260e349f643f12797fdc6f8c9d3df211d5577823') --pairsStorageAddress https://ostium-labs.gitbook.io/ostium-docs/security/smart-contract-audits
    AND TOPIC_0 = lower('0x797331683c7d888af91e5c6800626a01b5f1f7337a712c6915baa1b39c138a09') --event_name = 'PairAdded'
    AND tx_succeeded
)
select * from community_market_pairs