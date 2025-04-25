with all_burns AS (
    select
      date(block_timestamp) as date,
      sum(amount) * 100 as burn_amount
    from ethereum_flipside.core.ez_token_transfers
    where
        lower(contract_address) = lower('0x949D48EcA67b17269629c7194F4b727d4Ef9E5d6') -- MC contract
        and lower(to_address) = lower('0x0000000000000000000000000000000000000000')
        and lower(from_address) = lower('0x80e1dc8B02E0D44Bfc15E7F839A56C19d9d81a04') -- Merit Circle Token Burner
    group by 1
    
    union all
    
    select
      date(block_timestamp) as date,
      sum(amount) as burn_amount
    from ethereum_flipside.core.ez_token_transfers
    where
        lower(contract_address) = lower('0x62D0A8458eD7719FDAF978fe5929C6D342B0bFcE') -- BEAM contract
        and lower(to_address) = lower('0x0000000000000000000000000000000000000000')
    group by 1
),
  
yes_big_burn as(
    select
        date,
        sum(burn_amount) as burn_native
    from all_burns
    group by 1
),

no_big_burn as (
    select
        date,
        sum(burn_amount) as burn_native
    FROM
      all_burns
    GROUP BY 1
    having SUM(burn_amount) < 20000000000
)

select 
    y.date,
    coalesce(n.burn_native, 1/1e18) as burns_native
from yes_big_burn y left join no_big_burn n on y.date = n.date