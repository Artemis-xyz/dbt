with reserve_addresses as (
    select reserve_address
    from (values
        ('9DrvZvyWh1HuAoZxvYWMvkf2XCzryCpGgHqrMjyDWpmo'),  -- Kamino Reserve 1
        ('B9spsrMK6pJicYtukaZzDyzsUQLgc3jbx5gHVwdDxb6y'),  -- Kamino Reserve 2
        ('81BgcfZuZf9bESLvw3zDkh7cZmMtDwTPgkCvYu7zx26o'),  -- Kamino Reserve 3
        ('GuWEkEJb5bh8Ai2gaYmZWMTUq8MrFeoaDZ89BrQfB1FZ'),  -- Kamino Reserve 4
        ('Dx8iy2o46sK1DzWbEcznqSKeLbLVeu7otkibA3WohGAj')   -- Kamino Reserve 5
    ) as v(reserve_address)
) 

select distinct account_address  
from pc_dbt_db.prod.fact_solana_token_account_to_mint
where lower(owner) in (select distinct lower(reserve_address) from reserve_addresses)