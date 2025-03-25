select reserve_address
from (values
    ('9DrvZvyWh1HuAoZxvYWMvkf2XCzryCpGgHqrMjyDWpmo'),  -- Kamino Reserve 1
    ('B9spsrMK6pJicYtukaZzDyzsUQLgc3jbx5gHVwdDxb6y'),  -- Kamino Reserve 2
    ('81BgcfZuZf9bESLvw3zDkh7cZmMtDwTPgkCvYu7zx26o'),  -- Kamino Reserve 3
    ('GuWEkEJb5bh8Ai2gaYmZWMTUq8MrFeoaDZ89BrQfB1FZ'),  -- Kamino Reserve 4
    ('Dx8iy2o46sK1DzWbEcznqSKeLbLVeu7otkibA3WohGAj')   -- Kamino Reserve 5
) as v(reserve_address)
