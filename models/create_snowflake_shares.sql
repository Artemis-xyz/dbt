-- Should be run directly in the Snowflake Editor as an Account Admin! 
-- Used to recreate private sharing views
-- __________                 
-- \______   \_____    _____  
-- |    |  _/\__  \  /     \ 
-- |    |   \ / __ \|  Y Y  \
-- |______  /(____  /__|_|  /
-- \/      \/      \/ 
create or replace secure view PC_DBT_DB.BAM.FUNDAMENTAL_DATA_BY_CHAIN(
	DATE,
	TXNS,
	DAU,
	GAS,
	GAS_USD,
	RETURNING_USERS,
	NEW_USERS,
	CHAIN
) as SELECT DATE,
	TXNS,
	DAU,
	GAS,
	GAS_USD,
	RETURNING_USERS,
	NEW_USERS,
	CHAIN
FROM PC_DBT_DB.PROD.ALL_CHAINS_GAS_DAU_TXNS_BY_CHAIN;


create or replace secure view PC_DBT_DB.BAM.FUNDAMENTAL_DATA_BY_CATEGORY(
	DATE,
	CATEGORY,
	CHAIN,
	TOTAL_GAS,
	TOTAL_GAS_USD,
	TRANSACTIONS,
	DAU,
	RETURNING_USERS,
	NEW_USERS
) as SELECT DATE,
	CATEGORY,
	CHAIN,
	TOTAL_GAS,
	TOTAL_GAS_USD,
	TRANSACTIONS,
	DAU,
	RETURNING_USERS,
	NEW_USERS
FROM PC_DBT_DB.PROD.ALL_CHAINS_GAS_DAU_TXNS_BY_CATEGORY_V2;

create or replace secure view PC_DBT_DB.BAM.FUNDAMENTAL_DATA_BY_NAMESPACE(
	DATE,
	NAMESPACE,
	FRIENDLY_NAME,
	CATEGORY,
	CHAIN,
	IMAGE_THUMBNAIL,
	IMAGE_SMALL,
	COINGECKO_ID,
	TOTAL_GAS,
	TOTAL_GAS_USD,
	TRANSACTIONS,
	DAU,
	RETURNING_USERS,
	NEW_USERS
) as SELECT DATE,
	NAMESPACE,
	FRIENDLY_NAME,
	CATEGORY,
	CHAIN,
	IMAGE_THUMBNAIL,
	IMAGE_SMALL,
	COINGECKO_ID,
	TOTAL_GAS,
	TOTAL_GAS_USD,
	TRANSACTIONS,
	DAU,
	RETURNING_USERS,
	NEW_USERS
FROM PC_DBT_DB.PROD.ALL_CHAINS_GAS_DAU_TXNS_BY_APPLICATION;

create or replace secure view PC_DBT_DB.BAM.FUNDAMENTAL_DATA_BY_CONTRACT(
	CONTRACT_ADDRESS,
	DATE,
	NAME,
	SYMBOL,
	NAMESPACE,
	FRIENDLY_NAME,
	CATEGORY,
	TOTAL_GAS,
	TOTAL_GAS_USD,
	TRANSACTIONS,
	DAU,
	TOKEN_TRANSFER_USD,
	TOKEN_TRANSFER,
	AVG_TOKEN_PRICE,
	CHAIN
) as SELECT CONTRACT_ADDRESS,
	DATE,
	NAME,
	SYMBOL,
	NAMESPACE,
	FRIENDLY_NAME,
	CATEGORY,
	TOTAL_GAS,
	TOTAL_GAS_USD,
	TRANSACTIONS,
	DAU,
	TOKEN_TRANSFER_USD,
	TOKEN_TRANSFER,
	AVG_TOKEN_PRICE,
	CHAIN
FROM PC_DBT_DB.PROD.ALL_CHAINS_GAS_DAU_TXNS_BY_CONTRACT_V2;


-- Used by BH
create or replace secure view PC_DBT_DB.BAM.CONTRACTS(
	ADDRESS,
	NAME,
	APP,
	CHAIN,
	CATEGORY,
	SUB_CATEGORY
) as SELECT ADDRESS,
	NAME,
	APP,
	CHAIN,
	CATEGORY,
	SUB_CATEGORY
FROM PC_DBT_DB.PROD.dim_contracts_gold;

create or replace secure view PC_DBT_DB.BAM.NAMESPACES(
	NAMESPACE,
	FRIENDLY_NAME,
	SUB_CATEGORY,
	CATEGORY,
	ARTEMIS_ID,
	COINGECKO_ID,
	ECOSYSTEM_ID,
	DEFILLAMA_PROTOCOL_ID,
	PARENT_APP,
	VISIBILITY,
	SYMBOL,
	ICON
) as SELECT NAMESPACE,
	FRIENDLY_NAME,
	SUB_CATEGORY,
	CATEGORY,
	ARTEMIS_ID,
	COINGECKO_ID,
	ECOSYSTEM_ID,
	DEFILLAMA_PROTOCOL_ID,
	PARENT_APP,
	VISIBILITY,
	SYMBOL,
	ICON
FROM PC_DBT_DB.PROD.dim_apps_gold;


-- _________                                       
-- \_   ___ \  ____   _____   _____   ____   ____  
-- /    \  \/ /  _ \ /     \ /     \ /  _ \ /    \ 
-- \     \___(  <_> )  Y Y  \  Y Y  (  <_> )   |  \
-- \______  /\____/|__|_|  /__|_|  /\____/|___|  /
-- \/             \/      \/            \/ 
create or replace secure view PC_DBT_DB.COMMON.DAILY_MARKET_DATA(
	DATE,
	COINGECKO_ID,
	PRICE,
	MARKET_CAP,
	H24_VOLUME
) as select DATE, COINGECKO_ID, SHIFTED_TOKEN_PRICE_USD AS PRICE, SHIFTED_TOKEN_MARKET_CAP AS MARKET_CAP, SHIFTED_TOKEN_H24_VOLUME_USD AS H24_VOLUME from PC_DBT_DB.PROD.fact_coingecko_token_date_adjusted_gold;


-- __________.__  __               .__        
-- \______   \__|/  |_  ____  ____ |__| ____  
-- |    |  _/  \   __\/ ___\/  _ \|  |/    \ 
-- |    |   \  ||  | \  \__(  <_> )  |   |  \
-- |______  /__||__|  \___  >____/|__|___|  /
-- \/              \/              \/ 
create or replace secure view PC_DBT_DB.BITCOIN.ADDRESSES_WITH_BALANCE_GTE_POINT_ZERO_ONE(
	DATE,
	ADDRESSES
) as select DATE,
	ADDRESSES
from PC_DBT_DB.PROD.FACT_BITCOIN_ADDRESSES_WITH_BALANCE_GTE_POINT_ZERO_ONE_GOLD;


create or replace secure view PC_DBT_DB.BITCOIN.ADDRESSES_WITH_BALANCE_GTE_ONE(
	DATE,
	ADDRESSES
) as select DATE,
	ADDRESSES
from PC_DBT_DB.PROD.FACT_BITCOIN_ADDRESSES_WITH_BALANCE_GTE_ONE_GOLD;


create or replace secure view PC_DBT_DB.BITCOIN.ADDRESSES_WITH_BALANCE_GTE_TEN(
	DATE,
	ADDRESSES
) as select DATE,
	ADDRESSES
from PC_DBT_DB.PROD.FACT_BITCOIN_ADDRESSES_WITH_BALANCE_GTE_TEN_GOLD;


create or replace secure view PC_DBT_DB.BITCOIN.ADDRESSES_WITH_BALANCE_GTE_ONE_HUNDRED(
	DATE,
	ADDRESSES
) as select DATE,
	ADDRESSES
from PC_DBT_DB.PROD.FACT_BITCOIN_ADDRESSES_WITH_BALANCE_GTE_ONE_HUNDRED_GOLD;


 create or replace secure view PC_DBT_DB.BITCOIN.HODL_WAVE(
	DATE,
    TOTAL_UTXO_VALUE,
    UTXO_VALUE_UNDER_1D,
    UTXO_VALUE_1D_1W,
    UTXO_VALUE_1W_1M,
    UTXO_VALUE_1M_3M,
    UTXO_VALUE_3M_6M,
    UTXO_VALUE_6M_12M,
    UTXO_VALUE_1Y_2Y,
    UTXO_VALUE_2Y_3Y,
    UTXO_VALUE_3Y_5Y,
    UTXO_VALUE_5Y_7Y,
    UTXO_VALUE_7Y_10Y,
    UTXO_VALUE_GREATER_10Y
) as select 
DATE,
    TOTAL_UTXO_VALUE,
    UTXO_VALUE_UNDER_1D,
    UTXO_VALUE_1D_1W,
    UTXO_VALUE_1W_1M,
    UTXO_VALUE_1M_3M,
    UTXO_VALUE_3M_6M,
    UTXO_VALUE_6M_12M,
    UTXO_VALUE_1Y_2Y,
    UTXO_VALUE_2Y_3Y,
    UTXO_VALUE_3Y_5Y,
    UTXO_VALUE_5Y_7Y,
    UTXO_VALUE_7Y_10Y,
    UTXO_VALUE_GREATER_10Y
from PC_DBT_DB.PROD.fact_bitcoin_hodl_wave_gold;

create or replace secure view PC_DBT_DB.BITCOIN.MINER_FEES(
	DATE,
	TOTAL_REWARD,
    BLOCK_REWARD,
    FEES
) as select DATE,
	TOTAL_REWARD,
    BLOCK_REWARD,
    FEES
from PC_DBT_DB.PROD.FACT_BITCOIN_MINER_FEES_GOLD;

-- ________                     .__                                     
-- \______ \   _______  __ ____ |  |   ____ ______   ___________  ______
-- |    |  \_/ __ \  \/ // __ \|  |  /  _ \\____ \_/ __ \_  __ \/  ___/
-- |    `   \  ___/\   /\  ___/|  |_(  <_> )  |_> >  ___/|  | \/\___ \ 
-- /_______  /\___  >\_/  \___  >____/\____/|   __/ \___  >__|  /____  >
-- \/     \/          \/            |__|        \/           \/ 
create or replace secure view PC_DBT_DB.DEVELOPERS.ecosystems(
    ecosystem_id,
	ecosystem_name,
	symbol 
) as select
    id,
    ecosystem_name,
	symbol
from PC_DBT_DB.PROD.core_ecosystems;


create or replace secure view PC_DBT_DB.DEVELOPERS.ecosystem_repositories(
    ecosystem_id,
    repo_full_name,
    forked_from
) as select
    ecosystem_id,
    repo_full_name,
    forked_from
from PC_DBT_DB.PROD.core_EcosystemRepositories;


create or replace secure view PC_DBT_DB.DEVELOPERS.sub_ecosystems(
    ecosystem_id,
	subecosystem_name
) as select
    ecosystem_id,
	subecosystem_name
from PC_DBT_DB.PROD.core_SubEcosystems;


create or replace secure view PC_DBT_DB.DEVELOPERS.weekly_devs_global(
    date,
	val
) as select
    date,
	val
from PC_DBT_DB.PROD.core_WeeklyDevsGlobalWithoutForks;


create or replace secure view PC_DBT_DB.DEVELOPERS.weekly_commits_global(
    date,
	val
) as select
    date,
	val
from PC_DBT_DB.PROD.core_WeeklyCommitsGlobalWithoutForks;


create or replace secure view PC_DBT_DB.DEVELOPERS.weekly_commit_history(
    repo_full_name,
    date,
    github_author_id,
    num_commits,
    num_additions,
    num_deletions
) as select
    repo_full_name,
    start_of_week,
    author_id,
    num_commits,
    num_additions,
    num_deletions
from PC_DBT_DB.PROD.core_WeeklyCommitHistory;


create or replace secure view PC_DBT_DB.DEVELOPERS.weekly_devs_core_ecosystem(
    ecosystem_id,
    date,
    val
) as select
    ecosystem_id,
    date,
    val
from PC_DBT_DB.PROD.core_WeeklyDevsCoreEcosystemWithoutForks;

create or replace secure view PC_DBT_DB.DEVELOPERS.weekly_devs_sub_ecosystem(
    ecosystem_id,
    date,
    val
) as select
    ecosystem_id,
    date,
    val
from PC_DBT_DB.PROD.core_WeeklyDevsSubEcosystemsWithoutForks;

create or replace secure view PC_DBT_DB.DEVELOPERS.weekly_commits_core_ecosystem(
    ecosystem_id,
    date,
    val
) as select
    ecosystem_id,
    date,
    val
from PC_DBT_DB.PROD.core_WeeklyCommitsCoreEcosystemWithoutForks;

create or replace secure view PC_DBT_DB.DEVELOPERS.weekly_commits_sub_ecosystem(
    ecosystem_id,
    date,
    val
) as select
    ecosystem_id,
    date,
    val
from PC_DBT_DB.PROD.core_WeeklyCommitsSubEcosystemsWithoutForks;
