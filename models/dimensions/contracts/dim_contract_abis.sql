{{ config(materialized="table") }}

select null as contract_address, null as chain, 'ERC20_Standard' as abi_type, parse_json('[
  {
    "constant": true,
    "inputs": [],
    "name": "name",
    "outputs": [
      {
        "name": "",
        "type": "string"
      }
    ],
    "payable": false,
    "stateMutability": "view",
    "type": "function"
  },
  {
    "constant": false,
    "inputs": [
      {
        "name": "_spender",
        "type": "address"
      },
      {
        "name": "_value",
        "type": "uint256"
      }
    ],
    "name": "approve",
    "outputs": [
      {
        "name": "",
        "type": "bool"
      }
    ],
    "payable": false,
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "constant": true,
    "inputs": [],
    "name": "totalSupply",
    "outputs": [
      {
        "name": "",
        "type": "uint256"
      }
    ],
    "payable": false,
    "stateMutability": "view",
    "type": "function"
  },
  {
    "constant": false,
    "inputs": [
      {
        "name": "_from",
        "type": "address"
      },
      {
        "name": "_to",
        "type": "address"
      },
      {
        "name": "_value",
        "type": "uint256"
      }
    ],
    "name": "transferFrom",
    "outputs": [
      {
        "name": "",
        "type": "bool"
      }
    ],
    "payable": false,
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "constant": true,
    "inputs": [],
    "name": "decimals",
    "outputs": [
      {
        "name": "",
        "type": "uint8"
      }
    ],
    "payable": false,
    "stateMutability": "view",
    "type": "function"
  },
  {
    "constant": true,
    "inputs": [
      {
        "name": "_owner",
        "type": "address"
      }
    ],
    "name": "balanceOf",
    "outputs": [
      {
        "name": "balance",
        "type": "uint256"
      }
    ],
    "payable": false,
    "stateMutability": "view",
    "type": "function"
  },
  {
    "constant": true,
    "inputs": [],
    "name": "symbol",
    "outputs": [
      {
        "name": "",
        "type": "string"
      }
    ],
    "payable": false,
    "stateMutability": "view",
    "type": "function"
  },
  {
    "constant": false,
    "inputs": [
      {
        "name": "_to",
        "type": "address"
      },
      {
        "name": "_value",
        "type": "uint256"
      }
    ],
    "name": "transfer",
    "outputs": [
      {
        "name": "",
        "type": "bool"
      }
    ],
    "payable": false,
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "constant": true,
    "inputs": [
      {
        "name": "_owner",
        "type": "address"
      },
      {
        "name": "_spender",
        "type": "address"
      }
    ],
    "name": "allowance",
    "outputs": [
      {
        "name": "",
        "type": "uint256"
      }
    ],
    "payable": false,
    "stateMutability": "view",
    "type": "function"
  },
  {
    "payable": true,
    "stateMutability": "payable",
    "type": "fallback"
  },
  {
    "anonymous": false,
    "inputs": [
      {
        "indexed": true,
        "name": "owner",
        "type": "address"
      },
      {
        "indexed": true,
        "name": "spender",
        "type": "address"
      },
      {
        "indexed": false,
        "name": "value",
        "type": "uint256"
      }
    ],
    "name": "Approval",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {
        "indexed": true,
        "name": "from",
        "type": "address"
      },
      {
        "indexed": true,
        "name": "to",
        "type": "address"
      },
      {
        "indexed": false,
        "name": "value",
        "type": "uint256"
      }
    ],
    "name": "Transfer",
    "type": "event"
  }
]
') as abi

union all 
select '0x48065fbBE25f71C9282ddf5e1cD6D6A887483D5e' as contract_address, 'celo' as chain, 'Contract' as abi_type, parse_json('[
  {
    "anonymous": false,
    "inputs": [
      {
        "indexed": true,
        "internalType": "address",
        "name": "owner",
        "type": "address"
      },
      {
        "indexed": true,
        "internalType": "address",
        "name": "spender",
        "type": "address"
      },
      {
        "indexed": false,
        "internalType": "uint256",
        "name": "value",
        "type": "uint256"
      }
    ],
    "name": "Approval",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {
        "indexed": true,
        "internalType": "address",
        "name": "authorizer",
        "type": "address"
      },
      {
        "indexed": true,
        "internalType": "bytes32",
        "name": "nonce",
        "type": "bytes32"
      }
    ],
    "name": "AuthorizationCanceled",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {
        "indexed": true,
        "internalType": "address",
        "name": "authorizer",
        "type": "address"
      },
      {
        "indexed": true,
        "internalType": "bytes32",
        "name": "nonce",
        "type": "bytes32"
      }
    ],
    "name": "AuthorizationUsed",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {
        "indexed": true,
        "internalType": "address",
        "name": "_user",
        "type": "address"
      }
    ],
    "name": "BlockPlaced",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {
        "indexed": true,
        "internalType": "address",
        "name": "_user",
        "type": "address"
      }
    ],
    "name": "BlockReleased",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {
        "indexed": true,
        "internalType": "address",
        "name": "_blockedUser",
        "type": "address"
      },
      {
        "indexed": false,
        "internalType": "uint256",
        "name": "_balance",
        "type": "uint256"
      }
    ],
    "name": "DestroyedBlockedFunds",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {
        "indexed": true,
        "internalType": "address",
        "name": "feeCurrencyWrapper",
        "type": "address"
      }
    ],
    "name": "LogSetFeeCurrencyWrapper",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {
        "indexed": true,
        "internalType": "address",
        "name": "_destination",
        "type": "address"
      },
      {
        "indexed": false,
        "internalType": "uint256",
        "name": "_amount",
        "type": "uint256"
      }
    ],
    "name": "Mint",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {
        "indexed": true,
        "internalType": "address",
        "name": "previousOwner",
        "type": "address"
      },
      {
        "indexed": true,
        "internalType": "address",
        "name": "newOwner",
        "type": "address"
      }
    ],
    "name": "OwnershipTransferred",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {
        "indexed": false,
        "internalType": "uint256",
        "name": "_amount",
        "type": "uint256"
      }
    ],
    "name": "Redeem",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {
        "indexed": true,
        "internalType": "address",
        "name": "from",
        "type": "address"
      },
      {
        "indexed": true,
        "internalType": "address",
        "name": "to",
        "type": "address"
      },
      {
        "indexed": false,
        "internalType": "uint256",
        "name": "value",
        "type": "uint256"
      }
    ],
    "name": "Transfer",
    "type": "event"
  },
  {
    "inputs": [],
    "name": "CANCEL_AUTHORIZATION_TYPEHASH",
    "outputs": [{ "internalType": "bytes32", "name": "", "type": "bytes32" }],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "DOMAIN_SEPARATOR",
    "outputs": [{ "internalType": "bytes32", "name": "", "type": "bytes32" }],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "RECEIVE_WITH_AUTHORIZATION_TYPEHASH",
    "outputs": [{ "internalType": "bytes32", "name": "", "type": "bytes32" }],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "TRANSFER_WITH_AUTHORIZATION_TYPEHASH",
    "outputs": [{ "internalType": "bytes32", "name": "", "type": "bytes32" }],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "address", "name": "_user", "type": "address" }
    ],
    "name": "addToBlockedList",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "address", "name": "owner", "type": "address" },
      { "internalType": "address", "name": "spender", "type": "address" }
    ],
    "name": "allowance",
    "outputs": [{ "internalType": "uint256", "name": "", "type": "uint256" }],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "address", "name": "spender", "type": "address" },
      { "internalType": "uint256", "name": "amount", "type": "uint256" }
    ],
    "name": "approve",
    "outputs": [{ "internalType": "bool", "name": "", "type": "bool" }],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "address", "name": "authorizer", "type": "address" },
      { "internalType": "bytes32", "name": "nonce", "type": "bytes32" }
    ],
    "name": "authorizationState",
    "outputs": [{ "internalType": "bool", "name": "", "type": "bool" }],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "address", "name": "account", "type": "address" }
    ],
    "name": "balanceOf",
    "outputs": [{ "internalType": "uint256", "name": "", "type": "uint256" }],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "address", "name": "authorizer", "type": "address" },
      { "internalType": "bytes32", "name": "nonce", "type": "bytes32" },
      { "internalType": "uint8", "name": "v", "type": "uint8" },
      { "internalType": "bytes32", "name": "r", "type": "bytes32" },
      { "internalType": "bytes32", "name": "s", "type": "bytes32" }
    ],
    "name": "cancelAuthorization",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "address", "name": "authorizer", "type": "address" },
      { "internalType": "bytes32", "name": "nonce", "type": "bytes32" },
      { "internalType": "bytes", "name": "signature", "type": "bytes" }
    ],
    "name": "cancelAuthorization",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      {
        "internalType": "address",
        "name": "refundRecipient",
        "type": "address"
      },
      { "internalType": "address", "name": "tipRecipient", "type": "address" },
      { "internalType": "address", "name": "", "type": "address" },
      {
        "internalType": "address",
        "name": "baseFeeRecipient",
        "type": "address"
      },
      { "internalType": "uint256", "name": "refundAmount", "type": "uint256" },
      { "internalType": "uint256", "name": "tipAmount", "type": "uint256" },
      { "internalType": "uint256", "name": "", "type": "uint256" },
      { "internalType": "uint256", "name": "baseFeeAmount", "type": "uint256" }
    ],
    "name": "creditGasFees",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "address", "name": "from", "type": "address" },
      { "internalType": "uint256", "name": "value", "type": "uint256" }
    ],
    "name": "debitGasFees",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "decimals",
    "outputs": [{ "internalType": "uint8", "name": "", "type": "uint8" }],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "address", "name": "spender", "type": "address" },
      {
        "internalType": "uint256",
        "name": "subtractedValue",
        "type": "uint256"
      }
    ],
    "name": "decreaseAllowance",
    "outputs": [{ "internalType": "bool", "name": "", "type": "bool" }],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "address", "name": "_blockedUser", "type": "address" }
    ],
    "name": "destroyBlockedFunds",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "feeCurrencyWrapper",
    "outputs": [{ "internalType": "address", "name": "", "type": "address" }],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "address", "name": "spender", "type": "address" },
      { "internalType": "uint256", "name": "addedValue", "type": "uint256" }
    ],
    "name": "increaseAllowance",
    "outputs": [{ "internalType": "bool", "name": "", "type": "bool" }],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "string", "name": "_name", "type": "string" },
      { "internalType": "string", "name": "_symbol", "type": "string" },
      { "internalType": "uint8", "name": "_decimals", "type": "uint8" }
    ],
    "name": "initialize",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [{ "internalType": "address", "name": "", "type": "address" }],
    "name": "isBlocked",
    "outputs": [{ "internalType": "bool", "name": "", "type": "bool" }],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [{ "internalType": "address", "name": "", "type": "address" }],
    "name": "isTrusted",
    "outputs": [{ "internalType": "bool", "name": "", "type": "bool" }],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "address", "name": "_destination", "type": "address" },
      { "internalType": "uint256", "name": "_amount", "type": "uint256" }
    ],
    "name": "mint",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      {
        "internalType": "address[]",
        "name": "_recipients",
        "type": "address[]"
      },
      { "internalType": "uint256[]", "name": "_values", "type": "uint256[]" }
    ],
    "name": "multiTransfer",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "name",
    "outputs": [{ "internalType": "string", "name": "", "type": "string" }],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "address", "name": "owner", "type": "address" }
    ],
    "name": "nonces",
    "outputs": [{ "internalType": "uint256", "name": "", "type": "uint256" }],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "owner",
    "outputs": [{ "internalType": "address", "name": "", "type": "address" }],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "address", "name": "owner_", "type": "address" },
      { "internalType": "address", "name": "spender", "type": "address" },
      { "internalType": "uint256", "name": "value", "type": "uint256" },
      { "internalType": "uint256", "name": "deadline", "type": "uint256" },
      { "internalType": "bytes", "name": "signature", "type": "bytes" }
    ],
    "name": "permit",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "address", "name": "owner_", "type": "address" },
      { "internalType": "address", "name": "spender", "type": "address" },
      { "internalType": "uint256", "name": "value", "type": "uint256" },
      { "internalType": "uint256", "name": "deadline", "type": "uint256" },
      { "internalType": "uint8", "name": "v", "type": "uint8" },
      { "internalType": "bytes32", "name": "r", "type": "bytes32" },
      { "internalType": "bytes32", "name": "s", "type": "bytes32" }
    ],
    "name": "permit",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "address", "name": "from", "type": "address" },
      { "internalType": "address", "name": "to", "type": "address" },
      { "internalType": "uint256", "name": "value", "type": "uint256" },
      { "internalType": "uint256", "name": "validAfter", "type": "uint256" },
      { "internalType": "uint256", "name": "validBefore", "type": "uint256" },
      { "internalType": "bytes32", "name": "nonce", "type": "bytes32" },
      { "internalType": "bytes", "name": "signature", "type": "bytes" }
    ],
    "name": "receiveWithAuthorization",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "address", "name": "from", "type": "address" },
      { "internalType": "address", "name": "to", "type": "address" },
      { "internalType": "uint256", "name": "value", "type": "uint256" },
      { "internalType": "uint256", "name": "validAfter", "type": "uint256" },
      { "internalType": "uint256", "name": "validBefore", "type": "uint256" },
      { "internalType": "bytes32", "name": "nonce", "type": "bytes32" },
      { "internalType": "uint8", "name": "v", "type": "uint8" },
      { "internalType": "bytes32", "name": "r", "type": "bytes32" },
      { "internalType": "bytes32", "name": "s", "type": "bytes32" }
    ],
    "name": "receiveWithAuthorization",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "uint256", "name": "_amount", "type": "uint256" }
    ],
    "name": "redeem",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "address", "name": "_user", "type": "address" }
    ],
    "name": "removeFromBlockedList",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "renounceOwnership",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      {
        "internalType": "address",
        "name": "_feeCurrencyWrapper",
        "type": "address"
      }
    ],
    "name": "setFeeCurrencyWrapper",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "symbol",
    "outputs": [{ "internalType": "string", "name": "", "type": "string" }],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [],
    "name": "totalSupply",
    "outputs": [{ "internalType": "uint256", "name": "", "type": "uint256" }],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "address", "name": "recipient", "type": "address" },
      { "internalType": "uint256", "name": "amount", "type": "uint256" }
    ],
    "name": "transfer",
    "outputs": [{ "internalType": "bool", "name": "", "type": "bool" }],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "address", "name": "_sender", "type": "address" },
      { "internalType": "address", "name": "_recipient", "type": "address" },
      { "internalType": "uint256", "name": "_amount", "type": "uint256" }
    ],
    "name": "transferFrom",
    "outputs": [{ "internalType": "bool", "name": "", "type": "bool" }],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "address", "name": "newOwner", "type": "address" }
    ],
    "name": "transferOwnership",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "address", "name": "from", "type": "address" },
      { "internalType": "address", "name": "to", "type": "address" },
      { "internalType": "uint256", "name": "value", "type": "uint256" },
      { "internalType": "uint256", "name": "validAfter", "type": "uint256" },
      { "internalType": "uint256", "name": "validBefore", "type": "uint256" },
      { "internalType": "bytes32", "name": "nonce", "type": "bytes32" },
      { "internalType": "bytes", "name": "signature", "type": "bytes" }
    ],
    "name": "transferWithAuthorization",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "address", "name": "from", "type": "address" },
      { "internalType": "address", "name": "to", "type": "address" },
      { "internalType": "uint256", "name": "value", "type": "uint256" },
      { "internalType": "uint256", "name": "validAfter", "type": "uint256" },
      { "internalType": "uint256", "name": "validBefore", "type": "uint256" },
      { "internalType": "bytes32", "name": "nonce", "type": "bytes32" },
      { "internalType": "uint8", "name": "v", "type": "uint8" },
      { "internalType": "bytes32", "name": "r", "type": "bytes32" },
      { "internalType": "bytes32", "name": "s", "type": "bytes32" }
    ],
    "name": "transferWithAuthorization",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
  }
]') as abi

union all 
select '0xaEb865bCa93DdC8F47b8e29F40C5399cE34d0C58' as contract_address, 'celo' as chain, 'Contract' as abi_type, parse_json('
[{"inputs":[{"internalType":"bool","name":"test","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"delay","type":"uint256"}],"name":"CommissionUpdateDelaySet","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"duration","type":"uint256"}],"name":"GroupLockedGoldRequirementsSet","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"size","type":"uint256"}],"name":"MaxGroupSizeSet","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"length","type":"uint256"}],"name":"MembershipHistoryLengthSet","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"registryAddress","type":"address"}],"name":"RegistrySet","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"validator","type":"address"},{"indexed":true,"internalType":"address","name":"group","type":"address"}],"name":"ValidatorAffiliated","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"validator","type":"address"},{"indexed":false,"internalType":"bytes","name":"blsPublicKey","type":"bytes"}],"name":"ValidatorBlsPublicKeyUpdated","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"validator","type":"address"},{"indexed":true,"internalType":"address","name":"group","type":"address"}],"name":"ValidatorDeaffiliated","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"validator","type":"address"}],"name":"ValidatorDeregistered","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"validator","type":"address"},{"indexed":false,"internalType":"bytes","name":"ecdsaPublicKey","type":"bytes"}],"name":"ValidatorEcdsaPublicKeyUpdated","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"validator","type":"address"},{"indexed":false,"internalType":"uint256","name":"validatorPayment","type":"uint256"},{"indexed":true,"internalType":"address","name":"group","type":"address"},{"indexed":false,"internalType":"uint256","name":"groupPayment","type":"uint256"}],"name":"ValidatorEpochPaymentDistributed","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"group","type":"address"},{"indexed":false,"internalType":"uint256","name":"commission","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"activationBlock","type":"uint256"}],"name":"ValidatorGroupCommissionUpdateQueued","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"group","type":"address"},{"indexed":false,"internalType":"uint256","name":"commission","type":"uint256"}],"name":"ValidatorGroupCommissionUpdated","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"group","type":"address"}],"name":"ValidatorGroupDeregistered","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"group","type":"address"},{"indexed":true,"internalType":"address","name":"validator","type":"address"}],"name":"ValidatorGroupMemberAdded","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"group","type":"address"},{"indexed":true,"internalType":"address","name":"validator","type":"address"}],"name":"ValidatorGroupMemberRemoved","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"group","type":"address"},{"indexed":true,"internalType":"address","name":"validator","type":"address"}],"name":"ValidatorGroupMemberReordered","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"group","type":"address"},{"indexed":false,"internalType":"uint256","name":"commission","type":"uint256"}],"name":"ValidatorGroupRegistered","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"duration","type":"uint256"}],"name":"ValidatorLockedGoldRequirementsSet","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"validator","type":"address"}],"name":"ValidatorRegistered","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"exponent","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"adjustmentSpeed","type":"uint256"}],"name":"ValidatorScoreParametersSet","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"validator","type":"address"},{"indexed":false,"internalType":"uint256","name":"score","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"epochScore","type":"uint256"}],"name":"ValidatorScoreUpdated","type":"event"},{"constant":false,"inputs":[{"internalType":"address","name":"validator","type":"address"},{"internalType":"address","name":"lesser","type":"address"},{"internalType":"address","name":"greater","type":"address"}],"name":"addFirstMember","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"validator","type":"address"}],"name":"addMember","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"group","type":"address"}],"name":"affiliate","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"uptime","type":"uint256"}],"name":"calculateEpochScore","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256[]","name":"uptimes","type":"uint256[]"}],"name":"calculateGroupEpochScore","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"sender","type":"address"},{"internalType":"bytes","name":"blsKey","type":"bytes"},{"internalType":"bytes","name":"blsPop","type":"bytes"}],"name":"checkProofOfPossession","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"commissionUpdateDelay","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[],"name":"deaffiliate","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"index","type":"uint256"}],"name":"deregisterValidator","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"index","type":"uint256"}],"name":"deregisterValidatorGroup","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"signer","type":"address"},{"internalType":"uint256","name":"maxPayment","type":"uint256"}],"name":"distributeEpochPaymentsFromSigner","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"downtimeGracePeriod","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"validatorAccount","type":"address"}],"name":"forceDeaffiliateIfValidator","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"aNumerator","type":"uint256"},{"internalType":"uint256","name":"aDenominator","type":"uint256"},{"internalType":"uint256","name":"bNumerator","type":"uint256"},{"internalType":"uint256","name":"bDenominator","type":"uint256"},{"internalType":"uint256","name":"exponent","type":"uint256"},{"internalType":"uint256","name":"_decimals","type":"uint256"}],"name":"fractionMulExp","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"getAccountLockedGoldRequirement","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"bytes","name":"header","type":"bytes"}],"name":"getBlockNumberFromHeader","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getCommissionUpdateDelay","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getEpochNumber","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"blockNumber","type":"uint256"}],"name":"getEpochNumberOfBlock","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getEpochSize","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getGroupLockedGoldRequirements","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"getGroupNumMembers","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address[]","name":"accounts","type":"address[]"}],"name":"getGroupsNumMembers","outputs":[{"internalType":"uint256[]","name":"","type":"uint256[]"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getMaxGroupSize","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"getMembershipHistory","outputs":[{"internalType":"uint256[]","name":"","type":"uint256[]"},{"internalType":"address[]","name":"","type":"address[]"},{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"getMembershipInLastEpoch","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"signer","type":"address"}],"name":"getMembershipInLastEpochFromSigner","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getNumRegisteredValidators","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"blockNumber","type":"uint256"}],"name":"getParentSealBitmap","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getRegisteredValidatorGroups","outputs":[{"internalType":"address[]","name":"","type":"address[]"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getRegisteredValidatorSigners","outputs":[{"internalType":"address[]","name":"","type":"address[]"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getRegisteredValidators","outputs":[{"internalType":"address[]","name":"","type":"address[]"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"account","type":"address"},{"internalType":"uint256","name":"n","type":"uint256"}],"name":"getTopGroupValidators","outputs":[{"internalType":"address[]","name":"","type":"address[]"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"getValidator","outputs":[{"internalType":"bytes","name":"ecdsaPublicKey","type":"bytes"},{"internalType":"bytes","name":"blsPublicKey","type":"bytes"},{"internalType":"address","name":"affiliation","type":"address"},{"internalType":"uint256","name":"score","type":"uint256"},{"internalType":"address","name":"signer","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"signer","type":"address"}],"name":"getValidatorBlsPublicKeyFromSigner","outputs":[{"internalType":"bytes","name":"blsPublicKey","type":"bytes"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"getValidatorGroup","outputs":[{"internalType":"address[]","name":"","type":"address[]"},{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256[]","name":"","type":"uint256[]"},{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"getValidatorGroupSlashingMultiplier","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getValidatorLockedGoldRequirements","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getValidatorScoreParameters","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"bytes","name":"header","type":"bytes"}],"name":"getVerifiedSealBitmapFromHeader","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getVersionNumber","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"pure","type":"function"},{"constant":true,"inputs":[],"name":"groupLockedGoldRequirements","outputs":[{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"uint256","name":"duration","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"account","type":"address"},{"internalType":"uint256","name":"epochNumber","type":"uint256"},{"internalType":"uint256","name":"index","type":"uint256"}],"name":"groupMembershipInEpoch","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"halveSlashingMultiplier","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"bytes","name":"header","type":"bytes"}],"name":"hashHeader","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"registryAddress","type":"address"},{"internalType":"uint256","name":"groupRequirementValue","type":"uint256"},{"internalType":"uint256","name":"groupRequirementDuration","type":"uint256"},{"internalType":"uint256","name":"validatorRequirementValue","type":"uint256"},{"internalType":"uint256","name":"validatorRequirementDuration","type":"uint256"},{"internalType":"uint256","name":"validatorScoreExponent","type":"uint256"},{"internalType":"uint256","name":"validatorScoreAdjustmentSpeed","type":"uint256"},{"internalType":"uint256","name":"_membershipHistoryLength","type":"uint256"},{"internalType":"uint256","name":"_slashingMultiplierResetPeriod","type":"uint256"},{"internalType":"uint256","name":"_maxGroupSize","type":"uint256"},{"internalType":"uint256","name":"_commissionUpdateDelay","type":"uint256"},{"internalType":"uint256","name":"_downtimeGracePeriod","type":"uint256"}],"name":"initialize","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"initialized","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"isOwner","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"isValidator","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"isValidatorGroup","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"maxGroupSize","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"meetsAccountLockedGoldRequirements","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"membershipHistoryLength","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"blockNumber","type":"uint256"}],"name":"minQuorumSize","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"minQuorumSizeInCurrentSet","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"numberValidatorsInCurrentSet","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"blockNumber","type":"uint256"}],"name":"numberValidatorsInSet","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"bytes","name":"ecdsaPublicKey","type":"bytes"},{"internalType":"bytes","name":"blsPublicKey","type":"bytes"},{"internalType":"bytes","name":"blsPop","type":"bytes"}],"name":"registerValidator","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"commission","type":"uint256"}],"name":"registerValidatorGroup","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"registry","outputs":[{"internalType":"contract IRegistry","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"validator","type":"address"}],"name":"removeMember","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[],"name":"renounceOwnership","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"validator","type":"address"},{"internalType":"address","name":"lesserMember","type":"address"},{"internalType":"address","name":"greaterMember","type":"address"}],"name":"reorderMember","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[],"name":"resetSlashingMultiplier","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"delay","type":"uint256"}],"name":"setCommissionUpdateDelay","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"value","type":"uint256"}],"name":"setDowntimeGracePeriod","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"uint256","name":"duration","type":"uint256"}],"name":"setGroupLockedGoldRequirements","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"size","type":"uint256"}],"name":"setMaxGroupSize","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"length","type":"uint256"}],"name":"setMembershipHistoryLength","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"commission","type":"uint256"}],"name":"setNextCommissionUpdate","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"registryAddress","type":"address"}],"name":"setRegistry","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"value","type":"uint256"}],"name":"setSlashingMultiplierResetPeriod","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"uint256","name":"duration","type":"uint256"}],"name":"setValidatorLockedGoldRequirements","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"exponent","type":"uint256"},{"internalType":"uint256","name":"adjustmentSpeed","type":"uint256"}],"name":"setValidatorScoreParameters","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"slashingMultiplierResetPeriod","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"bytes","name":"blsPublicKey","type":"bytes"},{"internalType":"bytes","name":"blsPop","type":"bytes"}],"name":"updateBlsPublicKey","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[],"name":"updateCommission","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"account","type":"address"},{"internalType":"address","name":"signer","type":"address"},{"internalType":"bytes","name":"ecdsaPublicKey","type":"bytes"}],"name":"updateEcdsaPublicKey","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"account","type":"address"},{"internalType":"address","name":"signer","type":"address"},{"internalType":"bytes","name":"ecdsaPublicKey","type":"bytes"},{"internalType":"bytes","name":"blsPublicKey","type":"bytes"},{"internalType":"bytes","name":"blsPop","type":"bytes"}],"name":"updatePublicKeys","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"signer","type":"address"},{"internalType":"uint256","name":"uptime","type":"uint256"}],"name":"updateValidatorScoreFromSigner","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"validatorLockedGoldRequirements","outputs":[{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"uint256","name":"duration","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"index","type":"uint256"}],"name":"validatorSignerAddressFromCurrentSet","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"index","type":"uint256"},{"internalType":"uint256","name":"blockNumber","type":"uint256"}],"name":"validatorSignerAddressFromSet","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"}]
') as abi

union all 
select '0xDfca3a8d7699D8bAfe656823AD60C17cb8270ECC' as contract_address, 'celo' as chain, 'Contract' as abi_type, parse_json('
[{"inputs":[{"internalType":"bool","name":"test","type":"bool"}],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"adjustmentSpeed","type":"uint256"}],"name":"AdjustmentSpeedSet","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"baseFeeOpCodeActivationBlock","type":"uint256"}],"name":"BaseFeeOpCodeActivationBlockSet","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"gasPriceMinimumFloor","type":"uint256"}],"name":"GasPriceMinimumFloorSet","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"gasPriceMinimum","type":"uint256"}],"name":"GasPriceMinimumUpdated","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"registryAddress","type":"address"}],"name":"RegistrySet","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"targetDensity","type":"uint256"}],"name":"TargetDensitySet","type":"event"},{"inputs":[],"name":"ABSOLUTE_MINIMAL_GAS_PRICE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"adjustmentSpeed","outputs":[{"internalType":"uint256","name":"value","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"baseFeeOpCodeActivationBlock","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"deprecated_gasPriceMinimum","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"gasPriceMinimum","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"gasPriceMinimumFloor","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"tokenAddress","type":"address"}],"name":"getGasPriceMinimum","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"blockGasTotal","type":"uint256"},{"internalType":"uint256","name":"blockGasLimit","type":"uint256"}],"name":"getUpdatedGasPriceMinimum","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getVersionNumber","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"address","name":"_registryAddress","type":"address"},{"internalType":"uint256","name":"_gasPriceMinimumFloor","type":"uint256"},{"internalType":"uint256","name":"_targetDensity","type":"uint256"},{"internalType":"uint256","name":"_adjustmentSpeed","type":"uint256"},{"internalType":"uint256","name":"_baseFeeOpCodeActivationBlock","type":"uint256"}],"name":"initialize","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"initialized","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"registry","outputs":[{"internalType":"contract IRegistry","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"renounceOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_adjustmentSpeed","type":"uint256"}],"name":"setAdjustmentSpeed","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_baseFeeOpCodeActivationBlock","type":"uint256"}],"name":"setBaseFeeOpCodeActivationBlock","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_gasPriceMinimumFloor","type":"uint256"}],"name":"setGasPriceMinimumFloor","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"registryAddress","type":"address"}],"name":"setRegistry","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_targetDensity","type":"uint256"}],"name":"setTargetDensity","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"targetDensity","outputs":[{"internalType":"uint256","name":"value","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"blockGasTotal","type":"uint256"},{"internalType":"uint256","name":"blockGasLimit","type":"uint256"}],"name":"updateGasPriceMinimum","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"}]
') as abi

union all 
select '0x07F007d389883622Ef8D4d347b3f78007f28d8b7' as contract_address, 'celo' as chain, 'Contract' as abi_type, parse_json('
[{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"partner","type":"address"},{"indexed":false,"internalType":"uint256","name":"fraction","type":"uint256"}],"name":"CarbonOffsettingFundSet","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"fraction","type":"uint256"}],"name":"CommunityRewardFractionSet","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"registryAddress","type":"address"}],"name":"RegistrySet","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"max","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"underspendAdjustmentFactor","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"overspendAdjustmentFactor","type":"uint256"}],"name":"RewardsMultiplierParametersSet","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"payment","type":"uint256"}],"name":"TargetValidatorEpochPaymentSet","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"fraction","type":"uint256"}],"name":"TargetVotingGoldFractionSet","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"max","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"adjustmentFactor","type":"uint256"}],"name":"TargetVotingYieldParametersSet","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"target","type":"uint256"}],"name":"TargetVotingYieldSet","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"fraction","type":"uint256"}],"name":"TargetVotingYieldUpdated","type":"event"},{"constant":true,"inputs":[],"name":"calculateTargetEpochRewards","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"carbonOffsettingPartner","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"sender","type":"address"},{"internalType":"bytes","name":"blsKey","type":"bytes"},{"internalType":"bytes","name":"blsPop","type":"bytes"}],"name":"checkProofOfPossession","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"aNumerator","type":"uint256"},{"internalType":"uint256","name":"aDenominator","type":"uint256"},{"internalType":"uint256","name":"bNumerator","type":"uint256"},{"internalType":"uint256","name":"bDenominator","type":"uint256"},{"internalType":"uint256","name":"exponent","type":"uint256"},{"internalType":"uint256","name":"_decimals","type":"uint256"}],"name":"fractionMulExp","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"bytes","name":"header","type":"bytes"}],"name":"getBlockNumberFromHeader","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getCarbonOffsettingFraction","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getCommunityRewardFraction","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getEpochNumber","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"blockNumber","type":"uint256"}],"name":"getEpochNumberOfBlock","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getEpochSize","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"blockNumber","type":"uint256"}],"name":"getParentSealBitmap","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getRewardsMultiplier","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getRewardsMultiplierParameters","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getTargetGoldTotalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getTargetTotalEpochPaymentsInGold","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getTargetVoterRewards","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getTargetVotingGoldFraction","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getTargetVotingYieldParameters","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"bytes","name":"header","type":"bytes"}],"name":"getVerifiedSealBitmapFromHeader","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getVersionNumber","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"pure","type":"function"},{"constant":true,"inputs":[],"name":"getVotingGoldFraction","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"bytes","name":"header","type":"bytes"}],"name":"hashHeader","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"registryAddress","type":"address"},{"internalType":"uint256","name":"targetVotingYieldInitial","type":"uint256"},{"internalType":"uint256","name":"targetVotingYieldMax","type":"uint256"},{"internalType":"uint256","name":"targetVotingYieldAdjustmentFactor","type":"uint256"},{"internalType":"uint256","name":"rewardsMultiplierMax","type":"uint256"},{"internalType":"uint256","name":"rewardsMultiplierUnderspendAdjustmentFactor","type":"uint256"},{"internalType":"uint256","name":"rewardsMultiplierOverspendAdjustmentFactor","type":"uint256"},{"internalType":"uint256","name":"_targetVotingGoldFraction","type":"uint256"},{"internalType":"uint256","name":"_targetValidatorEpochPayment","type":"uint256"},{"internalType":"uint256","name":"_communityRewardFraction","type":"uint256"},{"internalType":"address","name":"_carbonOffsettingPartner","type":"address"},{"internalType":"uint256","name":"_carbonOffsettingFraction","type":"uint256"}],"name":"initialize","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"initialized","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"isOwner","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"isReserveLow","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"blockNumber","type":"uint256"}],"name":"minQuorumSize","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"minQuorumSizeInCurrentSet","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"numberValidatorsInCurrentSet","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"blockNumber","type":"uint256"}],"name":"numberValidatorsInSet","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"registry","outputs":[{"internalType":"contract IRegistry","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[],"name":"renounceOwnership","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"partner","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"setCarbonOffsettingFund","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"value","type":"uint256"}],"name":"setCommunityRewardFraction","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"registryAddress","type":"address"}],"name":"setRegistry","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"max","type":"uint256"},{"internalType":"uint256","name":"underspendAdjustmentFactor","type":"uint256"},{"internalType":"uint256","name":"overspendAdjustmentFactor","type":"uint256"}],"name":"setRewardsMultiplierParameters","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"value","type":"uint256"}],"name":"setTargetValidatorEpochPayment","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"value","type":"uint256"}],"name":"setTargetVotingGoldFraction","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"targetVotingYield","type":"uint256"}],"name":"setTargetVotingYield","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"max","type":"uint256"},{"internalType":"uint256","name":"adjustmentFactor","type":"uint256"}],"name":"setTargetVotingYieldParameters","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"startTime","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"targetValidatorEpochPayment","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[],"name":"updateTargetVotingYield","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"index","type":"uint256"}],"name":"validatorSignerAddressFromCurrentSet","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"index","type":"uint256"},{"internalType":"uint256","name":"blockNumber","type":"uint256"}],"name":"validatorSignerAddressFromSet","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"}]
') as abi

union all 
select '0x8d6677192144292870907e3fa8a5527fe55a7ff6' as contract_address, 'celo' as chain, 'Contract' as abi_type, parse_json('
[{"inputs":[{"internalType":"bool","name":"test","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"account","type":"address"},{"indexed":false,"internalType":"bool","name":"flag","type":"bool"}],"name":"AllowedToVoteOverMaxNumberOfGroups","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"electabilityThreshold","type":"uint256"}],"name":"ElectabilityThresholdSet","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"min","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"max","type":"uint256"}],"name":"ElectableValidatorsSet","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"group","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"EpochRewardsDistributedToVoters","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"maxNumGroupsVotedFor","type":"uint256"}],"name":"MaxNumGroupsVotedForSet","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"registryAddress","type":"address"}],"name":"RegistrySet","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"account","type":"address"},{"indexed":true,"internalType":"address","name":"group","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"units","type":"uint256"}],"name":"ValidatorGroupActiveVoteRevoked","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"group","type":"address"}],"name":"ValidatorGroupMarkedEligible","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"group","type":"address"}],"name":"ValidatorGroupMarkedIneligible","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"account","type":"address"},{"indexed":true,"internalType":"address","name":"group","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"ValidatorGroupPendingVoteRevoked","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"account","type":"address"},{"indexed":true,"internalType":"address","name":"group","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"units","type":"uint256"}],"name":"ValidatorGroupVoteActivated","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"account","type":"address"},{"indexed":true,"internalType":"address","name":"group","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"ValidatorGroupVoteCast","type":"event"},{"constant":false,"inputs":[{"internalType":"address","name":"group","type":"address"}],"name":"activate","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"group","type":"address"},{"internalType":"address","name":"account","type":"address"}],"name":"activateForAccount","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"allowedToVoteOverMaxNumberOfGroups","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"cachedVotesByAccount","outputs":[{"internalType":"uint256","name":"totalVotes","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"group","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"canReceiveVotes","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"sender","type":"address"},{"internalType":"bytes","name":"blsKey","type":"bytes"},{"internalType":"bytes","name":"blsPop","type":"bytes"}],"name":"checkProofOfPossession","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"group","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"address","name":"lesser","type":"address"},{"internalType":"address","name":"greater","type":"address"}],"name":"distributeEpochRewards","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"minElectableValidators","type":"uint256"},{"internalType":"uint256","name":"maxElectableValidators","type":"uint256"}],"name":"electNValidatorSigners","outputs":[{"internalType":"address[]","name":"","type":"address[]"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"electValidatorSigners","outputs":[{"internalType":"address[]","name":"","type":"address[]"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"electabilityThreshold","outputs":[{"internalType":"uint256","name":"value","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"electableValidators","outputs":[{"internalType":"uint256","name":"min","type":"uint256"},{"internalType":"uint256","name":"max","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"account","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"address[]","name":"lessers","type":"address[]"},{"internalType":"address[]","name":"greaters","type":"address[]"},{"internalType":"uint256[]","name":"indices","type":"uint256[]"}],"name":"forceDecrementVotes","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"aNumerator","type":"uint256"},{"internalType":"uint256","name":"aDenominator","type":"uint256"},{"internalType":"uint256","name":"bNumerator","type":"uint256"},{"internalType":"uint256","name":"bDenominator","type":"uint256"},{"internalType":"uint256","name":"exponent","type":"uint256"},{"internalType":"uint256","name":"_decimals","type":"uint256"}],"name":"fractionMulExp","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"group","type":"address"}],"name":"getActiveVoteUnitsForGroup","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"group","type":"address"},{"internalType":"address","name":"account","type":"address"}],"name":"getActiveVoteUnitsForGroupByAccount","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getActiveVotes","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"group","type":"address"}],"name":"getActiveVotesForGroup","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"group","type":"address"},{"internalType":"address","name":"account","type":"address"}],"name":"getActiveVotesForGroupByAccount","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"bytes","name":"header","type":"bytes"}],"name":"getBlockNumberFromHeader","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getCurrentValidatorSigners","outputs":[{"internalType":"address[]","name":"","type":"address[]"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getElectabilityThreshold","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getElectableValidators","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getEligibleValidatorGroups","outputs":[{"internalType":"address[]","name":"","type":"address[]"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getEpochNumber","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"blockNumber","type":"uint256"}],"name":"getEpochNumberOfBlock","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getEpochSize","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"group","type":"address"}],"name":"getGroupEligibility","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"group","type":"address"},{"internalType":"uint256","name":"totalEpochRewards","type":"uint256"},{"internalType":"uint256[]","name":"uptimes","type":"uint256[]"}],"name":"getGroupEpochRewards","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"getGroupsVotedForByAccount","outputs":[{"internalType":"address[]","name":"","type":"address[]"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"group","type":"address"}],"name":"getNumVotesReceivable","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"blockNumber","type":"uint256"}],"name":"getParentSealBitmap","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"group","type":"address"}],"name":"getPendingVotesForGroup","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"group","type":"address"},{"internalType":"address","name":"account","type":"address"}],"name":"getPendingVotesForGroupByAccount","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getTotalVotes","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"getTotalVotesByAccount","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getTotalVotesForEligibleValidatorGroups","outputs":[{"internalType":"address[]","name":"groups","type":"address[]"},{"internalType":"uint256[]","name":"values","type":"uint256[]"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"group","type":"address"}],"name":"getTotalVotesForGroup","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"group","type":"address"},{"internalType":"address","name":"account","type":"address"}],"name":"getTotalVotesForGroupByAccount","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"bytes","name":"header","type":"bytes"}],"name":"getVerifiedSealBitmapFromHeader","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getVersionNumber","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"pure","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"account","type":"address"},{"internalType":"address","name":"group","type":"address"}],"name":"hasActivatablePendingVotes","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"bytes","name":"header","type":"bytes"}],"name":"hashHeader","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"registryAddress","type":"address"},{"internalType":"uint256","name":"minElectableValidators","type":"uint256"},{"internalType":"uint256","name":"maxElectableValidators","type":"uint256"},{"internalType":"uint256","name":"_maxNumGroupsVotedFor","type":"uint256"},{"internalType":"uint256","name":"_electabilityThreshold","type":"uint256"}],"name":"initialize","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"initialized","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"isOwner","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"group","type":"address"},{"internalType":"address","name":"lesser","type":"address"},{"internalType":"address","name":"greater","type":"address"}],"name":"markGroupEligible","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"group","type":"address"}],"name":"markGroupIneligible","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"maxNumGroupsVotedFor","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"blockNumber","type":"uint256"}],"name":"minQuorumSize","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"minQuorumSizeInCurrentSet","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"numberValidatorsInCurrentSet","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"blockNumber","type":"uint256"}],"name":"numberValidatorsInSet","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"registry","outputs":[{"internalType":"contract IRegistry","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[],"name":"renounceOwnership","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"group","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"address","name":"lesser","type":"address"},{"internalType":"address","name":"greater","type":"address"},{"internalType":"uint256","name":"index","type":"uint256"}],"name":"revokeActive","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"group","type":"address"},{"internalType":"address","name":"lesser","type":"address"},{"internalType":"address","name":"greater","type":"address"},{"internalType":"uint256","name":"index","type":"uint256"}],"name":"revokeAllActive","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"group","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"address","name":"lesser","type":"address"},{"internalType":"address","name":"greater","type":"address"},{"internalType":"uint256","name":"index","type":"uint256"}],"name":"revokePending","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"bool","name":"flag","type":"bool"}],"name":"setAllowedToVoteOverMaxNumberOfGroups","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"threshold","type":"uint256"}],"name":"setElectabilityThreshold","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"min","type":"uint256"},{"internalType":"uint256","name":"max","type":"uint256"}],"name":"setElectableValidators","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"_maxNumGroupsVotedFor","type":"uint256"}],"name":"setMaxNumGroupsVotedFor","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"registryAddress","type":"address"}],"name":"setRegistry","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"account","type":"address"},{"internalType":"address","name":"group","type":"address"}],"name":"updateTotalVotesByAccountForGroup","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"index","type":"uint256"}],"name":"validatorSignerAddressFromCurrentSet","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"index","type":"uint256"},{"internalType":"uint256","name":"blockNumber","type":"uint256"}],"name":"validatorSignerAddressFromSet","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"group","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"address","name":"lesser","type":"address"},{"internalType":"address","name":"greater","type":"address"}],"name":"vote","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"}]
') as abi

union all 
select '0x471ece3750da237f93b8e339c536989b8978a438' as contract_address, 'celo' as chain, 'Contract' as abi_type, parse_json('
[{"inputs":[{"internalType":"bool","name":"test","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"spender","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"registryAddress","type":"address"}],"name":"RegistrySet","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"string","name":"comment","type":"string"}],"name":"TransferComment","type":"event"},{"constant":true,"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"owner","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"value","type":"uint256"}],"name":"burn","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"circulatingSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"decreaseAllowance","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"getBurnedAmount","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getVersionNumber","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"pure","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"increaseAllowance","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"increaseSupply","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"registryAddress","type":"address"}],"name":"initialize","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"initialized","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"isOwner","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"mint","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"registry","outputs":[{"internalType":"contract IRegistry","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[],"name":"renounceOwnership","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"registryAddress","type":"address"}],"name":"setRegistry","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"string","name":"comment","type":"string"}],"name":"transferWithComment","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"}]
') as abi

union all 
select '0x471ece3750da237f93b8e339c536989b8978a438' as contract_address, 'celo' as chain, 'Contract' as abi_type, parse_json('
[{"inputs":[{"internalType":"bool","name":"test","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"spender","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"registryAddress","type":"address"}],"name":"RegistrySet","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"string","name":"comment","type":"string"}],"name":"TransferComment","type":"event"},{"constant":true,"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"owner","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"value","type":"uint256"}],"name":"burn","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"circulatingSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"decreaseAllowance","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"getBurnedAmount","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"getVersionNumber","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"pure","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"increaseAllowance","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"increaseSupply","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"registryAddress","type":"address"}],"name":"initialize","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"initialized","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"isOwner","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"mint","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"registry","outputs":[{"internalType":"contract IRegistry","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[],"name":"renounceOwnership","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"registryAddress","type":"address"}],"name":"setRegistry","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"string","name":"comment","type":"string"}],"name":"transferWithComment","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"}]
') as abi
union all 
select '0x3baD7AD0728f9917d1Bf08af5782dCbD516cDd96' as contract_address, 'soneium' as chain, 'Contract' as abi_type, parse_json('
[{"inputs":[{"internalType":"address","name":"_wrappedNativeTokenAddress","type":"address"},{"internalType":"uint32","name":"_depositQuoteTimeBuffer","type":"uint32"},{"internalType":"uint32","name":"_fillDeadlineBuffer","type":"uint32"}],"stateMutability":"nonpayable","type":"constructor"},{"inputs":[],"name":"ClaimedMerkleLeaf","type":"error"},{"inputs":[],"name":"DepositsArePaused","type":"error"},{"inputs":[],"name":"DisabledRoute","type":"error"},{"inputs":[],"name":"ExpiredFillDeadline","type":"error"},{"inputs":[],"name":"FillsArePaused","type":"error"},{"inputs":[],"name":"InvalidChainId","type":"error"},{"inputs":[],"name":"InvalidCrossDomainAdmin","type":"error"},{"inputs":[],"name":"InvalidDepositorSignature","type":"error"},{"inputs":[],"name":"InvalidExclusiveRelayer","type":"error"},{"inputs":[],"name":"InvalidExclusivityDeadline","type":"error"},{"inputs":[],"name":"InvalidFillDeadline","type":"error"},{"inputs":[],"name":"InvalidHubPool","type":"error"},{"inputs":[],"name":"InvalidMerkleLeaf","type":"error"},{"inputs":[],"name":"InvalidMerkleProof","type":"error"},{"inputs":[],"name":"InvalidPayoutAdjustmentPct","type":"error"},{"inputs":[],"name":"InvalidQuoteTimestamp","type":"error"},{"inputs":[],"name":"InvalidRelayerFeePct","type":"error"},{"inputs":[],"name":"InvalidSlowFillRequest","type":"error"},{"inputs":[],"name":"MaxTransferSizeExceeded","type":"error"},{"inputs":[],"name":"MsgValueDoesNotMatchInputAmount","type":"error"},{"inputs":[],"name":"NoSlowFillsInExclusivityWindow","type":"error"},{"inputs":[],"name":"NotEOA","type":"error"},{"inputs":[],"name":"NotExclusiveRelayer","type":"error"},{"inputs":[],"name":"RelayFilled","type":"error"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"previousAdmin","type":"address"},{"indexed":false,"internalType":"address","name":"newAdmin","type":"address"}],"name":"AdminChanged","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"beacon","type":"address"}],"name":"BeaconUpgraded","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint256","name":"rootBundleId","type":"uint256"}],"name":"EmergencyDeleteRootBundle","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"originToken","type":"address"},{"indexed":true,"internalType":"uint256","name":"destinationChainId","type":"uint256"},{"indexed":false,"internalType":"bool","name":"enabled","type":"bool"}],"name":"EnabledDepositRoute","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"amountToReturn","type":"uint256"},{"indexed":true,"internalType":"uint256","name":"chainId","type":"uint256"},{"indexed":false,"internalType":"uint256[]","name":"refundAmounts","type":"uint256[]"},{"indexed":true,"internalType":"uint32","name":"rootBundleId","type":"uint32"},{"indexed":true,"internalType":"uint32","name":"leafId","type":"uint32"},{"indexed":false,"internalType":"address","name":"l2TokenAddress","type":"address"},{"indexed":false,"internalType":"address[]","name":"refundAddresses","type":"address[]"},{"indexed":false,"internalType":"address","name":"caller","type":"address"}],"name":"ExecutedRelayerRefundRoot","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"totalFilledAmount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"fillAmount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"repaymentChainId","type":"uint256"},{"indexed":true,"internalType":"uint256","name":"originChainId","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"destinationChainId","type":"uint256"},{"indexed":false,"internalType":"int64","name":"relayerFeePct","type":"int64"},{"indexed":false,"internalType":"int64","name":"realizedLpFeePct","type":"int64"},{"indexed":true,"internalType":"uint32","name":"depositId","type":"uint32"},{"indexed":false,"internalType":"address","name":"destinationToken","type":"address"},{"indexed":false,"internalType":"address","name":"relayer","type":"address"},{"indexed":true,"internalType":"address","name":"depositor","type":"address"},{"indexed":false,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"bytes","name":"message","type":"bytes"},{"components":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"bytes","name":"message","type":"bytes"},{"internalType":"int64","name":"relayerFeePct","type":"int64"},{"internalType":"bool","name":"isSlowRelay","type":"bool"},{"internalType":"int256","name":"payoutAdjustmentPct","type":"int256"}],"indexed":false,"internalType":"struct SpokePool.RelayExecutionInfo","name":"updatableRelayData","type":"tuple"}],"name":"FilledRelay","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"inputToken","type":"address"},{"indexed":false,"internalType":"address","name":"outputToken","type":"address"},{"indexed":false,"internalType":"uint256","name":"inputAmount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"outputAmount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"repaymentChainId","type":"uint256"},{"indexed":true,"internalType":"uint256","name":"originChainId","type":"uint256"},{"indexed":true,"internalType":"uint32","name":"depositId","type":"uint32"},{"indexed":false,"internalType":"uint32","name":"fillDeadline","type":"uint32"},{"indexed":false,"internalType":"uint32","name":"exclusivityDeadline","type":"uint32"},{"indexed":false,"internalType":"address","name":"exclusiveRelayer","type":"address"},{"indexed":true,"internalType":"address","name":"relayer","type":"address"},{"indexed":false,"internalType":"address","name":"depositor","type":"address"},{"indexed":false,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"bytes","name":"message","type":"bytes"},{"components":[{"internalType":"address","name":"updatedRecipient","type":"address"},{"internalType":"bytes","name":"updatedMessage","type":"bytes"},{"internalType":"uint256","name":"updatedOutputAmount","type":"uint256"},{"internalType":"enum V3SpokePoolInterface.FillType","name":"fillType","type":"uint8"}],"indexed":false,"internalType":"struct V3SpokePoolInterface.V3RelayExecutionEventInfo","name":"relayExecutionInfo","type":"tuple"}],"name":"FilledV3Relay","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"originChainId","type":"uint256"},{"indexed":true,"internalType":"uint256","name":"destinationChainId","type":"uint256"},{"indexed":false,"internalType":"int64","name":"relayerFeePct","type":"int64"},{"indexed":true,"internalType":"uint32","name":"depositId","type":"uint32"},{"indexed":false,"internalType":"uint32","name":"quoteTimestamp","type":"uint32"},{"indexed":false,"internalType":"address","name":"originToken","type":"address"},{"indexed":false,"internalType":"address","name":"recipient","type":"address"},{"indexed":true,"internalType":"address","name":"depositor","type":"address"},{"indexed":false,"internalType":"bytes","name":"message","type":"bytes"}],"name":"FundsDeposited","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint8","name":"version","type":"uint8"}],"name":"Initialized","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"bool","name":"isPaused","type":"bool"}],"name":"PausedDeposits","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"bool","name":"isPaused","type":"bool"}],"name":"PausedFills","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint32","name":"rootBundleId","type":"uint32"},{"indexed":true,"internalType":"bytes32","name":"relayerRefundRoot","type":"bytes32"},{"indexed":true,"internalType":"bytes32","name":"slowRelayRoot","type":"bytes32"}],"name":"RelayedRootBundle","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"int64","name":"newRelayerFeePct","type":"int64"},{"indexed":true,"internalType":"uint32","name":"depositId","type":"uint32"},{"indexed":true,"internalType":"address","name":"depositor","type":"address"},{"indexed":false,"internalType":"address","name":"updatedRecipient","type":"address"},{"indexed":false,"internalType":"bytes","name":"updatedMessage","type":"bytes"},{"indexed":false,"internalType":"bytes","name":"depositorSignature","type":"bytes"}],"name":"RequestedSpeedUpDeposit","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"updatedOutputAmount","type":"uint256"},{"indexed":true,"internalType":"uint32","name":"depositId","type":"uint32"},{"indexed":true,"internalType":"address","name":"depositor","type":"address"},{"indexed":false,"internalType":"address","name":"updatedRecipient","type":"address"},{"indexed":false,"internalType":"bytes","name":"updatedMessage","type":"bytes"},{"indexed":false,"internalType":"bytes","name":"depositorSignature","type":"bytes"}],"name":"RequestedSpeedUpV3Deposit","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"inputToken","type":"address"},{"indexed":false,"internalType":"address","name":"outputToken","type":"address"},{"indexed":false,"internalType":"uint256","name":"inputAmount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"outputAmount","type":"uint256"},{"indexed":true,"internalType":"uint256","name":"originChainId","type":"uint256"},{"indexed":true,"internalType":"uint32","name":"depositId","type":"uint32"},{"indexed":false,"internalType":"uint32","name":"fillDeadline","type":"uint32"},{"indexed":false,"internalType":"uint32","name":"exclusivityDeadline","type":"uint32"},{"indexed":false,"internalType":"address","name":"exclusiveRelayer","type":"address"},{"indexed":false,"internalType":"address","name":"depositor","type":"address"},{"indexed":false,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"bytes","name":"message","type":"bytes"}],"name":"RequestedV3SlowFill","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"newHubPool","type":"address"}],"name":"SetHubPool","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"newAdmin","type":"address"}],"name":"SetXDomainAdmin","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"amountToReturn","type":"uint256"},{"indexed":true,"internalType":"uint256","name":"chainId","type":"uint256"},{"indexed":true,"internalType":"uint32","name":"leafId","type":"uint32"},{"indexed":true,"internalType":"address","name":"l2TokenAddress","type":"address"},{"indexed":false,"internalType":"address","name":"caller","type":"address"}],"name":"TokensBridged","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"implementation","type":"address"}],"name":"Upgraded","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"inputToken","type":"address"},{"indexed":false,"internalType":"address","name":"outputToken","type":"address"},{"indexed":false,"internalType":"uint256","name":"inputAmount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"outputAmount","type":"uint256"},{"indexed":true,"internalType":"uint256","name":"destinationChainId","type":"uint256"},{"indexed":true,"internalType":"uint32","name":"depositId","type":"uint32"},{"indexed":false,"internalType":"uint32","name":"quoteTimestamp","type":"uint32"},{"indexed":false,"internalType":"uint32","name":"fillDeadline","type":"uint32"},{"indexed":false,"internalType":"uint32","name":"exclusivityDeadline","type":"uint32"},{"indexed":true,"internalType":"address","name":"depositor","type":"address"},{"indexed":false,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"address","name":"exclusiveRelayer","type":"address"},{"indexed":false,"internalType":"bytes","name":"message","type":"bytes"}],"name":"V3FundsDeposited","type":"event"},{"inputs":[],"name":"EMPTY_RELAYER","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"EMPTY_REPAYMENT_CHAIN_ID","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"INFINITE_FILL_DEADLINE","outputs":[{"internalType":"uint32","name":"","type":"uint32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MAX_TRANSFER_SIZE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"UPDATE_V3_DEPOSIT_DETAILS_HASH","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint32","name":"_initialDepositId","type":"uint32"},{"internalType":"address","name":"_crossDomainAdmin","type":"address"},{"internalType":"address","name":"_hubPool","type":"address"}],"name":"__SpokePool_init","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"chainId","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"crossDomainAdmin","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"address","name":"originToken","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"uint256","name":"destinationChainId","type":"uint256"},{"internalType":"int64","name":"relayerFeePct","type":"int64"},{"internalType":"uint32","name":"quoteTimestamp","type":"uint32"},{"internalType":"bytes","name":"message","type":"bytes"},{"internalType":"uint256","name":"","type":"uint256"}],"name":"deposit","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"depositor","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"address","name":"inputToken","type":"address"},{"internalType":"address","name":"outputToken","type":"address"},{"internalType":"uint256","name":"inputAmount","type":"uint256"},{"internalType":"uint256","name":"outputAmount","type":"uint256"},{"internalType":"uint256","name":"destinationChainId","type":"uint256"},{"internalType":"address","name":"exclusiveRelayer","type":"address"},{"internalType":"uint32","name":"quoteTimestamp","type":"uint32"},{"internalType":"uint32","name":"fillDeadline","type":"uint32"},{"internalType":"uint32","name":"exclusivityDeadlineOffset","type":"uint32"},{"internalType":"bytes","name":"message","type":"bytes"}],"name":"depositExclusive","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"depositor","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"address","name":"originToken","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"uint256","name":"destinationChainId","type":"uint256"},{"internalType":"int64","name":"relayerFeePct","type":"int64"},{"internalType":"uint32","name":"quoteTimestamp","type":"uint32"},{"internalType":"bytes","name":"message","type":"bytes"},{"internalType":"uint256","name":"","type":"uint256"}],"name":"depositFor","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[],"name":"depositQuoteTimeBuffer","outputs":[{"internalType":"uint32","name":"","type":"uint32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"depositor","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"address","name":"inputToken","type":"address"},{"internalType":"address","name":"outputToken","type":"address"},{"internalType":"uint256","name":"inputAmount","type":"uint256"},{"internalType":"uint256","name":"outputAmount","type":"uint256"},{"internalType":"uint256","name":"destinationChainId","type":"uint256"},{"internalType":"address","name":"exclusiveRelayer","type":"address"},{"internalType":"uint32","name":"quoteTimestamp","type":"uint32"},{"internalType":"uint32","name":"fillDeadline","type":"uint32"},{"internalType":"uint32","name":"exclusivityDeadline","type":"uint32"},{"internalType":"bytes","name":"message","type":"bytes"}],"name":"depositV3","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"depositor","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"address","name":"inputToken","type":"address"},{"internalType":"address","name":"outputToken","type":"address"},{"internalType":"uint256","name":"inputAmount","type":"uint256"},{"internalType":"uint256","name":"outputAmount","type":"uint256"},{"internalType":"uint256","name":"destinationChainId","type":"uint256"},{"internalType":"address","name":"exclusiveRelayer","type":"address"},{"internalType":"uint32","name":"fillDeadlineOffset","type":"uint32"},{"internalType":"uint32","name":"exclusivityDeadline","type":"uint32"},{"internalType":"bytes","name":"message","type":"bytes"}],"name":"depositV3Now","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"rootBundleId","type":"uint256"}],"name":"emergencyDeleteRootBundle","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"uint256","name":"","type":"uint256"}],"name":"enabledDepositRoutes","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint32","name":"rootBundleId","type":"uint32"},{"components":[{"internalType":"uint256","name":"amountToReturn","type":"uint256"},{"internalType":"uint256","name":"chainId","type":"uint256"},{"internalType":"uint256[]","name":"refundAmounts","type":"uint256[]"},{"internalType":"uint32","name":"leafId","type":"uint32"},{"internalType":"address","name":"l2TokenAddress","type":"address"},{"internalType":"address[]","name":"refundAddresses","type":"address[]"}],"internalType":"struct SpokePoolInterface.RelayerRefundLeaf","name":"relayerRefundLeaf","type":"tuple"},{"internalType":"bytes32[]","name":"proof","type":"bytes32[]"}],"name":"executeRelayerRefundLeaf","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"components":[{"components":[{"internalType":"address","name":"depositor","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"address","name":"exclusiveRelayer","type":"address"},{"internalType":"address","name":"inputToken","type":"address"},{"internalType":"address","name":"outputToken","type":"address"},{"internalType":"uint256","name":"inputAmount","type":"uint256"},{"internalType":"uint256","name":"outputAmount","type":"uint256"},{"internalType":"uint256","name":"originChainId","type":"uint256"},{"internalType":"uint32","name":"depositId","type":"uint32"},{"internalType":"uint32","name":"fillDeadline","type":"uint32"},{"internalType":"uint32","name":"exclusivityDeadline","type":"uint32"},{"internalType":"bytes","name":"message","type":"bytes"}],"internalType":"struct V3SpokePoolInterface.V3RelayData","name":"relayData","type":"tuple"},{"internalType":"uint256","name":"chainId","type":"uint256"},{"internalType":"uint256","name":"updatedOutputAmount","type":"uint256"}],"internalType":"struct V3SpokePoolInterface.V3SlowFill","name":"slowFillLeaf","type":"tuple"},{"internalType":"uint32","name":"rootBundleId","type":"uint32"},{"internalType":"bytes32[]","name":"proof","type":"bytes32[]"}],"name":"executeV3SlowRelayLeaf","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"fillDeadlineBuffer","outputs":[{"internalType":"uint32","name":"","type":"uint32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"name":"fillStatuses","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"depositor","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"address","name":"exclusiveRelayer","type":"address"},{"internalType":"address","name":"inputToken","type":"address"},{"internalType":"address","name":"outputToken","type":"address"},{"internalType":"uint256","name":"inputAmount","type":"uint256"},{"internalType":"uint256","name":"outputAmount","type":"uint256"},{"internalType":"uint256","name":"originChainId","type":"uint256"},{"internalType":"uint32","name":"depositId","type":"uint32"},{"internalType":"uint32","name":"fillDeadline","type":"uint32"},{"internalType":"uint32","name":"exclusivityDeadline","type":"uint32"},{"internalType":"bytes","name":"message","type":"bytes"}],"internalType":"struct V3SpokePoolInterface.V3RelayData","name":"relayData","type":"tuple"},{"internalType":"uint256","name":"repaymentChainId","type":"uint256"}],"name":"fillV3Relay","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"depositor","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"address","name":"exclusiveRelayer","type":"address"},{"internalType":"address","name":"inputToken","type":"address"},{"internalType":"address","name":"outputToken","type":"address"},{"internalType":"uint256","name":"inputAmount","type":"uint256"},{"internalType":"uint256","name":"outputAmount","type":"uint256"},{"internalType":"uint256","name":"originChainId","type":"uint256"},{"internalType":"uint32","name":"depositId","type":"uint32"},{"internalType":"uint32","name":"fillDeadline","type":"uint32"},{"internalType":"uint32","name":"exclusivityDeadline","type":"uint32"},{"internalType":"bytes","name":"message","type":"bytes"}],"internalType":"struct V3SpokePoolInterface.V3RelayData","name":"relayData","type":"tuple"},{"internalType":"uint256","name":"repaymentChainId","type":"uint256"},{"internalType":"uint256","name":"updatedOutputAmount","type":"uint256"},{"internalType":"address","name":"updatedRecipient","type":"address"},{"internalType":"bytes","name":"updatedMessage","type":"bytes"},{"internalType":"bytes","name":"depositorSignature","type":"bytes"}],"name":"fillV3RelayWithUpdatedDeposit","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"getCurrentTime","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"hubPool","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint32","name":"_initialDepositId","type":"uint32"},{"internalType":"address","name":"_hubPool","type":"address"}],"name":"initialize","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bytes[]","name":"data","type":"bytes[]"}],"name":"multicall","outputs":[{"internalType":"bytes[]","name":"results","type":"bytes[]"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"numberOfDeposits","outputs":[{"internalType":"uint32","name":"","type":"uint32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bool","name":"pause","type":"bool"}],"name":"pauseDeposits","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bool","name":"pause","type":"bool"}],"name":"pauseFills","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"pausedDeposits","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"pausedFills","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"proxiableUUID","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes32","name":"relayerRefundRoot","type":"bytes32"},{"internalType":"bytes32","name":"slowRelayRoot","type":"bytes32"}],"name":"relayRootBundle","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"renounceOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"depositor","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"address","name":"exclusiveRelayer","type":"address"},{"internalType":"address","name":"inputToken","type":"address"},{"internalType":"address","name":"outputToken","type":"address"},{"internalType":"uint256","name":"inputAmount","type":"uint256"},{"internalType":"uint256","name":"outputAmount","type":"uint256"},{"internalType":"uint256","name":"originChainId","type":"uint256"},{"internalType":"uint32","name":"depositId","type":"uint32"},{"internalType":"uint32","name":"fillDeadline","type":"uint32"},{"internalType":"uint32","name":"exclusivityDeadline","type":"uint32"},{"internalType":"bytes","name":"message","type":"bytes"}],"internalType":"struct V3SpokePoolInterface.V3RelayData","name":"relayData","type":"tuple"}],"name":"requestV3SlowFill","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"rootBundles","outputs":[{"internalType":"bytes32","name":"slowRelayRoot","type":"bytes32"},{"internalType":"bytes32","name":"relayerRefundRoot","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"newCrossDomainAdmin","type":"address"}],"name":"setCrossDomainAdmin","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"originToken","type":"address"},{"internalType":"uint256","name":"destinationChainId","type":"uint256"},{"internalType":"bool","name":"enabled","type":"bool"}],"name":"setEnableRoute","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newHubPool","type":"address"}],"name":"setHubPool","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"depositor","type":"address"},{"internalType":"uint32","name":"depositId","type":"uint32"},{"internalType":"uint256","name":"updatedOutputAmount","type":"uint256"},{"internalType":"address","name":"updatedRecipient","type":"address"},{"internalType":"bytes","name":"updatedMessage","type":"bytes"},{"internalType":"bytes","name":"depositorSignature","type":"bytes"}],"name":"speedUpV3Deposit","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bytes[]","name":"data","type":"bytes[]"}],"name":"tryMulticall","outputs":[{"components":[{"internalType":"bool","name":"success","type":"bool"},{"internalType":"bytes","name":"returnData","type":"bytes"}],"internalType":"struct MultiCallerUpgradeable.Result[]","name":"results","type":"tuple[]"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newImplementation","type":"address"}],"name":"upgradeTo","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newImplementation","type":"address"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"upgradeToAndCall","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[],"name":"wrappedNativeToken","outputs":[{"internalType":"contract WETH9Interface","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"stateMutability":"payable","type":"receive"}]
') as abi
union all
select '0xeF684C38F94F48775959ECf2012D7E864ffb9dd4' as contract_address, 'ink' as chain, 'Contract' as abi_type, parse_json('
[{"inputs":[{"internalType":"address","name":"_wrappedNativeTokenAddress","type":"address"},{"internalType":"uint32","name":"_depositQuoteTimeBuffer","type":"uint32"},{"internalType":"uint32","name":"_fillDeadlineBuffer","type":"uint32"}],"stateMutability":"nonpayable","type":"constructor"},{"inputs":[],"name":"ClaimedMerkleLeaf","type":"error"},{"inputs":[],"name":"DepositsArePaused","type":"error"},{"inputs":[],"name":"DisabledRoute","type":"error"},{"inputs":[],"name":"ExpiredFillDeadline","type":"error"},{"inputs":[],"name":"FillsArePaused","type":"error"},{"inputs":[],"name":"InvalidChainId","type":"error"},{"inputs":[],"name":"InvalidCrossDomainAdmin","type":"error"},{"inputs":[],"name":"InvalidDepositorSignature","type":"error"},{"inputs":[],"name":"InvalidExclusiveRelayer","type":"error"},{"inputs":[],"name":"InvalidExclusivityDeadline","type":"error"},{"inputs":[],"name":"InvalidFillDeadline","type":"error"},{"inputs":[],"name":"InvalidHubPool","type":"error"},{"inputs":[],"name":"InvalidMerkleLeaf","type":"error"},{"inputs":[],"name":"InvalidMerkleProof","type":"error"},{"inputs":[],"name":"InvalidPayoutAdjustmentPct","type":"error"},{"inputs":[],"name":"InvalidQuoteTimestamp","type":"error"},{"inputs":[],"name":"InvalidRelayerFeePct","type":"error"},{"inputs":[],"name":"InvalidSlowFillRequest","type":"error"},{"inputs":[],"name":"MaxTransferSizeExceeded","type":"error"},{"inputs":[],"name":"MsgValueDoesNotMatchInputAmount","type":"error"},{"inputs":[],"name":"NoSlowFillsInExclusivityWindow","type":"error"},{"inputs":[],"name":"NotEOA","type":"error"},{"inputs":[],"name":"NotExclusiveRelayer","type":"error"},{"inputs":[],"name":"RelayFilled","type":"error"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"previousAdmin","type":"address"},{"indexed":false,"internalType":"address","name":"newAdmin","type":"address"}],"name":"AdminChanged","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"beacon","type":"address"}],"name":"BeaconUpgraded","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint256","name":"rootBundleId","type":"uint256"}],"name":"EmergencyDeleteRootBundle","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"originToken","type":"address"},{"indexed":true,"internalType":"uint256","name":"destinationChainId","type":"uint256"},{"indexed":false,"internalType":"bool","name":"enabled","type":"bool"}],"name":"EnabledDepositRoute","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"amountToReturn","type":"uint256"},{"indexed":true,"internalType":"uint256","name":"chainId","type":"uint256"},{"indexed":false,"internalType":"uint256[]","name":"refundAmounts","type":"uint256[]"},{"indexed":true,"internalType":"uint32","name":"rootBundleId","type":"uint32"},{"indexed":true,"internalType":"uint32","name":"leafId","type":"uint32"},{"indexed":false,"internalType":"address","name":"l2TokenAddress","type":"address"},{"indexed":false,"internalType":"address[]","name":"refundAddresses","type":"address[]"},{"indexed":false,"internalType":"address","name":"caller","type":"address"}],"name":"ExecutedRelayerRefundRoot","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"totalFilledAmount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"fillAmount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"repaymentChainId","type":"uint256"},{"indexed":true,"internalType":"uint256","name":"originChainId","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"destinationChainId","type":"uint256"},{"indexed":false,"internalType":"int64","name":"relayerFeePct","type":"int64"},{"indexed":false,"internalType":"int64","name":"realizedLpFeePct","type":"int64"},{"indexed":true,"internalType":"uint32","name":"depositId","type":"uint32"},{"indexed":false,"internalType":"address","name":"destinationToken","type":"address"},{"indexed":false,"internalType":"address","name":"relayer","type":"address"},{"indexed":true,"internalType":"address","name":"depositor","type":"address"},{"indexed":false,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"bytes","name":"message","type":"bytes"},{"components":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"bytes","name":"message","type":"bytes"},{"internalType":"int64","name":"relayerFeePct","type":"int64"},{"internalType":"bool","name":"isSlowRelay","type":"bool"},{"internalType":"int256","name":"payoutAdjustmentPct","type":"int256"}],"indexed":false,"internalType":"struct SpokePool.RelayExecutionInfo","name":"updatableRelayData","type":"tuple"}],"name":"FilledRelay","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"inputToken","type":"address"},{"indexed":false,"internalType":"address","name":"outputToken","type":"address"},{"indexed":false,"internalType":"uint256","name":"inputAmount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"outputAmount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"repaymentChainId","type":"uint256"},{"indexed":true,"internalType":"uint256","name":"originChainId","type":"uint256"},{"indexed":true,"internalType":"uint32","name":"depositId","type":"uint32"},{"indexed":false,"internalType":"uint32","name":"fillDeadline","type":"uint32"},{"indexed":false,"internalType":"uint32","name":"exclusivityDeadline","type":"uint32"},{"indexed":false,"internalType":"address","name":"exclusiveRelayer","type":"address"},{"indexed":true,"internalType":"address","name":"relayer","type":"address"},{"indexed":false,"internalType":"address","name":"depositor","type":"address"},{"indexed":false,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"bytes","name":"message","type":"bytes"},{"components":[{"internalType":"address","name":"updatedRecipient","type":"address"},{"internalType":"bytes","name":"updatedMessage","type":"bytes"},{"internalType":"uint256","name":"updatedOutputAmount","type":"uint256"},{"internalType":"enum V3SpokePoolInterface.FillType","name":"fillType","type":"uint8"}],"indexed":false,"internalType":"struct V3SpokePoolInterface.V3RelayExecutionEventInfo","name":"relayExecutionInfo","type":"tuple"}],"name":"FilledV3Relay","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"originChainId","type":"uint256"},{"indexed":true,"internalType":"uint256","name":"destinationChainId","type":"uint256"},{"indexed":false,"internalType":"int64","name":"relayerFeePct","type":"int64"},{"indexed":true,"internalType":"uint32","name":"depositId","type":"uint32"},{"indexed":false,"internalType":"uint32","name":"quoteTimestamp","type":"uint32"},{"indexed":false,"internalType":"address","name":"originToken","type":"address"},{"indexed":false,"internalType":"address","name":"recipient","type":"address"},{"indexed":true,"internalType":"address","name":"depositor","type":"address"},{"indexed":false,"internalType":"bytes","name":"message","type":"bytes"}],"name":"FundsDeposited","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint8","name":"version","type":"uint8"}],"name":"Initialized","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"bool","name":"isPaused","type":"bool"}],"name":"PausedDeposits","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"bool","name":"isPaused","type":"bool"}],"name":"PausedFills","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint32","name":"rootBundleId","type":"uint32"},{"indexed":true,"internalType":"bytes32","name":"relayerRefundRoot","type":"bytes32"},{"indexed":true,"internalType":"bytes32","name":"slowRelayRoot","type":"bytes32"}],"name":"RelayedRootBundle","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"int64","name":"newRelayerFeePct","type":"int64"},{"indexed":true,"internalType":"uint32","name":"depositId","type":"uint32"},{"indexed":true,"internalType":"address","name":"depositor","type":"address"},{"indexed":false,"internalType":"address","name":"updatedRecipient","type":"address"},{"indexed":false,"internalType":"bytes","name":"updatedMessage","type":"bytes"},{"indexed":false,"internalType":"bytes","name":"depositorSignature","type":"bytes"}],"name":"RequestedSpeedUpDeposit","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"updatedOutputAmount","type":"uint256"},{"indexed":true,"internalType":"uint32","name":"depositId","type":"uint32"},{"indexed":true,"internalType":"address","name":"depositor","type":"address"},{"indexed":false,"internalType":"address","name":"updatedRecipient","type":"address"},{"indexed":false,"internalType":"bytes","name":"updatedMessage","type":"bytes"},{"indexed":false,"internalType":"bytes","name":"depositorSignature","type":"bytes"}],"name":"RequestedSpeedUpV3Deposit","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"inputToken","type":"address"},{"indexed":false,"internalType":"address","name":"outputToken","type":"address"},{"indexed":false,"internalType":"uint256","name":"inputAmount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"outputAmount","type":"uint256"},{"indexed":true,"internalType":"uint256","name":"originChainId","type":"uint256"},{"indexed":true,"internalType":"uint32","name":"depositId","type":"uint32"},{"indexed":false,"internalType":"uint32","name":"fillDeadline","type":"uint32"},{"indexed":false,"internalType":"uint32","name":"exclusivityDeadline","type":"uint32"},{"indexed":false,"internalType":"address","name":"exclusiveRelayer","type":"address"},{"indexed":false,"internalType":"address","name":"depositor","type":"address"},{"indexed":false,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"bytes","name":"message","type":"bytes"}],"name":"RequestedV3SlowFill","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"newHubPool","type":"address"}],"name":"SetHubPool","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"newAdmin","type":"address"}],"name":"SetXDomainAdmin","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"amountToReturn","type":"uint256"},{"indexed":true,"internalType":"uint256","name":"chainId","type":"uint256"},{"indexed":true,"internalType":"uint32","name":"leafId","type":"uint32"},{"indexed":true,"internalType":"address","name":"l2TokenAddress","type":"address"},{"indexed":false,"internalType":"address","name":"caller","type":"address"}],"name":"TokensBridged","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"implementation","type":"address"}],"name":"Upgraded","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"inputToken","type":"address"},{"indexed":false,"internalType":"address","name":"outputToken","type":"address"},{"indexed":false,"internalType":"uint256","name":"inputAmount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"outputAmount","type":"uint256"},{"indexed":true,"internalType":"uint256","name":"destinationChainId","type":"uint256"},{"indexed":true,"internalType":"uint32","name":"depositId","type":"uint32"},{"indexed":false,"internalType":"uint32","name":"quoteTimestamp","type":"uint32"},{"indexed":false,"internalType":"uint32","name":"fillDeadline","type":"uint32"},{"indexed":false,"internalType":"uint32","name":"exclusivityDeadline","type":"uint32"},{"indexed":true,"internalType":"address","name":"depositor","type":"address"},{"indexed":false,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"address","name":"exclusiveRelayer","type":"address"},{"indexed":false,"internalType":"bytes","name":"message","type":"bytes"}],"name":"V3FundsDeposited","type":"event"},{"inputs":[],"name":"EMPTY_RELAYER","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"EMPTY_REPAYMENT_CHAIN_ID","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"INFINITE_FILL_DEADLINE","outputs":[{"internalType":"uint32","name":"","type":"uint32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MAX_TRANSFER_SIZE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"UPDATE_V3_DEPOSIT_DETAILS_HASH","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint32","name":"_initialDepositId","type":"uint32"},{"internalType":"address","name":"_crossDomainAdmin","type":"address"},{"internalType":"address","name":"_hubPool","type":"address"}],"name":"__SpokePool_init","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"chainId","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"crossDomainAdmin","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"address","name":"originToken","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"uint256","name":"destinationChainId","type":"uint256"},{"internalType":"int64","name":"relayerFeePct","type":"int64"},{"internalType":"uint32","name":"quoteTimestamp","type":"uint32"},{"internalType":"bytes","name":"message","type":"bytes"},{"internalType":"uint256","name":"","type":"uint256"}],"name":"deposit","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"depositor","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"address","name":"inputToken","type":"address"},{"internalType":"address","name":"outputToken","type":"address"},{"internalType":"uint256","name":"inputAmount","type":"uint256"},{"internalType":"uint256","name":"outputAmount","type":"uint256"},{"internalType":"uint256","name":"destinationChainId","type":"uint256"},{"internalType":"address","name":"exclusiveRelayer","type":"address"},{"internalType":"uint32","name":"quoteTimestamp","type":"uint32"},{"internalType":"uint32","name":"fillDeadline","type":"uint32"},{"internalType":"uint32","name":"exclusivityDeadlineOffset","type":"uint32"},{"internalType":"bytes","name":"message","type":"bytes"}],"name":"depositExclusive","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"depositor","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"address","name":"originToken","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"uint256","name":"destinationChainId","type":"uint256"},{"internalType":"int64","name":"relayerFeePct","type":"int64"},{"internalType":"uint32","name":"quoteTimestamp","type":"uint32"},{"internalType":"bytes","name":"message","type":"bytes"},{"internalType":"uint256","name":"","type":"uint256"}],"name":"depositFor","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[],"name":"depositQuoteTimeBuffer","outputs":[{"internalType":"uint32","name":"","type":"uint32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"depositor","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"address","name":"inputToken","type":"address"},{"internalType":"address","name":"outputToken","type":"address"},{"internalType":"uint256","name":"inputAmount","type":"uint256"},{"internalType":"uint256","name":"outputAmount","type":"uint256"},{"internalType":"uint256","name":"destinationChainId","type":"uint256"},{"internalType":"address","name":"exclusiveRelayer","type":"address"},{"internalType":"uint32","name":"quoteTimestamp","type":"uint32"},{"internalType":"uint32","name":"fillDeadline","type":"uint32"},{"internalType":"uint32","name":"exclusivityDeadline","type":"uint32"},{"internalType":"bytes","name":"message","type":"bytes"}],"name":"depositV3","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"address","name":"depositor","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"address","name":"inputToken","type":"address"},{"internalType":"address","name":"outputToken","type":"address"},{"internalType":"uint256","name":"inputAmount","type":"uint256"},{"internalType":"uint256","name":"outputAmount","type":"uint256"},{"internalType":"uint256","name":"destinationChainId","type":"uint256"},{"internalType":"address","name":"exclusiveRelayer","type":"address"},{"internalType":"uint32","name":"fillDeadlineOffset","type":"uint32"},{"internalType":"uint32","name":"exclusivityDeadline","type":"uint32"},{"internalType":"bytes","name":"message","type":"bytes"}],"name":"depositV3Now","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"internalType":"uint256","name":"rootBundleId","type":"uint256"}],"name":"emergencyDeleteRootBundle","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"uint256","name":"","type":"uint256"}],"name":"enabledDepositRoutes","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint32","name":"rootBundleId","type":"uint32"},{"components":[{"internalType":"uint256","name":"amountToReturn","type":"uint256"},{"internalType":"uint256","name":"chainId","type":"uint256"},{"internalType":"uint256[]","name":"refundAmounts","type":"uint256[]"},{"internalType":"uint32","name":"leafId","type":"uint32"},{"internalType":"address","name":"l2TokenAddress","type":"address"},{"internalType":"address[]","name":"refundAddresses","type":"address[]"}],"internalType":"struct SpokePoolInterface.RelayerRefundLeaf","name":"relayerRefundLeaf","type":"tuple"},{"internalType":"bytes32[]","name":"proof","type":"bytes32[]"}],"name":"executeRelayerRefundLeaf","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[{"components":[{"components":[{"internalType":"address","name":"depositor","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"address","name":"exclusiveRelayer","type":"address"},{"internalType":"address","name":"inputToken","type":"address"},{"internalType":"address","name":"outputToken","type":"address"},{"internalType":"uint256","name":"inputAmount","type":"uint256"},{"internalType":"uint256","name":"outputAmount","type":"uint256"},{"internalType":"uint256","name":"originChainId","type":"uint256"},{"internalType":"uint32","name":"depositId","type":"uint32"},{"internalType":"uint32","name":"fillDeadline","type":"uint32"},{"internalType":"uint32","name":"exclusivityDeadline","type":"uint32"},{"internalType":"bytes","name":"message","type":"bytes"}],"internalType":"struct V3SpokePoolInterface.V3RelayData","name":"relayData","type":"tuple"},{"internalType":"uint256","name":"chainId","type":"uint256"},{"internalType":"uint256","name":"updatedOutputAmount","type":"uint256"}],"internalType":"struct V3SpokePoolInterface.V3SlowFill","name":"slowFillLeaf","type":"tuple"},{"internalType":"uint32","name":"rootBundleId","type":"uint32"},{"internalType":"bytes32[]","name":"proof","type":"bytes32[]"}],"name":"executeV3SlowRelayLeaf","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"fillDeadlineBuffer","outputs":[{"internalType":"uint32","name":"","type":"uint32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"name":"fillStatuses","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"depositor","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"address","name":"exclusiveRelayer","type":"address"},{"internalType":"address","name":"inputToken","type":"address"},{"internalType":"address","name":"outputToken","type":"address"},{"internalType":"uint256","name":"inputAmount","type":"uint256"},{"internalType":"uint256","name":"outputAmount","type":"uint256"},{"internalType":"uint256","name":"originChainId","type":"uint256"},{"internalType":"uint32","name":"depositId","type":"uint32"},{"internalType":"uint32","name":"fillDeadline","type":"uint32"},{"internalType":"uint32","name":"exclusivityDeadline","type":"uint32"},{"internalType":"bytes","name":"message","type":"bytes"}],"internalType":"struct V3SpokePoolInterface.V3RelayData","name":"relayData","type":"tuple"},{"internalType":"uint256","name":"repaymentChainId","type":"uint256"}],"name":"fillV3Relay","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"depositor","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"address","name":"exclusiveRelayer","type":"address"},{"internalType":"address","name":"inputToken","type":"address"},{"internalType":"address","name":"outputToken","type":"address"},{"internalType":"uint256","name":"inputAmount","type":"uint256"},{"internalType":"uint256","name":"outputAmount","type":"uint256"},{"internalType":"uint256","name":"originChainId","type":"uint256"},{"internalType":"uint32","name":"depositId","type":"uint32"},{"internalType":"uint32","name":"fillDeadline","type":"uint32"},{"internalType":"uint32","name":"exclusivityDeadline","type":"uint32"},{"internalType":"bytes","name":"message","type":"bytes"}],"internalType":"struct V3SpokePoolInterface.V3RelayData","name":"relayData","type":"tuple"},{"internalType":"uint256","name":"repaymentChainId","type":"uint256"},{"internalType":"uint256","name":"updatedOutputAmount","type":"uint256"},{"internalType":"address","name":"updatedRecipient","type":"address"},{"internalType":"bytes","name":"updatedMessage","type":"bytes"},{"internalType":"bytes","name":"depositorSignature","type":"bytes"}],"name":"fillV3RelayWithUpdatedDeposit","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"getCurrentTime","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"hubPool","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint32","name":"_initialDepositId","type":"uint32"},{"internalType":"address","name":"_hubPool","type":"address"}],"name":"initialize","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bytes[]","name":"data","type":"bytes[]"}],"name":"multicall","outputs":[{"internalType":"bytes[]","name":"results","type":"bytes[]"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"numberOfDeposits","outputs":[{"internalType":"uint32","name":"","type":"uint32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bool","name":"pause","type":"bool"}],"name":"pauseDeposits","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bool","name":"pause","type":"bool"}],"name":"pauseFills","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"pausedDeposits","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"pausedFills","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"proxiableUUID","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes32","name":"relayerRefundRoot","type":"bytes32"},{"internalType":"bytes32","name":"slowRelayRoot","type":"bytes32"}],"name":"relayRootBundle","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"renounceOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"depositor","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"address","name":"exclusiveRelayer","type":"address"},{"internalType":"address","name":"inputToken","type":"address"},{"internalType":"address","name":"outputToken","type":"address"},{"internalType":"uint256","name":"inputAmount","type":"uint256"},{"internalType":"uint256","name":"outputAmount","type":"uint256"},{"internalType":"uint256","name":"originChainId","type":"uint256"},{"internalType":"uint32","name":"depositId","type":"uint32"},{"internalType":"uint32","name":"fillDeadline","type":"uint32"},{"internalType":"uint32","name":"exclusivityDeadline","type":"uint32"},{"internalType":"bytes","name":"message","type":"bytes"}],"internalType":"struct V3SpokePoolInterface.V3RelayData","name":"relayData","type":"tuple"}],"name":"requestV3SlowFill","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"rootBundles","outputs":[{"internalType":"bytes32","name":"slowRelayRoot","type":"bytes32"},{"internalType":"bytes32","name":"relayerRefundRoot","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"newCrossDomainAdmin","type":"address"}],"name":"setCrossDomainAdmin","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"originToken","type":"address"},{"internalType":"uint256","name":"destinationChainId","type":"uint256"},{"internalType":"bool","name":"enabled","type":"bool"}],"name":"setEnableRoute","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newHubPool","type":"address"}],"name":"setHubPool","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"depositor","type":"address"},{"internalType":"uint32","name":"depositId","type":"uint32"},{"internalType":"uint256","name":"updatedOutputAmount","type":"uint256"},{"internalType":"address","name":"updatedRecipient","type":"address"},{"internalType":"bytes","name":"updatedMessage","type":"bytes"},{"internalType":"bytes","name":"depositorSignature","type":"bytes"}],"name":"speedUpV3Deposit","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bytes[]","name":"data","type":"bytes[]"}],"name":"tryMulticall","outputs":[{"components":[{"internalType":"bool","name":"success","type":"bool"},{"internalType":"bytes","name":"returnData","type":"bytes"}],"internalType":"struct MultiCallerUpgradeable.Result[]","name":"results","type":"tuple[]"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newImplementation","type":"address"}],"name":"upgradeTo","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newImplementation","type":"address"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"upgradeToAndCall","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[],"name":"wrappedNativeToken","outputs":[{"internalType":"contract WETH9Interface","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"stateMutability":"payable","type":"receive"}]
') as abi
