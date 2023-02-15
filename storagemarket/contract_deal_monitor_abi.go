package storagemarket

import "math/big"

// DealClientDealProposal is an auto generated low-level Go binding around an user-defined struct.
type DealClientDealProposal struct {
	PieceCid             []byte
	PaddedPieceSize      uint64
	VerifiedDeal         bool
	Client               []byte
	Label                []byte
	StartEpoch           uint64
	EndEpoch             uint64
	StoragePricePerEpoch uint64
	ProviderCollateral   uint64
	ClientCollateral     uint64
	Version              string
	Params               []byte
}

type paramsRecord struct {
	LocationRef      string
	CarSize          *big.Int
	SkipIpniAnnounce bool
}

var DealClientABI = `
[
    {
      "inputs": [],
      "stateMutability": "nonpayable",
      "type": "constructor"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": true,
          "internalType": "bytes32",
          "name": "id",
          "type": "bytes32"
        }
      ],
      "name": "DealProposalCreate",
      "type": "event"
    },
    {
      "anonymous": false,
      "inputs": [
        {
          "indexed": false,
          "internalType": "string",
          "name": "received",
          "type": "string"
        }
      ],
      "name": "ReceivedDataCap",
      "type": "event"
    },
    {
      "inputs": [],
      "name": "AUTHORIZE_MESSAGE_METHOD_NUM",
      "outputs": [
        {
          "internalType": "uint64",
          "name": "",
          "type": "uint64"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "DATACAP_RECEIVER_HOOK_METHOD_NUM",
      "outputs": [
        {
          "internalType": "uint64",
          "name": "",
          "type": "uint64"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "bytes",
          "name": "cidraw",
          "type": "bytes"
        },
        {
          "internalType": "uint256",
          "name": "size",
          "type": "uint256"
        }
      ],
      "name": "addCID",
      "outputs": [],
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "bytes",
          "name": "cidraw",
          "type": "bytes"
        },
        {
          "internalType": "bytes",
          "name": "provider",
          "type": "bytes"
        },
        {
          "internalType": "uint256",
          "name": "size",
          "type": "uint256"
        }
      ],
      "name": "authorizeData",
      "outputs": [],
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "bytes",
          "name": "",
          "type": "bytes"
        },
        {
          "internalType": "bytes",
          "name": "",
          "type": "bytes"
        }
      ],
      "name": "cidProviders",
      "outputs": [
        {
          "internalType": "bool",
          "name": "",
          "type": "bool"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "bytes",
          "name": "",
          "type": "bytes"
        }
      ],
      "name": "cidSet",
      "outputs": [
        {
          "internalType": "bool",
          "name": "",
          "type": "bool"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "bytes",
          "name": "",
          "type": "bytes"
        }
      ],
      "name": "cidSizes",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "bytes32",
          "name": "id",
          "type": "bytes32"
        }
      ],
      "name": "getDealProposal",
      "outputs": [
        {
          "components": [
            {
              "internalType": "bytes",
              "name": "pieceCid",
              "type": "bytes"
            },
            {
              "internalType": "uint64",
              "name": "paddedPieceSize",
              "type": "uint64"
            },
            {
              "internalType": "bool",
              "name": "verifiedDeal",
              "type": "bool"
            },
            {
              "internalType": "bytes",
              "name": "client",
              "type": "bytes"
            },
            {
              "internalType": "bytes",
              "name": "label",
              "type": "bytes"
            },
            {
              "internalType": "uint64",
              "name": "startEpoch",
              "type": "uint64"
            },
            {
              "internalType": "uint64",
              "name": "endEpoch",
              "type": "uint64"
            },
            {
              "internalType": "uint64",
              "name": "storagePricePerEpoch",
              "type": "uint64"
            },
            {
              "internalType": "uint64",
              "name": "providerCollateral",
              "type": "uint64"
            },
            {
              "internalType": "uint64",
              "name": "clientCollateral",
              "type": "uint64"
            },
            {
              "internalType": "string",
              "name": "version",
              "type": "string"
            },
            {
              "internalType": "bytes",
              "name": "params",
              "type": "bytes"
            }
          ],
          "internalType": "struct DealClient.DealProposal",
          "name": "",
          "type": "tuple"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "uint64",
          "name": "method",
          "type": "uint64"
        },
        {
          "internalType": "uint64",
          "name": "",
          "type": "uint64"
        },
        {
          "internalType": "bytes",
          "name": "params",
          "type": "bytes"
        }
      ],
      "name": "handle_filecoin_method",
      "outputs": [],
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "inputs": [
        {
          "components": [
            {
              "internalType": "bytes",
              "name": "pieceCid",
              "type": "bytes"
            },
            {
              "internalType": "uint64",
              "name": "paddedPieceSize",
              "type": "uint64"
            },
            {
              "internalType": "bool",
              "name": "verifiedDeal",
              "type": "bool"
            },
            {
              "internalType": "bytes",
              "name": "client",
              "type": "bytes"
            },
            {
              "internalType": "bytes",
              "name": "label",
              "type": "bytes"
            },
            {
              "internalType": "uint64",
              "name": "startEpoch",
              "type": "uint64"
            },
            {
              "internalType": "uint64",
              "name": "endEpoch",
              "type": "uint64"
            },
            {
              "internalType": "uint64",
              "name": "storagePricePerEpoch",
              "type": "uint64"
            },
            {
              "internalType": "uint64",
              "name": "providerCollateral",
              "type": "uint64"
            },
            {
              "internalType": "uint64",
              "name": "clientCollateral",
              "type": "uint64"
            },
            {
              "internalType": "string",
              "name": "version",
              "type": "string"
            },
            {
              "internalType": "bytes",
              "name": "params",
              "type": "bytes"
            }
          ],
          "internalType": "struct DealClient.DealProposal",
          "name": "_deal",
          "type": "tuple"
        }
      ],
      "name": "makeDealProposal",
      "outputs": [
        {
          "internalType": "bytes32",
          "name": "",
          "type": "bytes32"
        }
      ],
      "stateMutability": "nonpayable",
      "type": "function"
    },
    {
      "inputs": [],
      "name": "owner",
      "outputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "bytes32",
          "name": "",
          "type": "bytes32"
        }
      ],
      "name": "proposals",
      "outputs": [
        {
          "internalType": "bytes",
          "name": "pieceCid",
          "type": "bytes"
        },
        {
          "internalType": "uint64",
          "name": "paddedPieceSize",
          "type": "uint64"
        },
        {
          "internalType": "bool",
          "name": "verifiedDeal",
          "type": "bool"
        },
        {
          "internalType": "bytes",
          "name": "client",
          "type": "bytes"
        },
        {
          "internalType": "bytes",
          "name": "label",
          "type": "bytes"
        },
        {
          "internalType": "uint64",
          "name": "startEpoch",
          "type": "uint64"
        },
        {
          "internalType": "uint64",
          "name": "endEpoch",
          "type": "uint64"
        },
        {
          "internalType": "uint64",
          "name": "storagePricePerEpoch",
          "type": "uint64"
        },
        {
          "internalType": "uint64",
          "name": "providerCollateral",
          "type": "uint64"
        },
        {
          "internalType": "uint64",
          "name": "clientCollateral",
          "type": "uint64"
        },
        {
          "internalType": "string",
          "name": "version",
          "type": "string"
        },
        {
          "internalType": "bytes",
          "name": "params",
          "type": "bytes"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      "inputs": [
        {
          "internalType": "bytes",
          "name": "raw_auth_params",
          "type": "bytes"
        },
        {
          "internalType": "address",
          "name": "callee",
          "type": "address"
        }
      ],
      "name": "publish_deal",
      "outputs": [],
      "stateMutability": "nonpayable",
      "type": "function"
    }
]
`
