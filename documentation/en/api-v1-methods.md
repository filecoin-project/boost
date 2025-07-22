# Groups
* [](#)
  * [Discover](#discover)
* [Auth](#auth)
  * [AuthNew](#authnew)
  * [AuthVerify](#authverify)
* [Blockstore](#blockstore)
  * [BlockstoreGet](#blockstoreget)
  * [BlockstoreGetSize](#blockstoregetsize)
  * [BlockstoreHas](#blockstorehas)
* [Boost](#boost)
  * [BoostDeal](#boostdeal)
  * [BoostDealBySignedProposalCid](#boostdealbysignedproposalcid)
  * [BoostDirectDeal](#boostdirectdeal)
  * [BoostDummyDeal](#boostdummydeal)
  * [BoostIndexerAnnounceAllDeals](#boostindexerannouncealldeals)
  * [BoostIndexerAnnounceDeal](#boostindexerannouncedeal)
  * [BoostIndexerAnnounceDealRemoved](#boostindexerannouncedealremoved)
  * [BoostIndexerAnnounceLatest](#boostindexerannouncelatest)
  * [BoostIndexerAnnounceLatestHttp](#boostindexerannouncelatesthttp)
  * [BoostIndexerAnnounceLegacyDeal](#boostindexerannouncelegacydeal)
  * [BoostIndexerListMultihashes](#boostindexerlistmultihashes)
  * [BoostIndexerRemoveAll](#boostindexerremoveall)
  * [BoostLegacyDealByProposalCid](#boostlegacydealbyproposalcid)
  * [BoostOfflineDealWithData](#boostofflinedealwithdata)
* [I](#i)
  * [ID](#id)
* [Log](#log)
  * [LogList](#loglist)
  * [LogSetLevel](#logsetlevel)
* [Market](#market)
  * [MarketGetAsk](#marketgetask)
* [Net](#net)
  * [NetAddrsListen](#netaddrslisten)
  * [NetAgentVersion](#netagentversion)
  * [NetAutoNatStatus](#netautonatstatus)
  * [NetBandwidthStats](#netbandwidthstats)
  * [NetBandwidthStatsByPeer](#netbandwidthstatsbypeer)
  * [NetBandwidthStatsByProtocol](#netbandwidthstatsbyprotocol)
  * [NetBlockAdd](#netblockadd)
  * [NetBlockList](#netblocklist)
  * [NetBlockRemove](#netblockremove)
  * [NetConnect](#netconnect)
  * [NetConnectedness](#netconnectedness)
  * [NetDisconnect](#netdisconnect)
  * [NetFindPeer](#netfindpeer)
  * [NetLimit](#netlimit)
  * [NetPeerInfo](#netpeerinfo)
  * [NetPeers](#netpeers)
  * [NetPing](#netping)
  * [NetProtectAdd](#netprotectadd)
  * [NetProtectList](#netprotectlist)
  * [NetProtectRemove](#netprotectremove)
  * [NetPubsubScores](#netpubsubscores)
  * [NetSetLimit](#netsetlimit)
  * [NetStat](#netstat)
* [Online](#online)
  * [OnlineBackup](#onlinebackup)
* [Pd](#pd)
  * [PdBuildIndexForPieceCid](#pdbuildindexforpiececid)
  * [PdCleanup](#pdcleanup)
  * [PdRemoveDealForPiece](#pdremovedealforpiece)
## 


### Discover


Perms: read

Inputs: `null`

Response:
```json
{
  "info": {
    "title": "Boost RPC API",
    "version": "1.2.1/generated=2020-11-22T08:22:42-06:00"
  },
  "methods": [],
  "openrpc": "1.2.6"
}
```

## Auth


### AuthNew


Perms: admin

Inputs:
```json
[
  [
    "write"
  ]
]
```

Response: `"Ynl0ZSBhcnJheQ=="`

### AuthVerify


Perms: read

Inputs:
```json
[
  "string value"
]
```

Response:
```json
[
  "write"
]
```

## Blockstore


### BlockstoreGet
There are not yet any comments for this method.

Perms: read

Inputs:
```json
[
  null
]
```

Response: `"Ynl0ZSBhcnJheQ=="`

### BlockstoreGetSize


Perms: read

Inputs:
```json
[
  null
]
```

Response: `123`

### BlockstoreHas


Perms: read

Inputs:
```json
[
  null
]
```

Response: `true`

## Boost


### BoostDeal


Perms: admin

Inputs:
```json
[
  "07070707-0707-0707-0707-070707070707"
]
```

Response:
```json
{
  "DealUuid": "07070707-0707-0707-0707-070707070707",
  "CreatedAt": "0001-01-01T00:00:00Z",
  "ClientDealProposal": {
    "Proposal": {
      "PieceCID": null,
      "PieceSize": 1032,
      "VerifiedDeal": true,
      "Client": "f01234",
      "Provider": "f01234",
      "Label": "",
      "StartEpoch": 10101,
      "EndEpoch": 10101,
      "StoragePricePerEpoch": "0",
      "ProviderCollateral": "0",
      "ClientCollateral": "0"
    },
    "ClientSignature": {
      "Type": 2,
      "Data": "Ynl0ZSBhcnJheQ=="
    }
  },
  "IsOffline": true,
  "CleanupData": true,
  "ClientPeerID": "12D3KooWGzxzKZYveHXtpG6AsrUJBcWxHBFS2HsEoGTxrMLvKXtf",
  "DealDataRoot": null,
  "InboundFilePath": "string value",
  "Transfer": {
    "Type": "string value",
    "ClientID": "string value",
    "Params": "Ynl0ZSBhcnJheQ==",
    "Size": 42
  },
  "ChainDealID": 5432,
  "PublishCID": null,
  "SectorID": 9,
  "Offset": 1032,
  "Length": 1032,
  "Checkpoint": 1,
  "CheckpointAt": "0001-01-01T00:00:00Z",
  "Err": "string value",
  "Retry": "auto",
  "NBytesReceived": 9,
  "FastRetrieval": true,
  "AnnounceToIPNI": true
}
```

### BoostDealBySignedProposalCid


Perms: admin

Inputs:
```json
[
  null
]
```

Response:
```json
{
  "DealUuid": "07070707-0707-0707-0707-070707070707",
  "CreatedAt": "0001-01-01T00:00:00Z",
  "ClientDealProposal": {
    "Proposal": {
      "PieceCID": null,
      "PieceSize": 1032,
      "VerifiedDeal": true,
      "Client": "f01234",
      "Provider": "f01234",
      "Label": "",
      "StartEpoch": 10101,
      "EndEpoch": 10101,
      "StoragePricePerEpoch": "0",
      "ProviderCollateral": "0",
      "ClientCollateral": "0"
    },
    "ClientSignature": {
      "Type": 2,
      "Data": "Ynl0ZSBhcnJheQ=="
    }
  },
  "IsOffline": true,
  "CleanupData": true,
  "ClientPeerID": "12D3KooWGzxzKZYveHXtpG6AsrUJBcWxHBFS2HsEoGTxrMLvKXtf",
  "DealDataRoot": null,
  "InboundFilePath": "string value",
  "Transfer": {
    "Type": "string value",
    "ClientID": "string value",
    "Params": "Ynl0ZSBhcnJheQ==",
    "Size": 42
  },
  "ChainDealID": 5432,
  "PublishCID": null,
  "SectorID": 9,
  "Offset": 1032,
  "Length": 1032,
  "Checkpoint": 1,
  "CheckpointAt": "0001-01-01T00:00:00Z",
  "Err": "string value",
  "Retry": "auto",
  "NBytesReceived": 9,
  "FastRetrieval": true,
  "AnnounceToIPNI": true
}
```

### BoostDirectDeal


Perms: admin

Inputs:
```json
[
  {
    "DealUUID": "07070707-0707-0707-0707-070707070707",
    "AllocationID": 0,
    "PieceCid": null,
    "ClientAddr": "f01234",
    "StartEpoch": 10101,
    "EndEpoch": 10101,
    "FilePath": "string value",
    "DeleteAfterImport": true,
    "RemoveUnsealedCopy": true,
    "SkipIPNIAnnounce": true
  }
]
```

Response:
```json
{
  "Accepted": true,
  "Reason": "string value"
}
```

### BoostDummyDeal


Perms: admin

Inputs:
```json
[
  {
    "DealUUID": "07070707-0707-0707-0707-070707070707",
    "IsOffline": true,
    "ClientDealProposal": {
      "Proposal": {
        "PieceCID": null,
        "PieceSize": 1032,
        "VerifiedDeal": true,
        "Client": "f01234",
        "Provider": "f01234",
        "Label": "",
        "StartEpoch": 10101,
        "EndEpoch": 10101,
        "StoragePricePerEpoch": "0",
        "ProviderCollateral": "0",
        "ClientCollateral": "0"
      },
      "ClientSignature": {
        "Type": 2,
        "Data": "Ynl0ZSBhcnJheQ=="
      }
    },
    "DealDataRoot": null,
    "Transfer": {
      "Type": "string value",
      "ClientID": "string value",
      "Params": "Ynl0ZSBhcnJheQ==",
      "Size": 42
    },
    "RemoveUnsealedCopy": true,
    "SkipIPNIAnnounce": true
  }
]
```

Response:
```json
{
  "Accepted": true,
  "Reason": "string value"
}
```

### BoostIndexerAnnounceAllDeals


Perms: admin

Inputs: `null`

Response: `{}`

### BoostIndexerAnnounceDeal


Perms: admin

Inputs:
```json
[
  {
    "DealUuid": "07070707-0707-0707-0707-070707070707",
    "CreatedAt": "0001-01-01T00:00:00Z",
    "ClientDealProposal": {
      "Proposal": {
        "PieceCID": null,
        "PieceSize": 1032,
        "VerifiedDeal": true,
        "Client": "f01234",
        "Provider": "f01234",
        "Label": "",
        "StartEpoch": 10101,
        "EndEpoch": 10101,
        "StoragePricePerEpoch": "0",
        "ProviderCollateral": "0",
        "ClientCollateral": "0"
      },
      "ClientSignature": {
        "Type": 2,
        "Data": "Ynl0ZSBhcnJheQ=="
      }
    },
    "IsOffline": true,
    "CleanupData": true,
    "ClientPeerID": "12D3KooWGzxzKZYveHXtpG6AsrUJBcWxHBFS2HsEoGTxrMLvKXtf",
    "DealDataRoot": null,
    "InboundFilePath": "string value",
    "Transfer": {
      "Type": "string value",
      "ClientID": "string value",
      "Params": "Ynl0ZSBhcnJheQ==",
      "Size": 42
    },
    "ChainDealID": 5432,
    "PublishCID": null,
    "SectorID": 9,
    "Offset": 1032,
    "Length": 1032,
    "Checkpoint": 1,
    "CheckpointAt": "0001-01-01T00:00:00Z",
    "Err": "string value",
    "Retry": "auto",
    "NBytesReceived": 9,
    "FastRetrieval": true,
    "AnnounceToIPNI": true
  }
]
```

Response: `null`

### BoostIndexerAnnounceDealRemoved


Perms: admin

Inputs:
```json
[
  null
]
```

Response: `null`

### BoostIndexerAnnounceLatest


Perms: admin

Inputs: `null`

Response: `null`

### BoostIndexerAnnounceLatestHttp


Perms: admin

Inputs:
```json
[
  [
    "string value"
  ]
]
```

Response: `null`

### BoostIndexerAnnounceLegacyDeal


Perms: admin

Inputs:
```json
[
  null
]
```

Response: `null`

### BoostIndexerListMultihashes


Perms: admin

Inputs:
```json
[
  "Ynl0ZSBhcnJheQ=="
]
```

Response:
```json
[
  "Bw=="
]
```

### BoostIndexerRemoveAll
There are not yet any comments for this method.

Perms: admin

Inputs: `null`

Response:
```json
[
  null
]
```

### BoostLegacyDealByProposalCid


Perms: admin

Inputs:
```json
[
  null
]
```

Response:
```json
{
  "Proposal": {
    "PieceCID": null,
    "PieceSize": 1032,
    "VerifiedDeal": true,
    "Client": "f01234",
    "Provider": "f01234",
    "Label": "",
    "StartEpoch": 10101,
    "EndEpoch": 10101,
    "StoragePricePerEpoch": "0",
    "ProviderCollateral": "0",
    "ClientCollateral": "0"
  },
  "ClientSignature": {
    "Type": 2,
    "Data": "Ynl0ZSBhcnJheQ=="
  },
  "ProposalCid": null,
  "AddFundsCid": null,
  "PublishCid": null,
  "Miner": "12D3KooWGzxzKZYveHXtpG6AsrUJBcWxHBFS2HsEoGTxrMLvKXtf",
  "Client": "12D3KooWGzxzKZYveHXtpG6AsrUJBcWxHBFS2HsEoGTxrMLvKXtf",
  "State": 42,
  "PiecePath": "",
  "MetadataPath": "",
  "SlashEpoch": 10101,
  "FastRetrieval": true,
  "Message": "string value",
  "FundsReserved": "0",
  "Ref": {
    "TransferType": "string value",
    "Root": null,
    "PieceCid": null,
    "PieceSize": 1024,
    "RawBlockSize": 42
  },
  "AvailableForRetrieval": true,
  "DealID": 5432,
  "CreationTime": "0001-01-01T00:00:00Z",
  "TransferChannelId": {
    "Initiator": "12D3KooWGzxzKZYveHXtpG6AsrUJBcWxHBFS2HsEoGTxrMLvKXtf",
    "Responder": "12D3KooWGzxzKZYveHXtpG6AsrUJBcWxHBFS2HsEoGTxrMLvKXtf",
    "ID": 3
  },
  "SectorNumber": 9,
  "InboundCAR": "string value"
}
```

### BoostOfflineDealWithData


Perms: admin

Inputs:
```json
[
  "07070707-0707-0707-0707-070707070707",
  "string value",
  true
]
```

Response:
```json
{
  "Accepted": true,
  "Reason": "string value"
}
```

## I


### ID


Perms: read

Inputs: `null`

Response: `"12D3KooWGzxzKZYveHXtpG6AsrUJBcWxHBFS2HsEoGTxrMLvKXtf"`

## Log


### LogList


Perms: write

Inputs: `null`

Response:
```json
[
  "string value"
]
```

### LogSetLevel


Perms: write

Inputs:
```json
[
  "string value",
  "string value"
]
```

Response: `{}`

## Market


### MarketGetAsk


Perms: read

Inputs: `null`

Response:
```json
{
  "Ask": {
    "Price": "0",
    "VerifiedPrice": "0",
    "MinPieceSize": 1032,
    "MaxPieceSize": 1032,
    "Miner": "f01234",
    "Timestamp": 10101,
    "Expiry": 10101,
    "SeqNo": 42
  },
  "Signature": {
    "Type": 2,
    "Data": "Ynl0ZSBhcnJheQ=="
  }
}
```

## Net


### NetAddrsListen


Perms: read

Inputs: `null`

Response:
```json
{
  "ID": "12D3KooWGzxzKZYveHXtpG6AsrUJBcWxHBFS2HsEoGTxrMLvKXtf",
  "Addrs": [
    "/ip4/52.36.61.156/tcp/1347/p2p/12D3KooWFETiESTf1v4PGUvtnxMAcEFMzLZbJGg4tjWfGEimYior"
  ]
}
```

### NetAgentVersion


Perms: read

Inputs:
```json
[
  "12D3KooWGzxzKZYveHXtpG6AsrUJBcWxHBFS2HsEoGTxrMLvKXtf"
]
```

Response: `"string value"`

### NetAutoNatStatus


Perms: read

Inputs: `null`

Response:
```json
{
  "Reachability": 1,
  "PublicAddrs": [
    "string value"
  ]
}
```

### NetBandwidthStats


Perms: read

Inputs: `null`

Response:
```json
{
  "TotalIn": 9,
  "TotalOut": 9,
  "RateIn": 12.3,
  "RateOut": 12.3
}
```

### NetBandwidthStatsByPeer


Perms: read

Inputs: `null`

Response:
```json
{
  "12D3KooWSXmXLJmBR1M7i9RW9GQPNUhZSzXKzxDHWtAgNuJAbyEJ": {
    "TotalIn": 174000,
    "TotalOut": 12500,
    "RateIn": 100,
    "RateOut": 50
  }
}
```

### NetBandwidthStatsByProtocol


Perms: read

Inputs: `null`

Response:
```json
{
  "/fil/hello/1.0.0": {
    "TotalIn": 174000,
    "TotalOut": 12500,
    "RateIn": 100,
    "RateOut": 50
  }
}
```

### NetBlockAdd


Perms: admin

Inputs:
```json
[
  {
    "Peers": [
      "12D3KooWGzxzKZYveHXtpG6AsrUJBcWxHBFS2HsEoGTxrMLvKXtf"
    ],
    "IPAddrs": [
      "string value"
    ],
    "IPSubnets": [
      "string value"
    ]
  }
]
```

Response: `{}`

### NetBlockList


Perms: read

Inputs: `null`

Response:
```json
{
  "Peers": [
    "12D3KooWGzxzKZYveHXtpG6AsrUJBcWxHBFS2HsEoGTxrMLvKXtf"
  ],
  "IPAddrs": [
    "string value"
  ],
  "IPSubnets": [
    "string value"
  ]
}
```

### NetBlockRemove


Perms: admin

Inputs:
```json
[
  {
    "Peers": [
      "12D3KooWGzxzKZYveHXtpG6AsrUJBcWxHBFS2HsEoGTxrMLvKXtf"
    ],
    "IPAddrs": [
      "string value"
    ],
    "IPSubnets": [
      "string value"
    ]
  }
]
```

Response: `{}`

### NetConnect


Perms: write

Inputs:
```json
[
  {
    "ID": "12D3KooWGzxzKZYveHXtpG6AsrUJBcWxHBFS2HsEoGTxrMLvKXtf",
    "Addrs": [
      "/ip4/52.36.61.156/tcp/1347/p2p/12D3KooWFETiESTf1v4PGUvtnxMAcEFMzLZbJGg4tjWfGEimYior"
    ]
  }
]
```

Response: `{}`

### NetConnectedness


Perms: read

Inputs:
```json
[
  "12D3KooWGzxzKZYveHXtpG6AsrUJBcWxHBFS2HsEoGTxrMLvKXtf"
]
```

Response: `1`

### NetDisconnect


Perms: write

Inputs:
```json
[
  "12D3KooWGzxzKZYveHXtpG6AsrUJBcWxHBFS2HsEoGTxrMLvKXtf"
]
```

Response: `{}`

### NetFindPeer


Perms: read

Inputs:
```json
[
  "12D3KooWGzxzKZYveHXtpG6AsrUJBcWxHBFS2HsEoGTxrMLvKXtf"
]
```

Response:
```json
{
  "ID": "12D3KooWGzxzKZYveHXtpG6AsrUJBcWxHBFS2HsEoGTxrMLvKXtf",
  "Addrs": [
    "/ip4/52.36.61.156/tcp/1347/p2p/12D3KooWFETiESTf1v4PGUvtnxMAcEFMzLZbJGg4tjWfGEimYior"
  ]
}
```

### NetLimit


Perms: read

Inputs:
```json
[
  "string value"
]
```

Response:
```json
{
  "Memory": 9,
  "Streams": 123,
  "StreamsInbound": 123,
  "StreamsOutbound": 123,
  "Conns": 123,
  "ConnsInbound": 123,
  "ConnsOutbound": 123,
  "FD": 123
}
```

### NetPeerInfo


Perms: read

Inputs:
```json
[
  "12D3KooWGzxzKZYveHXtpG6AsrUJBcWxHBFS2HsEoGTxrMLvKXtf"
]
```

Response:
```json
{
  "ID": "12D3KooWGzxzKZYveHXtpG6AsrUJBcWxHBFS2HsEoGTxrMLvKXtf",
  "Agent": "string value",
  "Addrs": [
    "string value"
  ],
  "Protocols": [
    "string value"
  ],
  "ConnMgrMeta": {
    "FirstSeen": "0001-01-01T00:00:00Z",
    "Value": 123,
    "Tags": {
      "name": 42
    },
    "Conns": {
      "name": "2021-03-08T22:52:18Z"
    }
  }
}
```

### NetPeers


Perms: read

Inputs: `null`

Response:
```json
[
  {
    "ID": "12D3KooWGzxzKZYveHXtpG6AsrUJBcWxHBFS2HsEoGTxrMLvKXtf",
    "Addrs": [
      "/ip4/52.36.61.156/tcp/1347/p2p/12D3KooWFETiESTf1v4PGUvtnxMAcEFMzLZbJGg4tjWfGEimYior"
    ]
  }
]
```

### NetPing


Perms: read

Inputs:
```json
[
  "12D3KooWGzxzKZYveHXtpG6AsrUJBcWxHBFS2HsEoGTxrMLvKXtf"
]
```

Response: `60000000000`

### NetProtectAdd


Perms: admin

Inputs:
```json
[
  [
    "12D3KooWGzxzKZYveHXtpG6AsrUJBcWxHBFS2HsEoGTxrMLvKXtf"
  ]
]
```

Response: `{}`

### NetProtectList


Perms: read

Inputs: `null`

Response:
```json
[
  "12D3KooWGzxzKZYveHXtpG6AsrUJBcWxHBFS2HsEoGTxrMLvKXtf"
]
```

### NetProtectRemove


Perms: admin

Inputs:
```json
[
  [
    "12D3KooWGzxzKZYveHXtpG6AsrUJBcWxHBFS2HsEoGTxrMLvKXtf"
  ]
]
```

Response: `{}`

### NetPubsubScores


Perms: read

Inputs: `null`

Response:
```json
[
  {
    "ID": "12D3KooWGzxzKZYveHXtpG6AsrUJBcWxHBFS2HsEoGTxrMLvKXtf",
    "Score": {
      "Score": 12.3,
      "Topics": {
        "/blocks": {
          "TimeInMesh": 60000000000,
          "FirstMessageDeliveries": 122,
          "MeshMessageDeliveries": 1234,
          "InvalidMessageDeliveries": 3
        }
      },
      "AppSpecificScore": 12.3,
      "IPColocationFactor": 12.3,
      "BehaviourPenalty": 12.3
    }
  }
]
```

### NetSetLimit


Perms: admin

Inputs:
```json
[
  "string value",
  {
    "Memory": 9,
    "Streams": 123,
    "StreamsInbound": 123,
    "StreamsOutbound": 123,
    "Conns": 123,
    "ConnsInbound": 123,
    "ConnsOutbound": 123,
    "FD": 123
  }
]
```

Response: `{}`

### NetStat


Perms: read

Inputs:
```json
[
  "string value"
]
```

Response:
```json
{
  "System": {
    "NumStreamsInbound": 123,
    "NumStreamsOutbound": 123,
    "NumConnsInbound": 123,
    "NumConnsOutbound": 123,
    "NumFD": 123,
    "Memory": 9
  },
  "Transient": {
    "NumStreamsInbound": 123,
    "NumStreamsOutbound": 123,
    "NumConnsInbound": 123,
    "NumConnsOutbound": 123,
    "NumFD": 123,
    "Memory": 9
  },
  "Services": {
    "abc": {
      "NumStreamsInbound": 1,
      "NumStreamsOutbound": 2,
      "NumConnsInbound": 3,
      "NumConnsOutbound": 4,
      "NumFD": 5,
      "Memory": 123
    }
  },
  "Protocols": {
    "abc": {
      "NumStreamsInbound": 1,
      "NumStreamsOutbound": 2,
      "NumConnsInbound": 3,
      "NumConnsOutbound": 4,
      "NumFD": 5,
      "Memory": 123
    }
  },
  "Peers": {
    "abc": {
      "NumStreamsInbound": 1,
      "NumStreamsOutbound": 2,
      "NumConnsInbound": 3,
      "NumConnsOutbound": 4,
      "NumFD": 5,
      "Memory": 123
    }
  }
}
```

## Online


### OnlineBackup
There are not yet any comments for this method.

Perms: admin

Inputs:
```json
[
  "string value"
]
```

Response: `{}`

## Pd


### PdBuildIndexForPieceCid
There are not yet any comments for this method.

Perms: admin

Inputs:
```json
[
  null
]
```

Response: `{}`

### PdCleanup


Perms: admin

Inputs: `null`

Response: `{}`

### PdRemoveDealForPiece


Perms: admin

Inputs:
```json
[
  null,
  "string value"
]
```

Response: `{}`

