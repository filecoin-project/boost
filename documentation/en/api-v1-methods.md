# Groups
* [](#)
  * [Deal](#Deal)
* [Actor](#Actor)
  * [ActorSectorSize](#ActorSectorSize)
* [Auth](#Auth)
  * [AuthNew](#AuthNew)
  * [AuthVerify](#AuthVerify)
* [Deals](#Deals)
  * [DealsConsiderOfflineRetrievalDeals](#DealsConsiderOfflineRetrievalDeals)
  * [DealsConsiderOfflineStorageDeals](#DealsConsiderOfflineStorageDeals)
  * [DealsConsiderOnlineRetrievalDeals](#DealsConsiderOnlineRetrievalDeals)
  * [DealsConsiderOnlineStorageDeals](#DealsConsiderOnlineStorageDeals)
  * [DealsConsiderUnverifiedStorageDeals](#DealsConsiderUnverifiedStorageDeals)
  * [DealsConsiderVerifiedStorageDeals](#DealsConsiderVerifiedStorageDeals)
  * [DealsPieceCidBlocklist](#DealsPieceCidBlocklist)
  * [DealsSetConsiderOfflineRetrievalDeals](#DealsSetConsiderOfflineRetrievalDeals)
  * [DealsSetConsiderOfflineStorageDeals](#DealsSetConsiderOfflineStorageDeals)
  * [DealsSetConsiderOnlineRetrievalDeals](#DealsSetConsiderOnlineRetrievalDeals)
  * [DealsSetConsiderOnlineStorageDeals](#DealsSetConsiderOnlineStorageDeals)
  * [DealsSetConsiderUnverifiedStorageDeals](#DealsSetConsiderUnverifiedStorageDeals)
  * [DealsSetConsiderVerifiedStorageDeals](#DealsSetConsiderVerifiedStorageDeals)
  * [DealsSetPieceCidBlocklist](#DealsSetPieceCidBlocklist)
* [I](#I)
  * [ID](#ID)
* [Indexer](#Indexer)
  * [IndexerAnnounceAllDeals](#IndexerAnnounceAllDeals)
* [Log](#Log)
  * [LogList](#LogList)
  * [LogSetLevel](#LogSetLevel)
* [Make](#Make)
  * [MakeOfflineDealWithData](#MakeOfflineDealWithData)
* [Market](#Market)
  * [MarketDataTransferUpdates](#MarketDataTransferUpdates)
  * [MarketDummyDeal](#MarketDummyDeal)
  * [MarketGetAsk](#MarketGetAsk)
  * [MarketGetRetrievalAsk](#MarketGetRetrievalAsk)
  * [MarketImportDealData](#MarketImportDealData)
  * [MarketListDataTransfers](#MarketListDataTransfers)
  * [MarketListRetrievalDeals](#MarketListRetrievalDeals)
  * [MarketRestartDataTransfer](#MarketRestartDataTransfer)
  * [MarketSetAsk](#MarketSetAsk)
  * [MarketSetRetrievalAsk](#MarketSetRetrievalAsk)
* [Net](#Net)
  * [NetAddrsListen](#NetAddrsListen)
  * [NetAgentVersion](#NetAgentVersion)
  * [NetAutoNatStatus](#NetAutoNatStatus)
  * [NetBandwidthStats](#NetBandwidthStats)
  * [NetBandwidthStatsByPeer](#NetBandwidthStatsByPeer)
  * [NetBandwidthStatsByProtocol](#NetBandwidthStatsByProtocol)
  * [NetBlockAdd](#NetBlockAdd)
  * [NetBlockList](#NetBlockList)
  * [NetBlockRemove](#NetBlockRemove)
  * [NetConnect](#NetConnect)
  * [NetConnectedness](#NetConnectedness)
  * [NetDisconnect](#NetDisconnect)
  * [NetFindPeer](#NetFindPeer)
  * [NetPeerInfo](#NetPeerInfo)
  * [NetPeers](#NetPeers)
## 


### Deal


([]*docgen.Method) (len=1 cap=1) {
 (*docgen.Method)(0xc0005f79c0)({
  Comment: (string) "",
  Name: (string) (len=4) "Deal",
  InputExample: (string) (len=44) "[\n  \"07070707-0707-0707-0707-070707070707\"\n]",
  ResponseExample: (string) (len=1153) "{\n  \"DealUuid\": \"07070707-0707-0707-0707-070707070707\",\n  \"CreatedAt\": \"0001-01-01T00:00:00Z\",\n  \"ClientDealProposal\": {\n    \"Proposal\": {\n      \"PieceCID\": {\n        \"/\": \"bafy2bzacea3wsdh6y3a36tb3skempjoxqpuyompjbmfeyf34fi3uy6uue42v4\"\n      },\n      \"PieceSize\": 1032,\n      \"VerifiedDeal\": true,\n      \"Client\": \"f01234\",\n      \"Provider\": \"f01234\",\n      \"Label\": \"string value\",\n      \"StartEpoch\": 10101,\n      \"EndEpoch\": 10101,\n      \"StoragePricePerEpoch\": \"0\",\n      \"ProviderCollateral\": \"0\",\n      \"ClientCollateral\": \"0\"\n    },\n    \"ClientSignature\": {\n      \"Type\": 2,\n      \"Data\": \"Ynl0ZSBhcnJheQ==\"\n    }\n  },\n  \"IsOffline\": true,\n  \"ClientPeerID\": \"12D3KooWGzxzKZYveHXtpG6AsrUJBcWxHBFS2HsEoGTxrMLvKXtf\",\n  \"DealDataRoot\": {\n    \"/\": \"bafy2bzacea3wsdh6y3a36tb3skempjoxqpuyompjbmfeyf34fi3uy6uue42v4\"\n  },\n  \"InboundFilePath\": \"string value\",\n  \"Transfer\": {\n    \"Type\": \"string value\",\n    \"ClientID\": \"string value\",\n    \"Params\": \"Ynl0ZSBhcnJheQ==\",\n    \"Size\": 42\n  },\n  \"ChainDealID\": 5432,\n  \"PublishCID\": null,\n  \"SectorID\": 9,\n  \"Offset\": 1032,\n  \"Length\": 1032,\n  \"Checkpoint\": 1,\n  \"Err\": \"string value\",\n  \"NBytesReceived\": 9\n}"
 })
}
