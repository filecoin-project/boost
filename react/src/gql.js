import gql from "graphql-tag";
import { ApolloClient, ApolloLink, HttpLink, InMemoryCache, split, from } from "@apollo/client";
import { getMainDefinition } from '@apollo/client/utilities';
import { WebSocketLink } from "@apollo/client/link/ws";
import Observable from 'zen-observable';
import { transformResponse } from "./transform";

var graphqlEndpoint = window.location.host
var graphqlHttpEndpoint = window.location.origin

if (process.env.NODE_ENV === 'development') {
    graphqlEndpoint = 'localhost:8080'
    graphqlHttpEndpoint = 'http://' + graphqlEndpoint
}

// Transform response data (eg convert date string to Date object)
const transformResponseLink = new ApolloLink((operation, forward) => {
    const res = forward(operation)
    return Observable.from(res).map(data => {
        transformResponse(data)
        return data
    });
});

// HTTP Link
const httpLink = new HttpLink({
    uri: `${graphqlHttpEndpoint}/graphql/query`,
});

// WebSocket Link
const wsLink = new WebSocketLink({
    uri: `ws://${graphqlEndpoint}/graphql/subscription`,
    options: {
        reconnect: true,
        minTimeout: 5000,
        lazy: true,
    },
});

// Send query request based on the type definition
const link = from([
    transformResponseLink,
    split(
    ({ query }) => {
            const definition = getMainDefinition(query);
            return (
                definition.kind === 'OperationDefinition' &&
                definition.operation === 'subscription'
            );
        },
        wsLink,
        httpLink
    )
]);

const cache = new InMemoryCache();

const gqlClient = new ApolloClient({
    link,
    cache
});

const EpochQuery = gql`
    query AppEpochQuery {
        epoch {
            Epoch
            SecondsPerEpoch
        }
    }
`;

const MinerAddressQuery = gql`
    query AppMinerAddressQuery {
        minerAddress {
            MinerID
            IsCurio
        }
    }
`;

const GraphsyncRetrievalMinerAddressesQuery = gql`
    query AppGraphsyncRetrievalMinerAddressesQuery {
        graphsyncRetrievalMinerAddresses
    }
`;

const DealsListQuery = gql`
    query AppDealsListQuery($query: String, $filter: DealFilter, $cursor: ID, $offset: Int, $limit: Int) {
        deals(query: $query, filter: $filter, cursor: $cursor, offset: $offset, limit: $limit) {
            deals {
                ID
                CreatedAt
                ClientAddress
                Checkpoint
                CheckpointAt
                AnnounceToIPNI
                KeepUnsealedCopy
                IsOffline
                CleanupData
                Err
                Retry
                Message
                SealingState
                ChainDealID
                PieceSize
                Transfer {
                    Type
                    Size
                    Params
                }
                Transferred
                TransferSamples {
                    At
                    Bytes
                }
                IsTransferStalled
                Sector {
                    ID
                    Offset
                    Length
                }
            }
            totalCount
            more
        }
    }
`;

const LegacyDealsListQuery = gql`
    query AppLegacyDealsListQuery($query: String, $cursor: ID, $offset: Int, $limit: Int) {
        legacyDeals(query: $query, cursor: $cursor, offset: $offset, limit: $limit) {
            deals {
                ID
                CreatedAt
                PieceCid
                PieceSize
                ClientAddress
                StartEpoch
                EndEpoch
                ProviderCollateral
                ClientPeerID
                DealDataRoot
                PublishCid
                Status
                Message
                SectorNumber
                ChainDealID
            }
            totalCount
            more
        }
    }
`;

const DirectDealsListQuery = gql`
    query AppDirectDealsListQuery($query: String, $filter: DealFilter, $cursor: ID, $offset: Int, $limit: Int) {
        directDeals(query: $query, filter: $filter, cursor: $cursor, offset: $offset, limit: $limit) {
            deals {
                ID
                CreatedAt
                AllocationID
                ClientAddress
                Checkpoint
                CheckpointAt
                AnnounceToIPNI
                KeepUnsealedCopy
                CleanupData
                Err
                Retry
                Message
                Sector {
                    ID
                    Offset
                    Length
                }
                SealingState
            }
            totalCount
            more
        }
    }
`;

const DealsCountQuery = gql`
    query AppDealCountQuery {
        dealsCount
    }
`;

const LegacyDealsCountQuery = gql`
    query AppLegacyDealCountQuery {
        legacyDealsCount
    }
`;

const DirectDealsCountQuery = gql`
    query AppDirectDealCountQuery {
        directDealsCount
    }
`;

const DealSubscription = gql`
    subscription AppDealSubscription($id: ID!) {
        dealUpdate(id: $id) {
            ID
            CreatedAt
            PieceCid
            PieceSize
            IsVerified
            ProposalLabel
            ClientAddress
            StartEpoch
            EndEpoch
            ProviderCollateral
            ClientCollateral
            StoragePricePerEpoch
            ClientPeerID
            DealDataRoot
            SignedProposalCid
            InboundFilePath
            ChainDealID
            PublishCid
            IsOffline
            CleanupData
            Checkpoint
            CheckpointAt
            AnnounceToIPNI
            KeepUnsealedCopy
            Retry
            Err
            Message
            Transferred
            Transfer {
                Type
                Size
                Params
            }
            Sector {
                ID
                Offset
                Length
            }
            Logs {
                CreatedAt
                LogMsg
                LogParams
                Subsystem
            }
        }
    }
`;

const ProposalLogsListQuery = gql`
    query AppProposalLogsListQuery($accepted: Boolean, $cursor: BigInt, $offset: Int, $limit: Int) {
        proposalLogs(accepted: $accepted, cursor: $cursor, offset: $offset, limit: $limit) {
            logs {
                DealUUID
                Accepted
                Reason
                CreatedAt
                ClientAddress
                PieceSize
            }
            totalCount
            more
        }
    }
`;

const ProposalLogsCountQuery = gql`
    query AppProposalLogsCountQuery {
        proposalLogsCount {
            Accepted
            Rejected
            Period
        }
    }
`;

const RetrievalLogQuery = gql`
    query AppRetrievalLogQuery($peerID: String!, $transferID: Uint64!) {
        retrievalLog(peerID: $peerID, transferID: $transferID) {
            CreatedAt
            UpdatedAt
            PeerID
            TransferID
            DealID
            PayloadCID
            PieceCID
            PaymentInterval
            PaymentIntervalIncrease
            PricePerByte
            UnsealPrice
            Status
            Message
            TotalSent
            DTStatus
            DTMessage
            DTEvents {
                CreatedAt
                Name
                Message
            }
            MarketEvents {
                CreatedAt
                Name
                Status
                Message
            }
        }
    }
`;

const RetrievalLogsListQuery = gql`
    query AppRetrievalLogsListQuery($isIndexer: Boolean, $cursor: Uint64, $offset: Int, $limit: Int) {
        retrievalLogs(isIndexer: $isIndexer, cursor: $cursor, offset: $offset, limit: $limit) {
            logs {
                RowID
                CreatedAt
                UpdatedAt
                PeerID
                TransferID
                DealID
                PayloadCID
                PieceCID
                PaymentInterval
                PaymentIntervalIncrease
                PricePerByte
                UnsealPrice
                Status
                Message
                TotalSent
                DTStatus
                DTMessage
            }
            totalCount
            more
        }
    }
`;

const RetrievalLogsCountQuery = gql`
    query AppRetrievalLogsCountQuery($isIndexer: Boolean) {
        retrievalLogsCount(isIndexer: $isIndexer) {
            Count
            Period
        }
    }
`;

const IpniProviderInfoQuery = gql`
    query AppIpniProviderInfoQuery {
        ipniProviderInfo {
            PeerID
            Config
        }
    }
`;

const IpniAdQuery = gql`
    query AppIpniAdQuery($adCid: String!) {
        ipniAdvertisement(adCid: $adCid) {
            ContextID
            Metadata {
                Protocol
                Metadata
            }
            PreviousEntry
            Provider
            Addresses
            IsRemove
            ExtendedProviders {
                Override
                Providers {
                    ID
                    Addresses
                    Metadata {
                        Protocol
                        Metadata
                    }
                }
            }
        }
    }
`;

const IpniAdEntriesQuery = gql`
    query AppIpniAdEntriesQuery($adCid: String!) {
        ipniAdvertisementEntries(adCid: $adCid)
    }
`;

const IpniAdEntriesCountQuery = gql`
    query AppIpniAdEntriesCountQuery($adCid: String!) {
        ipniAdvertisementEntriesCount(adCid: $adCid)
    }
`;

const IpniLatestAdQuery = gql`
    query AppIpniLatestAdQuery {
        ipniLatestAdvertisement
    }
`;

const DealCancelMutation = gql`
    mutation AppDealCancelMutation($id: ID!) {
        dealCancel(id: $id)
    }
`;

const DealRetryPausedMutation = gql`
    mutation AppDealRetryMutation($id: ID!) {
        dealRetryPaused(id: $id)
    }
`;

const DealFailPausedMutation = gql`
    mutation AppDealRetryMutation($id: ID!) {
        dealFailPaused(id: $id)
    }
`;

const NewDealsSubscription = gql`
    subscription AppNewDealsSubscription {
        dealNew {
            deal {
                ID
                CreatedAt
                PieceCid
                PieceSize
                AnnounceToIPNI
                KeepUnsealedCopy
                ClientAddress
                StartEpoch
                EndEpoch
                ProviderCollateral
                ClientPeerID
                DealDataRoot
                SignedProposalCid
                PublishCid
                Checkpoint
                CheckpointAt
                Retry
                Err
                Message
                Transfer {
                    Type
                    Size
                    Params
                }
                Sector {
                    ID
                    Offset
                    Length
                }
                Logs {
                    CreatedAt
                    LogMsg
                    LogParams
                    Subsystem
                }
            }
            totalCount
        }
    }
`;

const LegacyDealQuery = gql`
    query AppLegacyDealQuery($id: ID!) {
        legacyDeal(id: $id) {
            ID
            CreatedAt
            PieceCid
            PieceSize
            ClientAddress
            StartEpoch
            EndEpoch
            ProviderCollateral
            ClientPeerID
            DealDataRoot
            PublishCid
            TransferType
            Transferred
            TransferSize
            Status
            Message
            SectorNumber
            FundsReserved
            ChainDealID
            InboundCARPath
            AvailableForRetrieval
        }
    }
`;

const DirectDealQuery = gql`
    query AppDirectDealQuery($id: ID!) {
        directDeal(id: $id) {
            ID
            CreatedAt
            AllocationID
            ClientAddress
            ProviderAddress
            PieceCid
            PieceSize
            StartEpoch
            EndEpoch
            InboundFilePath
            Checkpoint
            CheckpointAt
            AnnounceToIPNI
            KeepUnsealedCopy
            CleanupData
            Err
            Retry
            Message
            Sector {
                ID
                Offset
                Length
            }
            Logs {
                CreatedAt
                LogMsg
                LogParams
                Subsystem
            }
        }
    }
`;

const PiecesWithPayloadCidQuery = gql`
    query AppPiecesWithPayloadCidQuery($payloadCid: String!) {
        piecesWithPayloadCid(payloadCid: $payloadCid)
    }
`;

const FlaggedPiecesQuery = gql`
    query AppFlaggedPiecesQuery($hasUnsealedCopy: Boolean, $cursor: BigInt, $offset: Int, $limit: Int) {
        piecesFlagged(hasUnsealedCopy: $hasUnsealedCopy, cursor: $cursor, offset: $offset, limit: $limit) {
            pieces {
                CreatedAt
                PieceCid
                IndexStatus {
                    Status
                    Error
                }
                DealCount
            }
            totalCount
            more
        }
    }
`;

const FlaggedPiecesCountQuery = gql`
    query AppFlaggedPiecesCountQuery($hasUnsealedCopy: Boolean) {
        piecesFlaggedCount(hasUnsealedCopy: $hasUnsealedCopy)
    }
`;

const PieceBuildIndexMutation = gql`
    mutation AppPieceBuildIndexMutation($pieceCid: String!) {
        pieceBuildIndex(pieceCid: $pieceCid)
    }
`;

const PieceStatusQuery = gql`
    query AppPieceStatusQuery($pieceCid: String!) {
        pieceStatus(pieceCid: $pieceCid) {
            PieceCid
            IndexStatus {
                Status
                Error
            }
            Deals {
                SealStatus {
                    Status
                    Error
                }
                Deal {
                    ID
                    IsLegacy
                    CreatedAt
                    DealDataRoot
                    IsDirect
                }
                Sector {
                    ID
                    Offset
                    Length
                }
            }
            PieceInfoDeals {
                MinerAddress
                ChainDealID
                Sector {
                    ID
                    Offset
                    Length
                }
                SealStatus {
                    Status
                    Error
                }
            }
        }
    }
`;

const LIDQuery = gql`
    query AppLIDQuery {
        lid {
            ScanProgress {
                Progress
                LastScan
            }
            Pieces {
                Indexed
                FlaggedUnsealed
                FlaggedSealed
            }
            SectorUnsealedCopies {
                Unsealed
                Sealed
            }
            SectorProvingState {
                Active
                Inactive
            }
            FlaggedPieces
        }
    }
`;

const StorageQuery = gql`
    query AppStorageQuery {
        storage {
            Staged
            Transferred
            Pending
            Free
            MountPoint
        }
    }
`;

const SealingPipelineQuery = gql`
    query AppSealingPipelineQuery {
        sealingpipeline {
            WaitDealsSectors {
                SectorID
                Used
                SectorSize
                Deals {
                    ID
                    Size
                    IsLegacy
                    IsDirect
                }
            }
            SnapDealsWaitDealsSectors {
                SectorID
                Used
                SectorSize
                Deals {
                    ID
                    Size
                    IsLegacy
                    IsDirect
                }
            }
            SectorStates {
                Regular {
                    Key
                    Value
                    Order
                }
                RegularError {
                    Key
                    Value
                    Order
                }
                SnapDeals {
                    Key
                    Value
                    Order
                }
                SnapDealsError {
                    Key
                    Value
                    Order
                }
            }
            Workers {
                ID
                Start
                Stage
                Sector
            }
        }
    }
`;

const SectorStatusQuery = gql`
    query AppSectorStatusQuery($sectorNumber: Uint64!) {
        sectorStatus(sectorNumber: $sectorNumber) {
            Number
            State
            DealIDs
        }
    }
`;

const FundsQuery = gql`
    query AppFundsQuery {
        funds {
            Escrow {
                Tagged
                Available
                Locked
            }
            Collateral {
                Address
                Balance
            }
            PubMsg {
                Address
                Balance
                Tagged
            }
        }
    }
`;

const TransfersQuery = gql`
    query AppTransfersQuery {
        transfers {
            At
            Bytes
        }
    }
`;

const TransferStatsQuery = gql`
    query AppTransferStatsQuery {
        transferStats {
            HttpMaxConcurrentDownloads
            Stats {
                Host
                Total
                Started
                Stalled
                TransferSamples {
                    At
                    Bytes
                }
            }
        }
    }
`;

const FundsLogsQuery = gql`
    query AppFundsLogsQuery($cursor: BigInt, $offset: Int, $limit: Int) {
        fundsLogs(cursor: $cursor, offset: $offset, limit: $limit) {
            totalCount
            more
            logs {
                CreatedAt
                DealUUID
                Amount
                Text
            }
        }
    }
`;

const DealPublishQuery = gql`
    query AppDealPublishQuery {
        dealPublish {
            ManualPSD
            Start
            Period
            MaxDealsPerMsg
            Deals {
                ID
                IsLegacy
                CreatedAt
                Transfer {
                    Size
                }
                ClientAddress
                PieceSize
                IsDirect
            }
        }
    }
`;

const DealPublishNowMutation = gql`
    mutation AppDealPublishNowMutation {
        dealPublishNow
    }
`;

const FundsMoveToEscrow = gql`
    mutation AppDealPublishNowMutation($amount: BigInt!) {
        fundsMoveToEscrow(amount: $amount)
    }
`;

const StorageAskUpdate = gql`
    mutation AppStorageAskUpdateMutation($update: StorageAskUpdate!) {
        storageAskUpdate(update: $update)
    }
`;

const MpoolQuery = gql`
    query AppMpoolQuery($alerts: Boolean!) {
        mpool(alerts: $alerts) {
            Count
            Messages {
                SentEpoch
                From
                To
                Nonce
                Value
                GasFeeCap
                GasLimit
                GasPremium
                Method
                Params
                BaseFee
            }
        }
    }
`;

const Libp2pAddrInfoQuery = gql`
    query AppLibp2pAddrInfoQuery {
        libp2pAddrInfo {
            PeerID
            Addresses
            Protocols
        }
    }
`;

const StorageAskQuery = gql`
    query AppStorageAskQuery {
        storageAsk {
            Price
            VerifiedPrice
            MinPieceSize
            MaxPieceSize
            ExpiryEpoch
            ExpiryTime
        }
    }
`;

const PublishPendingDealsMutation = gql`
    mutation AppPublishPendingDealMutation($ids: [ID!]!) {
        publishPendingDeals(ids: $ids)
    }
`;

const PiecePayloadCidsQuery = gql`
    query AppPiecePayloadCidsQuery($pieceCid: String!) {
        piecePayloadCids(pieceCid: $pieceCid) {
            PayloadCid
            Multihash
        }
    }
`;

const IpniDistanceFromLatestAdQuery = gql`
    query AppIpniDistanceFromLatestAdQuery($latestAdcid: String!, $adcid: String!) {
        ipniDistanceFromLatestAd(LatestAdcid: $latestAdcid, Adcid: $adcid)
    }
`;

export {
    gqlClient,
    EpochQuery,
    MinerAddressQuery,
    GraphsyncRetrievalMinerAddressesQuery,
    DealsListQuery,
    LegacyDealsListQuery,
    DirectDealsListQuery,
    DealsCountQuery,
    LegacyDealsCountQuery,
    DirectDealsCountQuery,
    LegacyDealQuery,
    DirectDealQuery,
    DealSubscription,
    DealCancelMutation,
    DealRetryPausedMutation,
    DealFailPausedMutation,
    NewDealsSubscription,
    ProposalLogsListQuery,
    ProposalLogsCountQuery,
    RetrievalLogQuery,
    RetrievalLogsListQuery,
    RetrievalLogsCountQuery,
    IpniProviderInfoQuery,
    IpniAdQuery,
    IpniAdEntriesQuery,
    IpniAdEntriesCountQuery,
    IpniLatestAdQuery,
    IpniDistanceFromLatestAdQuery,
    PiecesWithPayloadCidQuery,
    PieceBuildIndexMutation,
    PieceStatusQuery,
    FlaggedPiecesQuery,
    FlaggedPiecesCountQuery,
    LIDQuery,
    StorageQuery,
    FundsQuery,
    FundsLogsQuery,
    DealPublishQuery,
    DealPublishNowMutation,
    FundsMoveToEscrow,
    StorageAskUpdate,
    TransfersQuery,
    TransferStatsQuery,
    MpoolQuery,
    SealingPipelineQuery,
    SectorStatusQuery,
    Libp2pAddrInfoQuery,
    StorageAskQuery,
    PublishPendingDealsMutation,
    PiecePayloadCidsQuery,
}
