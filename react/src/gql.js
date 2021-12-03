import gql from "graphql-tag";
import { ApolloClient, HttpLink, split } from "@apollo/client";
import { getMainDefinition } from '@apollo/client/utilities';
import {WebSocketLink} from "apollo-link-ws";
import {InMemoryCache} from "apollo-cache-inmemory";
import { parseDates } from "./hooks";

const graphqlEndpoint = "localhost:8080"

// HTTP Link
const httpLink = new HttpLink({
    uri: `http://${graphqlEndpoint}/graphql/query`,
});

// WebSocket Link
const wsLink = new WebSocketLink({
    uri: `ws://${graphqlEndpoint}/graphql/subscription`,
    options: {
        reconnect: true,
        lazy: true,
    },
});

// Send query request based on the type definition
const link = split(
    ({ query }) => {
        const definition = getMainDefinition(query);
        return (
            definition.kind === 'OperationDefinition' &&
            definition.operation === 'subscription'
        );
    },
    wsLink,
    httpLink
);

const cache = new InMemoryCache();

const gqlClient = new ApolloClient({
    link,
    cache
});

const gqlQuery = function(...args) {
    var res = gqlClient.query.apply(gqlClient, args)
    return res.then(ret => {
        if (ret && ret.data) {
            parseDates(ret.data)
        }
        return ret
    })
}

const gqlSubscribe = function(...args) {
    return gqlClient.subscribe.apply(gqlClient, args)
}

const DealsListQuery = gql`
    query AppDealsListQuery($first: ID, $limit: Int) {
        deals(first: $first, limit: $limit) {
            deals {
                ID
                CreatedAt
                PieceCid
                PieceSize
                ClientAddress
                Message
                Logs {
                    CreatedAt
                    Text
                }
            }
            totalCount
            next
        }
    }
`;

const DealSubscription = gql`
    subscription AppDealSubscription($id: ID!) {
        dealUpdate(id: $id) {
            ID
            CreatedAt
            PieceCid
            PieceSize
            ClientAddress
            Message
            Logs {
                CreatedAt
                Text
            }
        }
    }
`;

const DealCancelMutation = gql`
    mutation AppDealCancelMutation($id: ID!) {
        dealCancel(id: $id)
    }
`;

const NewDealsSubscription = gql`
    subscription AppNewDealsSubscription {
        dealNew {
            ID
            CreatedAt
            PieceCid
            PieceSize
            ClientAddress
            Message
            Logs {
                CreatedAt
                Text
            }
        }
    }
`;

const StorageQuery = gql`
    query AppStorageQuery {
        storage {
            Completed
            Transferring
            Pending
            Free
            MountPoint
        }
    }
`;

const FundsQuery = gql`
    query AppFundsQuery {
        funds {
            Name
            Amount
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

const FundsLogsQuery = gql`
    query AppFundsLogsQuery {
        fundsLogs {
            totalCount
            next
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
            Start
            Period
            MaxDealsPerMsg
            Deals {
                ID
                CreatedAt
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
    mutation AppDealPublishNowMutation($amount: Float!) {
        fundsMoveToEscrow(amount: $amount)
    }
`;

const MpoolQuery = gql`
    query AppMpoolQuery($local: Boolean!) {
        mpool(local: $local) {
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
`;

export {
    gqlClient,
    gqlQuery,
    gqlSubscribe,
    DealsListQuery,
    DealSubscription,
    DealCancelMutation,
    NewDealsSubscription,
    StorageQuery,
    FundsQuery,
    FundsLogsQuery,
    DealPublishQuery,
    DealPublishNowMutation,
    FundsMoveToEscrow,
    TransfersQuery,
    MpoolQuery,
}
