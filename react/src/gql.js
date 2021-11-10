import gql from "graphql-tag";
import ApolloClient from "apollo-client";
import {WebSocketLink} from "apollo-link-ws";
import {InMemoryCache} from "apollo-cache-inmemory";

const gqlClient = new ApolloClient({
    link: new WebSocketLink({
        uri: "ws://localhost:8080/graphql",
        options: {
            reconnect: true,
        },
    }),
    cache: new InMemoryCache(),
});

const DealsListQuery = gql`
    query AppDealsListQuery {
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

export {
    gqlClient,
    DealsListQuery,
    DealSubscription,
    DealCancelMutation,
    NewDealsSubscription,
}