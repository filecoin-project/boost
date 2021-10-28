/**
 * @flow
 */

/* eslint-disable */

'use strict';

/*::
import type { ConcreteRequest } from 'relay-runtime';
export type AppDealSubscriptionVariables = {|
  id: string
|};
export type AppDealSubscriptionResponse = {|
  +dealSub: ?{|
    +ID: string,
    +CreatedAt: any,
    +PieceCid: string,
    +PieceSize: number,
    +Client: string,
    +State: string,
    +Logs: $ReadOnlyArray<?{|
      +CreatedAt: any,
      +Text: string,
    |}>,
  |}
|};
export type AppDealSubscription = {|
  variables: AppDealSubscriptionVariables,
  response: AppDealSubscriptionResponse,
|};
*/


/*
subscription AppDealSubscription(
  $id: ID!
) {
  dealSub(id: $id) {
    ID
    CreatedAt
    PieceCid
    PieceSize
    Client
    State
    Logs {
      CreatedAt
      Text
    }
  }
}
*/

const node/*: ConcreteRequest*/ = (function(){
var v0 = [
  {
    "defaultValue": null,
    "kind": "LocalArgument",
    "name": "id"
  }
],
v1 = {
  "alias": null,
  "args": null,
  "kind": "ScalarField",
  "name": "CreatedAt",
  "storageKey": null
},
v2 = [
  {
    "alias": null,
    "args": [
      {
        "kind": "Variable",
        "name": "id",
        "variableName": "id"
      }
    ],
    "concreteType": "Deal",
    "kind": "LinkedField",
    "name": "dealSub",
    "plural": false,
    "selections": [
      {
        "alias": null,
        "args": null,
        "kind": "ScalarField",
        "name": "ID",
        "storageKey": null
      },
      (v1/*: any*/),
      {
        "alias": null,
        "args": null,
        "kind": "ScalarField",
        "name": "PieceCid",
        "storageKey": null
      },
      {
        "alias": null,
        "args": null,
        "kind": "ScalarField",
        "name": "PieceSize",
        "storageKey": null
      },
      {
        "alias": null,
        "args": null,
        "kind": "ScalarField",
        "name": "Client",
        "storageKey": null
      },
      {
        "alias": null,
        "args": null,
        "kind": "ScalarField",
        "name": "State",
        "storageKey": null
      },
      {
        "alias": null,
        "args": null,
        "concreteType": "DealLog",
        "kind": "LinkedField",
        "name": "Logs",
        "plural": true,
        "selections": [
          (v1/*: any*/),
          {
            "alias": null,
            "args": null,
            "kind": "ScalarField",
            "name": "Text",
            "storageKey": null
          }
        ],
        "storageKey": null
      }
    ],
    "storageKey": null
  }
];
return {
  "fragment": {
    "argumentDefinitions": (v0/*: any*/),
    "kind": "Fragment",
    "metadata": null,
    "name": "AppDealSubscription",
    "selections": (v2/*: any*/),
    "type": "RootSubscription",
    "abstractKey": null
  },
  "kind": "Request",
  "operation": {
    "argumentDefinitions": (v0/*: any*/),
    "kind": "Operation",
    "name": "AppDealSubscription",
    "selections": (v2/*: any*/)
  },
  "params": {
    "cacheID": "4cfeaa1178fad2ab31d9d9e19ff4fdcc",
    "id": null,
    "metadata": {},
    "name": "AppDealSubscription",
    "operationKind": "subscription",
    "text": "subscription AppDealSubscription(\n  $id: ID!\n) {\n  dealSub(id: $id) {\n    ID\n    CreatedAt\n    PieceCid\n    PieceSize\n    Client\n    State\n    Logs {\n      CreatedAt\n      Text\n    }\n  }\n}\n"
  }
};
})();
// prettier-ignore
(node/*: any*/).hash = '35242bf1ebcf870125737b97cba2d823';

module.exports = node;
