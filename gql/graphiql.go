package gql

import (
	"net/http"
	"text/template"
)

// HTTP handler for Graphiql (GUI for making graphql requests)
func graphiql(httpPort int) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		t := template.Must(template.New("graphiql").Parse(`
  <!DOCTYPE html>
  <html>
       <head>
               <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/graphiql/0.11.10/graphiql.css" />
               <script src="https://cdnjs.cloudflare.com/ajax/libs/fetch/1.1.0/fetch.min.js"></script>
               <script src="https://cdnjs.cloudflare.com/ajax/libs/react/15.5.4/react.min.js"></script>
               <script src="https://cdnjs.cloudflare.com/ajax/libs/react/15.5.4/react-dom.min.js"></script>
               <script src="https://cdnjs.cloudflare.com/ajax/libs/graphiql/0.11.10/graphiql.js"></script>
               <script src="//unpkg.com/subscriptions-transport-ws@0.8.3/browser/client.js"></script>
               <script src="//unpkg.com/graphiql-subscriptions-fetcher@0.0.2/browser/client.js"></script>
       </head>
       <body style="width: 100%; height: 100%; margin: 0; overflow: hidden;">
               <div id="graphiql" style="height: 100vh;">Loading...</div>
               <script>
                       function graphQLFetcher(graphQLParams) {
                               return fetch("/graphql", {
                                       method: "post",
                                       body: JSON.stringify(graphQLParams),
                                       credentials: "include",
                               }).then(function (response) {
                                       return response.text();
                               }).then(function (responseBody) {
                                       try {
                                               return JSON.parse(responseBody);
                                       } catch (error) {
                                               return responseBody;
                                       }
                               });
                       }

                       var subscriptionsClient = new window.SubscriptionsTransportWs.SubscriptionClient('ws://localhost:{{ . }}/graphql', { reconnect: true });
                       var subscriptionsFetcher = window.GraphiQLSubscriptionsFetcher.graphQLFetcher(subscriptionsClient, graphQLFetcher);

                       ReactDOM.render(
                               React.createElement(GraphiQL, {fetcher: subscriptionsFetcher}),
                               document.getElementById("graphiql")
                       );
               </script>
       </body>
  </html>
  `))
		_ = t.Execute(w, httpPort)
	}
}
