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
               <script src="https://unpkg.com/graphql-ws@5.16.2/umd/graphql-ws.min.js"></script>
       </head>
       <body style="width: 100%; height: 100%; margin: 0; overflow: hidden;">
               <div id="graphiql" style="height: 100vh;">Loading...</div>
               <script>
                       function isSubscriptionOperation(query) {
                               if (typeof query !== "string") {
                                       return false;
                               }
                               return /^\s*subscription[\s({]/m.test(query);
                       }

                       var wsProtocol = window.location.protocol === "https:" ? "wss" : "ws";
                       var wsClient = window.graphqlWs.createClient({
                               url: wsProtocol + "://" + window.location.host + "/graphql/subscription",
                               lazy: true,
                               retryAttempts: Number.MAX_SAFE_INTEGER,
                       });

                       function graphQLFetcher(graphQLParams) {
                               if (isSubscriptionOperation(graphQLParams.query)) {
                                       return {
                                               subscribe: function (sink) {
                                                       var dispose = wsClient.subscribe(graphQLParams, sink);
                                                       return {
                                                               unsubscribe: dispose,
                                                       };
                                               },
                                       };
                               }

                               return fetch("/graphql/query", {
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

                       ReactDOM.render(
                               React.createElement(GraphiQL, {fetcher: graphQLFetcher}),
                               document.getElementById("graphiql")
                       );
               </script>
       </body>
  </html>
  `))
		_ = t.Execute(w, httpPort)
	}
}
