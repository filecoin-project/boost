package gql

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/filecoin-project/boost/db"
	"github.com/graph-gophers/graphql-go"
	"github.com/graph-gophers/graphql-go/relay"
	"github.com/graph-gophers/graphql-transport-ws/graphqlws"
)

const httpPort = 8080

func DemoServer() {
	listenAddr := fmt.Sprintf(":%d", httpPort)
	err := runDemoServer(listenAddr)
	if err != nil {
		fmt.Printf("%s\n", err)
	}
}

func runDemoServer(listenAddr string) error {
	// Create a temporary DB with some data for demo purposes
	dbPath, err := db.CreateTmpDB()
	if err != nil {
		return err
	}
	defer os.Remove(dbPath)

	// Open a connection to the database
	fmt.Printf("Opening database %s\n", dbPath)
	dealsDB, err := db.Open(dbPath)
	if err != nil {
		return err
	}

	// Start GraphQL server
	return Serve(listenAddr, dealsDB)
}

func Serve(listenAddr string, dealsDB *db.DealsDB) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Graphiql handler (GUI for making graphql queries)
	http.HandleFunc("/", graphiql(httpPort))

	// Init graphQL schema
	schemaText, err := ioutil.ReadFile("gql/schema.graphql")
	if err != nil {
		return err
	}

	// Allow resolving directly to fields (instead of requiring resolvers to
	// have a method for every graphql field)
	opts := []graphql.SchemaOpt{graphql.UseFieldResolvers()}
	schema, err := graphql.ParseSchema(string(schemaText), newResolver(ctx, dealsDB), opts...)
	if err != nil {
		return err
	}

	// graphQL handler
	graphQLHandler := graphqlws.NewHandlerFunc(schema, &relay.Handler{Schema: schema})

	fmt.Printf("graphql server listening on %s\n", listenAddr)
	http.Handle("/graphql", &corsHandler{graphQLHandler})

	return http.ListenAndServe(listenAddr, nil)
}
