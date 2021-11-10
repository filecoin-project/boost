package gql

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"

	logging "github.com/ipfs/go-log/v2"

	"github.com/filecoin-project/boost/storagemarket"

	"github.com/filecoin-project/boost/db"
	"github.com/graph-gophers/graphql-go"
	"github.com/graph-gophers/graphql-go/relay"
	"github.com/graph-gophers/graphql-transport-ws/graphqlws"
)

var log = logging.Logger("gql")

const httpPort = 8080

type Server struct {
	provider *storagemarket.Provider
	db       *db.DealsDB
}

func NewServer(prov *storagemarket.Provider, db *db.DealsDB) *Server {
	return &Server{provider: prov, db: db}
}

func (s *Server) Serve(ctx context.Context) error {
	log.Info("graphql server: starting")

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
	schema, err := graphql.ParseSchema(string(schemaText), newResolver(ctx, s.db, s.provider), opts...)
	if err != nil {
		return err
	}

	// graphQL handler
	graphQLHandler := graphqlws.NewHandlerFunc(schema, &relay.Handler{Schema: schema})

	go func() {
		listenAddr := fmt.Sprintf(":%d", httpPort)
		fmt.Printf("Graphql server listening on %s\n", listenAddr)
		http.Handle("/graphql", &corsHandler{graphQLHandler})

		_ = http.ListenAndServe(listenAddr, nil)
	}()

	log.Info("graphql server: started")
	return nil
}
