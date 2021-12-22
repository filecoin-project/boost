package gql

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"path/filepath"
	"runtime"

	logging "github.com/ipfs/go-log/v2"

	"github.com/graph-gophers/graphql-go"
	"github.com/graph-gophers/graphql-go/relay"
	"github.com/graph-gophers/graphql-transport-ws/graphqlws"
)

var log = logging.Logger("gql")

const httpPort = 8080

type Server struct {
	resolver *resolver
}

func NewServer(resolver *resolver) *Server {
	return &Server{resolver: resolver}
}

func (s *Server) Serve(ctx context.Context) error {
	log.Info("graphql server: starting")

	// Get the absolute path to the directory that this go file is in
	_, goFilePath, _, ok := runtime.Caller(0)
	goFileDir := filepath.Dir(goFilePath)

	// Serve react app
	urlPath := "/"
	directory := "react/build"
	reactDir := path.Clean(filepath.Join(goFileDir, "..", directory))
	http.Handle(urlPath, http.StripPrefix(urlPath, http.FileServer(http.Dir(reactDir))))

	// Serve dummy deals
	err := serveDummyDeals()
	if err != nil {
		return err
	}

	// Graphiql handler (GUI for making graphql queries)
	http.HandleFunc("/graphiql", graphiql(httpPort))

	// Init graphQL schema
	if !ok {
		return fmt.Errorf("couldnt call runtime.Caller")
	}
	fpath := filepath.Join(goFileDir, "schema.graphql")

	schemaText, err := ioutil.ReadFile(fpath)
	if err != nil {
		return err
	}

	// Allow resolving directly to fields (instead of requiring resolvers to
	// have a method for every graphql field)
	opts := []graphql.SchemaOpt{graphql.UseFieldResolvers()}
	schema, err := graphql.ParseSchema(string(schemaText), s.resolver, opts...)
	if err != nil {
		return err
	}

	// graphQL handler
	queryHandler := &relay.Handler{Schema: schema}
	wsHandler := graphqlws.NewHandlerFunc(schema, queryHandler)

	go func() {
		listenAddr := fmt.Sprintf(":%d", httpPort)
		fmt.Printf("Graphql server listening on %s\n", listenAddr)
		http.Handle("/graphql/subscription", &corsHandler{wsHandler})
		http.Handle("/graphql/query", &corsHandler{queryHandler})

		_ = http.ListenAndServe(listenAddr, nil)
	}()

	return nil
}
