package gql

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
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

	// Serve react app
	path := "/"
	directory := "react/build"
	http.Handle(path, http.StripPrefix(path, http.FileServer(http.Dir(directory))))

	spath := "/static/"
	sdirectory := "react/build/static"
	http.Handle(spath, http.StripPrefix(spath, http.FileServer(http.Dir(sdirectory))))

	// Graphiql handler (GUI for making graphql queries)
	http.HandleFunc("/graphiql", graphiql(httpPort))

	// Init graphQL schema
	_, currentDir, _, ok := runtime.Caller(0)
	if !ok {
		return fmt.Errorf("couldnt call runtime.Caller")
	}
	fpath := filepath.Join(filepath.Dir(currentDir), "schema.graphql")

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
	graphQLHandler := graphqlws.NewHandlerFunc(schema, &relay.Handler{Schema: schema})

	go func() {
		listenAddr := fmt.Sprintf(":%d", httpPort)
		fmt.Printf("Graphql server listening on %s\n", listenAddr)
		http.Handle("/graphql", &corsHandler{graphQLHandler})

		_ = http.ListenAndServe(listenAddr, nil)
	}()

	return nil
}
