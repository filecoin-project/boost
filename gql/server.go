package gql

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"path/filepath"
	"runtime"
	"sync"

	logging "github.com/ipfs/go-log/v2"

	"github.com/graph-gophers/graphql-go"
	"github.com/graph-gophers/graphql-go/relay"
	"github.com/graph-gophers/graphql-transport-ws/graphqlws"
)

var log = logging.Logger("gql")

const httpPort = 8080

type Server struct {
	resolver *resolver
	srv      *http.Server
	wg       sync.WaitGroup
}

func NewServer(resolver *resolver) *Server {
	return &Server{resolver: resolver}
}

func (s *Server) Start(ctx context.Context) error {
	log.Info("graphql server: starting")

	// Get the absolute path to the directory that this go file is in
	_, goFilePath, _, ok := runtime.Caller(0)
	goFileDir := filepath.Dir(goFilePath)

	// Serve React app
	urlPath := "/"
	directory := "react/build"
	reactDir := path.Clean(filepath.Join(goFileDir, "..", directory))
	mux := http.NewServeMux()
	mux.Handle(urlPath, http.StripPrefix(urlPath, http.FileServer(http.Dir(reactDir))))

	// Serve dummy deals
	err := serveDummyDeals()
	if err != nil {
		return err
	}

	// GraphQL handler (GUI for making GraphQL queries)
	mux.HandleFunc("/graphiql", graphiql(httpPort))

	// Init GraphQL schema
	if !ok {
		return fmt.Errorf("couldnt call runtime.Caller")
	}
	fpath := filepath.Join(goFileDir, "schema.graphql")

	schemaText, err := ioutil.ReadFile(fpath)
	if err != nil {
		return err
	}

	// Allow resolving directly to fields (instead of requiring resolvers to
	// have a method for every GraphQL field)
	opts := []graphql.SchemaOpt{graphql.UseFieldResolvers()}
	schema, err := graphql.ParseSchema(string(schemaText), s.resolver, opts...)
	if err != nil {
		return err
	}

	// GraphQL handler
	queryHandler := &relay.Handler{Schema: schema}
	wsHandler := graphqlws.NewHandlerFunc(schema, queryHandler)

	listenAddr := fmt.Sprintf(":%d", httpPort)
	s.srv = &http.Server{Addr: listenAddr, Handler: mux}
	fmt.Printf("Graphql server listening on %s\n", listenAddr)
	mux.Handle("/graphql/subscription", &corsHandler{wsHandler})
	mux.Handle("/graphql/query", &corsHandler{queryHandler})

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		if err := s.srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("gql.ListenAndServe(): %v", err)
		}
	}()

	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	if err := s.srv.Shutdown(ctx); err != nil {
		return err
	}

	s.wg.Wait()

	return nil
}
