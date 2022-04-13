package gql

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"golang.org/x/xerrors"

	"github.com/graph-gophers/graphql-go"
	"github.com/graph-gophers/graphql-go/relay"
	"github.com/graph-gophers/graphql-transport-ws/graphqlws"
	logging "github.com/ipfs/go-log/v2"
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
	mux := http.NewServeMux()
	err := serveReactApp(mux, goFileDir)
	if err != nil {
		return err
	}

	// Serve dummy deals
	err = serveDummyDeals(mux)
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
	wsOpts := []graphqlws.Option{
		// Add a 5 second timeout for writing responses to the web socket.
		// A lot of people will expose Boost over an ssh tunnel so the
		// connection may be quite laggy.
		graphqlws.WithWriteTimeout(5 * time.Second),
	}
	wsHandler := graphqlws.NewHandlerFunc(schema, queryHandler, wsOpts...)

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

func serveReactApp(mux *http.ServeMux, goFileDir string) error {
	// Catch all requests that are not handled by other handlers
	urlPath := "/"

	// Path from root of project to static react build
	directory := "react/build"
	reactDir := path.Clean(filepath.Join(goFileDir, "..", directory))
	reactFiles, err := ioutil.ReadDir(reactDir)
	if err != nil {
		if !xerrors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("reading files in react dir '%s': %w", reactDir, err)
		}

		log.Warnw("not serving react app: no files found in react directory", "directory", directory)
		return nil
	}

	// Serves files in the react dir
	reactApp := http.StripPrefix(urlPath, http.FileServer(http.Dir(reactDir)))

	mux.HandleFunc(urlPath, func(writer http.ResponseWriter, request *http.Request) {
		matchesFile := func() bool {
			// Check each file in the react build path for a match against
			// the URL path
			for _, f := range reactFiles {
				basePath := urlPath + f.Name()
				if f.IsDir() {
					// If the file is a directory, the URL must have the
					// directory as a prefix
					// eg "/static/somefile.js" matches "/static/"
					if strings.HasPrefix(request.URL.Path, basePath+"/") {
						return true
					}
				} else if request.URL.Path == basePath {
					// If it's not a directory, the file must be an exact match
					// eg favicon.ico
					return true
				}
			}
			return false
		}()
		if !matchesFile {
			// The URL doesn't match anything in the react build path, so just
			// serve the root of the react app.
			// The react app javascript will read the URL from the browser and
			// navigate to the path.
			// eg for the url "/storage-deals":
			// - serve the root react app from the server
			// - the react app javascript will navigate to "/storage-deals"
			request.URL.Path = "/"
		}
		reactApp.ServeHTTP(writer, request)
	})

	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	if err := s.srv.Shutdown(ctx); err != nil {
		return err
	}

	s.wg.Wait()

	return nil
}
