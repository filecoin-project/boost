package gql

import (
	"context"
	_ "embed"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/filecoin-project/boost/react"
	"github.com/graph-gophers/graphql-go"
	"github.com/graph-gophers/graphql-go/relay"
	"github.com/graph-gophers/graphql-transport-ws/graphqlws"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"
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

//go:embed schema.graphql
var schemaGraqhql string

func (s *Server) Start(ctx context.Context) error {
	log.Info("graphql server: starting")

	// Serve React app
	mux := http.NewServeMux()
	err := serveReactApp(mux)
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

	// Allow resolving directly to fields (instead of requiring resolvers to
	// have a method for every GraphQL field)
	opts := []graphql.SchemaOpt{graphql.UseFieldResolvers()}
	schema, err := graphql.ParseSchema(string(schemaGraqhql), s.resolver, opts...)
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

// fsPrefix adds a prefix to all Open() calls
type fsPrefix struct {
	fs.FS
	prefix string
}

var _ fs.FS = (*fsPrefix)(nil)

func (f *fsPrefix) Open(name string) (fs.File, error) {
	if name == "." || name == "" {
		return f.FS.Open(f.prefix)
	}
	return f.FS.Open(f.prefix + "/" + name)
}

func serveReactApp(mux *http.ServeMux) error {
	// Catch all requests that are not handled by other handlers
	urlPath := "/"

	reactFiles, err := react.BuildDir.ReadDir("build")
	if err != nil {
		if !xerrors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("reading files in embedded react build dir: %w", err)
		}

		log.Warnw("not serving react app: no files found in embedded react build directory - " +
			"you may need to build the boost react app")
		return nil
	}

	if len(reactFiles) == 1 && reactFiles[0].Name() == "README.md" {
		log.Warnw("not serving react app: no files found in embedded react build directory - " +
			"you may need to build the boost react app")
		return nil
	}

	// Serves files in the react build dir
	reactFS := &fsPrefix{
		FS:     react.BuildDir,
		prefix: "build",
	}
	reactApp := http.StripPrefix(urlPath, http.FileServer(http.FS(reactFS)))

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
