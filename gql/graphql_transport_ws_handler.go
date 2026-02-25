package gql

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/graph-gophers/graphql-go"
)

const (
	transportWSProtocol = "graphql-transport-ws"

	transportWSConnectionInit = "connection_init"
	transportWSConnectionAck  = "connection_ack"
	transportWSPing           = "ping"
	transportWSPong           = "pong"
	transportWSSubscribe      = "subscribe"
	transportWSNext           = "next"
	transportWSComplete       = "complete"
	transportWSError          = "error"

	transportWSCloseBadRequest   = 4400
	transportWSCloseUnauthorized = 4401
	transportWSCloseTooManyInit  = 4429
)

type transportWSMessage struct {
	ID      string          `json:"id,omitempty"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type transportWSSubscribePayload struct {
	Query         string                 `json:"query"`
	OperationName string                 `json:"operationName,omitempty"`
	Variables     map[string]interface{} `json:"variables,omitempty"`
}

type graphQLTransportWSHandler struct {
	schema       *graphql.Schema
	writeTimeout time.Duration
}

var graphQLTransportWSUpgrader = websocket.Upgrader{
	CheckOrigin:  func(r *http.Request) bool { return true },
	Subprotocols: []string{transportWSProtocol},
}

func newGraphQLTransportWSHandler(schema *graphql.Schema) http.Handler {
	return &graphQLTransportWSHandler{
		schema:       schema,
		writeTimeout: 5 * time.Second,
	}
}

func (h *graphQLTransportWSHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := graphQLTransportWSUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	if conn.Subprotocol() != transportWSProtocol {
		_ = h.closeWithCode(conn, transportWSCloseBadRequest, "unsupported websocket subprotocol")
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	var (
		writeMu       sync.Mutex
		subscriptions = make(map[string]context.CancelFunc)
		subsMu        sync.Mutex
		initialized   bool
	)

	writeMessage := func(msg transportWSMessage) error {
		writeMu.Lock()
		defer writeMu.Unlock()

		if err := conn.SetWriteDeadline(time.Now().Add(h.writeTimeout)); err != nil {
			return err
		}
		return conn.WriteJSON(msg)
	}

	cancelSubscription := func(id string) {
		subsMu.Lock()
		cancelSub, ok := subscriptions[id]
		if ok {
			delete(subscriptions, id)
		}
		subsMu.Unlock()
		if ok {
			cancelSub()
		}
	}

	cancelAllSubscriptions := func() {
		subsMu.Lock()
		localSubs := subscriptions
		subscriptions = make(map[string]context.CancelFunc)
		subsMu.Unlock()

		for _, cancelSub := range localSubs {
			cancelSub()
		}
	}
	defer cancelAllSubscriptions()

	for {
		var msg transportWSMessage
		if err := conn.ReadJSON(&msg); err != nil {
			return
		}

		switch msg.Type {
		case transportWSConnectionInit:
			if initialized {
				_ = h.closeWithCode(conn, transportWSCloseTooManyInit, "connection already initialised")
				return
			}
			initialized = true
			if err := writeMessage(transportWSMessage{Type: transportWSConnectionAck}); err != nil {
				return
			}

		case transportWSPing:
			if err := writeMessage(transportWSMessage{Type: transportWSPong, Payload: msg.Payload}); err != nil {
				return
			}

		case transportWSPong:
			continue

		case transportWSSubscribe:
			if !initialized {
				_ = h.closeWithCode(conn, transportWSCloseUnauthorized, "connection has not been initialised")
				return
			}

			if msg.ID == "" {
				if err := writeMessage(transportWSMessage{Type: transportWSError, Payload: transportWSErrorPayload("missing operation id")}); err != nil {
					return
				}
				continue
			}

			if len(msg.Payload) == 0 {
				if err := writeMessage(transportWSMessage{ID: msg.ID, Type: transportWSError, Payload: transportWSErrorPayload("missing subscribe payload")}); err != nil {
					return
				}
				if err := writeMessage(transportWSMessage{ID: msg.ID, Type: transportWSComplete}); err != nil {
					return
				}
				continue
			}

			var payload transportWSSubscribePayload
			if err := json.Unmarshal(msg.Payload, &payload); err != nil {
				if err := writeMessage(transportWSMessage{ID: msg.ID, Type: transportWSError, Payload: transportWSErrorPayload(fmt.Sprintf("invalid subscribe payload: %s", err))}); err != nil {
					return
				}
				if err := writeMessage(transportWSMessage{ID: msg.ID, Type: transportWSComplete}); err != nil {
					return
				}
				continue
			}

			if payload.Query == "" {
				if err := writeMessage(transportWSMessage{ID: msg.ID, Type: transportWSError, Payload: transportWSErrorPayload("missing GraphQL query")}); err != nil {
					return
				}
				if err := writeMessage(transportWSMessage{ID: msg.ID, Type: transportWSComplete}); err != nil {
					return
				}
				continue
			}

			opCtx, opCancel := context.WithCancel(ctx)
			cancelSubscription(msg.ID)

			subsMu.Lock()
			subscriptions[msg.ID] = opCancel
			subsMu.Unlock()

			go h.runOperation(opCtx, msg.ID, payload, writeMessage, func() {
				cancelSubscription(msg.ID)
			})

		case transportWSComplete:
			if msg.ID != "" {
				cancelSubscription(msg.ID)
			}

		default:
			_ = h.closeWithCode(conn, transportWSCloseBadRequest, fmt.Sprintf("unsupported message type: %s", msg.Type))
			return
		}
	}
}

func (h *graphQLTransportWSHandler) runOperation(ctx context.Context, operationID string, payload transportWSSubscribePayload, writeMessage func(transportWSMessage) error, onDone func()) {
	defer onDone()

	responses, err := h.schema.Subscribe(ctx, payload.Query, payload.OperationName, payload.Variables)
	if err != nil {
		_ = writeMessage(transportWSMessage{ID: operationID, Type: transportWSError, Payload: transportWSErrorPayload(err.Error())})
		_ = writeMessage(transportWSMessage{ID: operationID, Type: transportWSComplete})
		return
	}

	for responseValue := range responses {
		response, ok := responseValue.(*graphql.Response)
		if !ok {
			_ = writeMessage(transportWSMessage{ID: operationID, Type: transportWSError, Payload: transportWSErrorPayload("invalid subscription response type")})
			_ = writeMessage(transportWSMessage{ID: operationID, Type: transportWSComplete})
			return
		}

		execPayload, err := transportWSExecutionPayload(response)
		if err != nil {
			_ = writeMessage(transportWSMessage{ID: operationID, Type: transportWSError, Payload: transportWSErrorPayload(err.Error())})
			_ = writeMessage(transportWSMessage{ID: operationID, Type: transportWSComplete})
			return
		}

		if err := writeMessage(transportWSMessage{ID: operationID, Type: transportWSNext, Payload: execPayload}); err != nil {
			return
		}
	}

	_ = writeMessage(transportWSMessage{ID: operationID, Type: transportWSComplete})
}

func transportWSExecutionPayload(resp *graphql.Response) (json.RawMessage, error) {
	result := map[string]interface{}{}
	if len(resp.Data) > 0 {
		result["data"] = json.RawMessage(resp.Data)
	}
	if len(resp.Errors) > 0 {
		result["errors"] = resp.Errors
	}
	if len(resp.Extensions) > 0 {
		result["extensions"] = resp.Extensions
	}
	if len(result) == 0 {
		result["data"] = nil
	}

	payload, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("marshalling subscription payload: %w", err)
	}
	return payload, nil
}

func transportWSErrorPayload(message string) json.RawMessage {
	payload, _ := json.Marshal([]map[string]string{{"message": message}})
	return payload
}

func (h *graphQLTransportWSHandler) closeWithCode(conn *websocket.Conn, code int, reason string) error {
	return conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, reason), time.Now().Add(h.writeTimeout))
}
