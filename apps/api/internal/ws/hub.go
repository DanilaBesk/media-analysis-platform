package ws

import (
	"context"
	"fmt"
	"net/http"
	neturl "net/url"
	"strings"
	"sync"

	"golang.org/x/net/websocket"
)

type Hub struct {
	mu      sync.RWMutex
	clients map[*websocketClient]struct{}
}

type websocketClient struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func NewHub() *Hub {
	return &Hub{
		clients: map[*websocketClient]struct{}{},
	}
}

func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	server := websocket.Server{
		Handshake: allowLocalWebsocketOrigin,
		Handler: func(conn *websocket.Conn) {
			h.handleConn(conn)
		},
	}
	server.ServeHTTP(w, r)
}

func (h *Hub) handleConn(conn *websocket.Conn) {
	client := &websocketClient{conn: conn}
	h.add(client)
	defer h.remove(client)

	for {
		var ignored string
		if err := websocket.Message.Receive(conn, &ignored); err != nil {
			return
		}
	}
}

func allowLocalWebsocketOrigin(_ *websocket.Config, r *http.Request) error {
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		return nil
	}
	parsed, err := neturl.Parse(origin)
	if err != nil {
		return fmt.Errorf("parse websocket origin: %w", err)
	}
	if isLoopbackHost(parsed.Hostname()) {
		return nil
	}
	if strings.EqualFold(parsed.Hostname(), requestHostname(r.Host)) {
		return nil
	}
	return fmt.Errorf("websocket origin %q is not allowed", origin)
}

func isLoopbackHost(host string) bool {
	switch strings.ToLower(strings.Trim(host, "[]")) {
	case "localhost", "127.0.0.1", "::1":
		return true
	default:
		return false
	}
}

func requestHostname(hostport string) string {
	parsed, err := neturl.Parse("//" + hostport)
	if err == nil && parsed.Hostname() != "" {
		return parsed.Hostname()
	}
	return hostport
}

func (h *Hub) Broadcast(ctx context.Context, envelope JobEventEnvelope) error {
	clients := h.snapshotClients()
	for _, client := range clients {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := client.send(envelope); err != nil {
			h.remove(client)
		}
	}
	return nil
}

func (h *Hub) add(client *websocketClient) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[client] = struct{}{}
}

func (h *Hub) remove(client *websocketClient) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, client)
	_ = client.conn.Close()
}

func (h *Hub) snapshotClients() []*websocketClient {
	h.mu.RLock()
	defer h.mu.RUnlock()
	clients := make([]*websocketClient, 0, len(h.clients))
	for client := range h.clients {
		clients = append(clients, client)
	}
	return clients
}

func (c *websocketClient) send(envelope JobEventEnvelope) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return websocket.JSON.Send(c.conn, envelope)
}
