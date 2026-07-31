// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package e2e

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/portforward"
	"github.com/agent-substrate/substrate/internal/resources"
	"k8s.io/client-go/kubernetes"
)

const (
	routerNamespace = "ate-system"
	routerService   = "atenet-router"
)

// RouterClient sends HTTP requests to actors through the atenet router, the
// same way real traffic arrives (so the request is routed and, if needed, the
// actor is resumed). It port-forwards the router Service, mirroring the
// approach in internal/ateclient.
type RouterClient struct {
	addr    string
	baseURL string
	http    *http.Client
	stop    func()
}

// NewRouterClient establishes a port-forward to the atenet router. Call Close
// to tear it down.
func NewRouterClient(ctx context.Context) (*RouterClient, error) {
	config, err := ateclient.LoadConfig(KubeConfig, KubeContext)
	if err != nil {
		return nil, fmt.Errorf("loading kubeconfig: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("creating k8s client: %w", err)
	}

	localPort, stop, err := portforward.ServicePortForward(ctx, config, clientset, routerNamespace, routerService, 80)
	if err != nil {
		return nil, err
	}

	return &RouterClient{
		addr:    fmt.Sprintf("127.0.0.1:%d", localPort),
		baseURL: fmt.Sprintf("http://127.0.0.1:%d", localPort),
		http:    &http.Client{Timeout: 30 * time.Second},
		stop:    stop,
	}, nil
}

// Close stops the port-forward tunnel.
func (c *RouterClient) Close() {
	c.stop()
}

// Get issues GET path to actor through the router, setting the actor's mesh Host
// so the router routes (and resumes) it. The caller must close the body.
func (c *RouterClient) Get(ctx context.Context, actorRef resources.ActorRef, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	// The router routes on the Host/:authority, not a header.
	req.Host = actorRef.DNSName()
	return c.http.Do(req)
}

// Connect opens a raw HTTP CONNECT tunnel through the router to port on the
// actor, mirroring how atenet-router's arbitrary-port ingress support is
// reached: the target port is carried in the CONNECT request's :authority
// (actorRef.DNSName():port), not a header. On success the returned net.Conn
// carries the actor's raw response bytes; the caller must close it.
func (c *RouterClient) Connect(ctx context.Context, actorRef resources.ActorRef, port int) (net.Conn, error) {
	destination := fmt.Sprintf("%s:%d", actorRef.DNSName(), port)

	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return nil, fmt.Errorf("connecting to router: %w", err)
	}

	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Host: destination},
		Host:   destination,
	}
	if err := req.Write(conn); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("writing CONNECT request: %w", err)
	}

	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, req)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("reading CONNECT response: %w", err)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		_ = resp.Body.Close()
		_ = conn.Close()
		message := strings.TrimSpace(string(body))
		if message == "" {
			message = http.StatusText(resp.StatusCode)
		}
		return nil, fmt.Errorf("router rejected CONNECT to %s with %s: %s", destination, resp.Status, message)
	}

	return &bufferedConn{Conn: conn, reader: reader}, nil
}

// bufferedConn serves reads from a bufio.Reader that may already hold bytes
// buffered past an HTTP response's header boundary, while writes and the
// remaining net.Conn behavior pass straight through.
type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}
