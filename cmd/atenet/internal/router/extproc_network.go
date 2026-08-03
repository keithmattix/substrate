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

package router

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	networkextprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/network_ext_proc/v3"
)

// substrateAuthorityMetadataKey is the field within SubstrateMetadataNamespace
// carrying the CONNECT request's :authority, captured once at connect_terminate
// by the connect_authority header_to_metadata filter (see xds.go's
// buildConnectTerminateHCM) and forwarded to this server as request metadata.
// There is no HTTP Host header at this layer -- this is the only source of
// which actor/atespace a raw TCP connection is addressed to.
const substrateAuthorityMetadataKey = "authority"

// Envoy's network ext_proc filter only ingests DynamicMetadata fields whose
// top-level key is allowlisted via MetadataOptions.ReceivingNamespaces (see
// buildTcpConnectFilterChain), merging that field's own (nested) value into
// the connection's dynamic metadata under the same key; the tcp_proxy leg's
// cluster needs an OriginalDstLbConfig.MetadataKey pointed at
// OriginalDstMetadataKey/OriginalDstAddressKey to pick it up. Requires Envoy
// >= envoyproxy/envoy@b27925c960 (first released in 1.39) for the
// ReceivingNamespaces field to exist at all.

// NetworkExtProcServer implements Envoy's network (L4) external processing
// gRPC server for CONNECT-tunneled TCP traffic reinjected through
// main_internal (see xds.go's buildMainInternalListener/buildTcpConnectFilterChain).
// Unlike ExtProcServer, it never sees an HTTP Host header to resolve the actor
// from -- the connect_terminate listener already captured the tunnel's
// authority into dynamic metadata when the CONNECT was established, and that
// is all this server needs: there is nothing to inspect on the wire, and the
// routing decision is made once, from that one piece of metadata, at the
// start of the connection.
type NetworkExtProcServer struct {
	resumer *ActorResumer
}

// NewNetworkExtProcServer constructs a NetworkExtProcServer. Unlike
// ExtProcServer it does not park: parking exists to retry/hold a request while
// an actor's worker pool is briefly saturated, and how that interacts with a
// long-lived TCP connection (whose ext_proc round trip guards a full data
// frame's worth of end-user latency rather than a single header exchange) is
// not yet understood -- resume failures here fail fast instead.
func NewNetworkExtProcServer(apiClient ateapipb.ControlClient) *NetworkExtProcServer {
	return &NetworkExtProcServer{
		resumer: NewActorResumer(apiClient),
	}
}

// Serve serves the NetworkExternalProcessor gRPC service on lis until ctx is
// canceled or the server fails.
func (s *NetworkExtProcServer) Serve(ctx context.Context, lis net.Listener) error {
	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	networkextprocv3.RegisterNetworkExternalProcessorServer(grpcServer, s)

	errChan := make(chan error, 1)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			errChan <- err
		}
	}()

	select {
	case <-ctx.Done():
		grpcServer.GracefulStop()
		return nil
	case err := <-errChan:
		return err
	}
}

// Process implements the bidirectional NetworkExternalProcessor stream. Envoy
// opens one stream per TCP connection and sends one ProcessingRequest per data
// frame in whichever direction(s) ProcessingMode enables (see
// buildTcpConnectFilterChain); only the first request -- which carries the
// connection's forwarded metadata -- is needed to decide whether, and where,
// to route the connection. Every later frame passes through unmodified.
func (s *NetworkExtProcServer) Process(stream networkextprocv3.NetworkExternalProcessor_ProcessServer) error {
	var routed bool
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		if routed {
			if err := stream.Send(passthroughResponse()); err != nil {
				return err
			}
			continue
		}

		resp, err := s.handleFirstFrame(stream.Context(), req)
		if err != nil {
			slog.ErrorContext(stream.Context(), "Error during network ext_proc processing", slog.String("err", err.Error()))
			// The connection's fate is decided; there is nothing useful left
			// to do with this stream.
			return stream.Send(closeResponse())
		}
		routed = true
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

// handleFirstFrame resumes the actor identified by the connection's forwarded
// substrate authority metadata and builds the CONTINUE response that routes
// the connection to that actor's worker.
//
// TODO(router): authority is always empty today (see SubstrateMetadataNamespace's
// doc comment) -- dynamic metadata doesn't survive the connect_terminate ->
// main_internal internal-listener hop, and NetworkExternalProcessor has no
// filter-state-reading mechanism (no request_attributes) to fall back on the
// way the HTTP leg's handleRequestHeaders does. Until Envoy adds one, every
// CONNECT scenario that reaches this raw TCP leg -- non-HTTP payloads, and any
// TLS-wrapped payload per buildMainInternalListener's transport-protocol
// match -- fails here.
func (s *NetworkExtProcServer) handleFirstFrame(ctx context.Context, req *networkextprocv3.ProcessingRequest) (*networkextprocv3.ProcessingResponse, error) {
	authority := req.GetMetadata().GetFilterMetadata()[SubstrateMetadataNamespace].GetFields()[substrateAuthorityMetadataKey].GetStringValue()
	if authority == "" {
		return nil, fmt.Errorf("network ext_proc request missing %s/%s metadata", SubstrateMetadataNamespace, substrateAuthorityMetadataKey)
	}

	actorRef, err := parseActorRef(authority)
	if err != nil {
		return nil, fmt.Errorf("invalid authority %q: %w", authority, err)
	}

	slog.InfoContext(ctx, "ResumeActor", slog.Any("actor", actorRef))
	actor, _, err := s.resumer.ResumeActor(ctx, actorRef)
	if err != nil {
		return nil, fmt.Errorf("resuming actor %s: %w", actorRef, err)
	}

	workerIP := actor.GetAteomPodIp()
	if net.ParseIP(workerIP) == nil {
		return nil, fmt.Errorf("actor %s routing failed: invalid worker IP %q", actorRef, workerIP)
	}
	// The actor is reached through the same in-worker atunnel ingress server as
	// the HTTP leg (see extproc.go). Unlike the HTTP leg, there is no header to
	// carry the CONNECT authority's arbitrary port through atunnel's reverse
	// proxy on this raw TCP leg -- untouched for now, pending its own fix.
	targetAddr := net.JoinHostPort(workerIP, "443")
	slog.InfoContext(ctx, "Route ok", slog.Any("actor", actorRef), slog.String("targetAddr", targetAddr))

	dynamicMetadata, err := structpb.NewStruct(map[string]any{
		OriginalDstMetadataKey: map[string]any{
			OriginalDstAddressKey: targetAddr,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("building dynamic metadata for actor %s: %w", actorRef, err)
	}

	return &networkextprocv3.ProcessingResponse{
		DataProcessingStatus: networkextprocv3.ProcessingResponse_UNMODIFIED,
		ConnectionStatus:     networkextprocv3.ProcessingResponse_CONTINUE,
		DynamicMetadata:      dynamicMetadata,
	}, nil
}

// passthroughResponse continues a connection whose routing decision was
// already made by handleFirstFrame, leaving the data frame unmodified.
func passthroughResponse() *networkextprocv3.ProcessingResponse {
	return &networkextprocv3.ProcessingResponse{
		DataProcessingStatus: networkextprocv3.ProcessingResponse_UNMODIFIED,
		ConnectionStatus:     networkextprocv3.ProcessingResponse_CONTINUE,
	}
}

// closeResponse rejects the connection outright, e.g. because its authority
// metadata was missing or the actor failed to resume.
func closeResponse() *networkextprocv3.ProcessingResponse {
	return &networkextprocv3.ProcessingResponse{
		ConnectionStatus: networkextprocv3.ProcessingResponse_CLOSE_RST,
	}
}
