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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"time"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/atunnel"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// ExtProcServer implements the Envoy external processing gRPC server
// to dynamically manage actor activations based on request traffic.
type ExtProcServer struct {
	port              int
	apiClient         ateapipb.ControlClient
	recorder          *QueryRecorder
	resumer           *ActorResumer
	routeDuration     metric.Float64Histogram
	parking           *parkingLot
	routeViaAuthority bool
}

func NewExtProcServer(port int, apiClient ateapipb.ControlClient, routeDuration metric.Float64Histogram, parkCfg ParkedRequestConfig, parkMetrics *parkingMetrics, routeViaAuthority bool) *ExtProcServer {
	return &ExtProcServer{
		port:              port,
		apiClient:         apiClient,
		recorder:          NewQueryRecorder(100),
		resumer:           NewActorResumer(apiClient, withParking(parkCfg)),
		routeDuration:     routeDuration,
		parking:           newParkingLot(parkCfg, parkMetrics),
		routeViaAuthority: routeViaAuthority,
	}
}

// NewGRPCServer builds the gRPC server with the ext_proc service registered.
// The caller owns its lifecycle: Run serves it, and the drain sequence in
// drain.go stops it — gracefully first so in-flight streams (parked requests
// above all) finish, forcefully past the drain timeout.
func (s *ExtProcServer) NewGRPCServer() *grpc.Server {
	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	extprocv3.RegisterExternalProcessorServer(grpcServer, s)
	return grpcServer
}

func (s *ExtProcServer) Process(stream extprocv3.ExternalProcessor_ProcessServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		resp := &extprocv3.ProcessingResponse{}

		switch reqType := req.Request.(type) {
		case *extprocv3.ProcessingRequest_RequestHeaders:
			start := time.Now()
			hResponse, dynamicMetadata, rqm, target, tmplNs, tmplName, resumeOutcome, err := s.handleRequestHeaders(stream.Context(), reqType.RequestHeaders, req.GetAttributes())
			elapsed := time.Since(start)
			outcomeStr := classifyOutcome(err)
			resumeStr := string(resumeOutcome)
			if err != nil {
				slog.ErrorContext(stream.Context(), "Error during ext_proc RequestHeaders processing", slog.String("err", err.Error()))
				var reqErr *reqError
				if errors.As(err, &reqErr) {
					resp = immediateResponse(envoy_type.StatusCode(reqErr.statusCode), reqErr.Error())
				} else {
					resp = immediateResponse(envoy_type.StatusCode_InternalServerError, err.Error())
				}
				s.recordRouteDuration(stream.Context(), elapsed, tmplNs, tmplName, outcomeStr, resumeStr)
				s.recorder.AddRouterRequest(start, elapsed, "Error", "-", rqm)
			} else {
				resp.Response = &extprocv3.ProcessingResponse_RequestHeaders{RequestHeaders: hResponse}
				resp.DynamicMetadata = dynamicMetadata
				s.recordRouteDuration(stream.Context(), elapsed, tmplNs, tmplName, outcomeStr, resumeStr)
				s.recorder.AddRouterRequest(start, elapsed, "Route ok", target, rqm)
			}

		default:
			// No modification for other processing states, but log because this should
			// not be called.
			slog.Error("Unexpected request type", slog.String("reqType", fmt.Sprintf("%T", reqType)))
			resp.Response = &extprocv3.ProcessingResponse_RequestHeaders{
				RequestHeaders: &extprocv3.HeadersResponse{
					Response: &extprocv3.CommonResponse{},
				},
			}
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

func (s *ExtProcServer) handleRequestHeaders(
	ctx context.Context,
	reqHeaders *extprocv3.HttpHeaders,
	attributes map[string]*structpb.Struct,
) (*extprocv3.HeadersResponse, *structpb.Struct, *requestMetadata, string, string, string, ResumeOutcome, error) {
	metadata := newRequestMetadata(reqHeaders.Headers.GetHeaders())
	slog.InfoContext(ctx, "Request", slog.String("host", metadata.host))

	// Envoy doesn't propagate trace context into the ext_proc gRPC
	// stream's metadata — the per-request traceparent arrives in the
	// HTTP headers carried inside the ProcessingRequest payload. Extract
	// from there so our span links to the Envoy ingress span.
	ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(metadata.headers))
	ctx, span := otel.Tracer(routerServiceName).Start(ctx, "ExtProc.RequestHeaders")
	defer span.End()

	// The actor is always resolved from the forwarded filter-state authority
	// attribute, never the Host/:authority header directly: that header is only
	// reliable for the plain ingress_http/ingress_https listeners (which
	// populate the filter state themselves via authorityFilterStateFilter --
	// see buildHcm's captureAuthority). For CONNECT-tunneled traffic reinjected
	// through main_internal, the tunneled protocol's own :authority is
	// unrelated to the actor's DNS name; the authoritative value is whatever
	// connect_terminate captured at CONNECT time and shared with upstream via
	// filter state (see buildMainInternalCluster). Same source
	// NetworkExtProcServer.handleFirstFrame uses for the TCP leg.
	authority := attributes[HttpExtProcFilterName].GetFields()[AuthorityFilterStateAttribute].GetStringValue()
	if authority == "" {
		return nil, nil, metadata, "", "", "", ResumeOutcomeNone, invalidHostErr(metadata.host, fmt.Errorf("missing %s request attribute", AuthorityFilterStateAttribute))
	}
	actorRef, err := parseActorRef(authority)
	if err != nil {
		return nil, nil, metadata, "", "", "", ResumeOutcomeNone, invalidHostErr(authority, err)
	}

	// The port to reach on the actor itself travels in the same authority:
	// for CONNECT-tunneled traffic it's the arbitrary port the client asked
	// for (e.g. ":9090"), and for plain ingress_http/ingress_https it's
	// absent, defaulting to the actor's normal port 80. atunnel's Server
	// can't learn this any other way -- its Config.Upstream is fixed for
	// its whole lifetime -- so it's forwarded via TargetPortHeader (see
	// atunnel.Server.NewServer's Rewrite func).
	targetPort := 80
	if _, portStr, err := net.SplitHostPort(authority); err == nil {
		if p, err := strconv.Atoi(portStr); err == nil && p > 0 && p <= 65535 {
			targetPort = p
		}
	}

	// Admit the request to the parking lot before resuming. While resume is
	// in-flight the request occupies a slot; if the actor's worker pool is
	// momentarily saturated the resumer parks (retries) here rather than failing
	// fast. A full lot sheds the request immediately so the router applies
	// backpressure instead of queueing without bound.
	release, ok := s.parking.enter(ctx)
	if !ok {
		return nil, nil, metadata, "", "", "", ResumeOutcomeNone, parkingFullErr(actorRef.String())
	}

	slog.InfoContext(ctx, "ResumeActor", slog.Any("actor", actorRef))
	actor, resumeOutcome, err := s.resumer.ResumeActor(ctx, actorRef)
	release(parkOutcomeFor(err))
	if err != nil {
		return nil, nil, metadata, "", "", "", resumeOutcome, mapResumeError(actorRef, err)
	}

	// Actor template identity, used as low-cardinality route-latency metric
	// attributes (see recordRouteDuration).
	tmplNs := actor.GetActorTemplateNamespace()
	tmplName := actor.GetActorTemplateName()

	workerIP := actor.GetWorkerAssignment().GetWorkerPodIp()
	slog.InfoContext(ctx, "ResumeActor result",
		slog.Any("actor", actorRef),
		slog.String("status", actor.GetStatus().String()),
		slog.String("workerIP", workerIP))

	if ip := net.ParseIP(workerIP); ip == nil {
		return nil, nil, metadata, "", tmplNs, tmplName, resumeOutcome, newReqError(envoy_type.StatusCode_InternalServerError,
			"actor %s routing failed", actorRef)
	}

	// The actor is reached through the in-worker atunnel ingress server, which
	// listens on :443 (mTLS) and forwards to targetPort on the actor. The
	// worker no longer DNATs pod-IP:80 to the actor, so the router dials :443
	// and the ORIGINAL_DST cluster's upstream TLS context presents the
	// router's podidentity client cert (see buildOriginalDstCluster and
	// buildUpstreamTransportSocket).
	targetAddr := net.JoinHostPort(workerIP, "443")

	slog.InfoContext(ctx, "Route ok", slog.Any("actor", actorRef), slog.String("targetAddr", targetAddr))

	// Report the resolved worker address as dynamic metadata rather than a
	// header mutation: a header only works for HTTP traffic, and this same
	// server may in principle be reused by transports that aren't. See
	// ActorTargetMetadataNamespace and buildOriginalDstCluster's MetadataKey.
	dynamicMetadata, err := structpb.NewStruct(map[string]any{
		ActorTargetMetadataNamespace: map[string]any{
			OriginalDstHeader: targetAddr,
		},
	})
	if err != nil {
		return nil, nil, metadata, "", tmplNs, tmplName, resumeOutcome, newReqError(envoy_type.StatusCode_InternalServerError,
			"actor %s routing failed", actorRef)
	}

	// Route by telling the ORIGINAL_DST cluster which worker atunnel address to
	// dial, without touching :authority — atunnel authorizes the actor by the
	// original Host (actor DNS name).
	mutation := &extprocv3.HeaderMutation{}
	addRoutingMutations(targetAddr, metadata.host, s.routeViaAuthority, mutation)
	// atunnel picks which port on the actor to reach from this header (the
	// CONNECT authority's port, or 80 for plain ingress -- see targetPort
	// above); it can't read Envoy's dynamic metadata directly.
	mutation.SetHeaders = append(mutation.SetHeaders, &corev3.HeaderValueOption{
		Header: &corev3.HeaderValue{
			Key:      atunnel.TargetPortHeader,
			RawValue: []byte(strconv.Itoa(targetPort)),
		},
		AppendAction: corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD,
	})

	return &extprocv3.HeadersResponse{
		Response: &extprocv3.CommonResponse{
			HeaderMutation: mutation,
		},
	}, dynamicMetadata, metadata, targetAddr, tmplNs, tmplName, resumeOutcome, nil
}

func (s *ExtProcServer) recordRouteDuration(ctx context.Context, d time.Duration, tmplNs, tmplName, outcome, resume string) {
	if s.routeDuration == nil {
		return
	}
	s.routeDuration.Record(ctx, d.Seconds(), metric.WithAttributes(
		ateattr.TemplateNamespaceKey.String(tmplNs),
		ateattr.TemplateNameKey.String(tmplName),
		ateattr.RouterOutcomeKey.String(outcome),
		ateattr.RouterResumeKey.String(resume),
	))
}

func classifyOutcome(err error) string {
	if err == nil {
		return "ok"
	}
	if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
		return "cancelled"
	}
	if errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded {
		return "timeout"
	}
	switch status.Code(err) {
	case codes.FailedPrecondition:
		return "no_capacity"
	case codes.Aborted:
		return "lock_conflict"
	case codes.NotFound:
		return "not_found"
	case codes.Unavailable:
		return "unavailable"
	case codes.ResourceExhausted:
		return "rate_limited"
	}
	var re *reqError
	if errors.As(err, &re) {
		switch envoy_type.StatusCode(re.statusCode) {
		case envoy_type.StatusCode_NotFound:
			return "not_found"
		case envoy_type.StatusCode_ServiceUnavailable:
			return "no_capacity"
		case envoy_type.StatusCode_GatewayTimeout:
			return "timeout"
		case envoy_type.StatusCode_TooManyRequests:
			return "rate_limited"
		}
	}
	return "resume_error"
}
