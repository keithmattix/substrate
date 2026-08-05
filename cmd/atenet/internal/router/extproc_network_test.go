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
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	networkextprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/network_ext_proc/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

// TestNetworkExtProcHandleFirstFrameNestsMetadata locks in that
// handleFirstFrame's resolved worker address is nested under
// OriginalDstMetadataKey, matching MetadataOptions.ReceivingNamespaces in
// buildTcpConnectFilterChain -- Envoy's network ext_proc filter only ingests
// DynamicMetadata fields whose top-level key is on that allowlist, silently
// dropping anything else (see xds.go's ReceivingNamespaces comment).
func TestNetworkExtProcHandleFirstFrameNestsMetadata(t *testing.T) {
	const testUUID = "123e4567-e89b-12d3-a456-426614174000"
	clientMock := &mockClient{
		resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
			return &ateapipb.ResumeActorResponse{Actor: &ateapipb.Actor{WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: "10.0.0.52"}}}, nil
		},
	}
	s := NewNetworkExtProcServer(clientMock)

	authorityStruct, err := structpb.NewStruct(map[string]any{
		substrateAuthorityMetadataKey: testUUID + ".team-a.actors.resources.substrate.ate.dev:9090",
	})
	if err != nil {
		t.Fatal(err)
	}
	req := &networkextprocv3.ProcessingRequest{
		Metadata: &corev3.Metadata{
			FilterMetadata: map[string]*structpb.Struct{
				SubstrateMetadataNamespace: authorityStruct,
			},
		},
	}

	resp, err := s.handleFirstFrame(context.Background(), req)
	if err != nil {
		t.Fatalf("handleFirstFrame: %v", err)
	}

	const wantTarget = "10.0.0.52:443"
	got := resp.GetDynamicMetadata().GetFields()[OriginalDstMetadataKey].GetStructValue().GetFields()[OriginalDstAddressKey].GetStringValue()
	if got != wantTarget {
		t.Errorf("dynamic metadata %s/%s = %q, want %q", OriginalDstMetadataKey, OriginalDstAddressKey, got, wantTarget)
	}
	if flat := resp.GetDynamicMetadata().GetFields()[OriginalDstAddressKey].GetStringValue(); flat != "" {
		t.Errorf("dynamic metadata unexpectedly set %s at the top level (%q): Envoy's ReceivingNamespaces allowlist only ingests %s", OriginalDstAddressKey, flat, OriginalDstMetadataKey)
	}
}
