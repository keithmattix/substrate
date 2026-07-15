//go:build linux

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

package main

import (
	"bytes"
	"net"
	"testing"

	"github.com/google/nftables"
	"github.com/google/nftables/binaryutil"
	"github.com/google/nftables/expr"
	"golang.org/x/sys/unix"
)

func TestActorEgressRedirectRule(t *testing.T) {
	table := &nftables.Table{Name: actorNftTableName}
	chain := &nftables.Chain{Name: "prerouting", Table: table}
	rule := actorEgressRedirectRule(table, chain, 15001)
	if rule == nil {
		t.Fatal("actorEgressRedirectRule returned nil")
	}
	if rule.Table != table || rule.Chain != chain {
		t.Fatal("redirect rule is attached to the wrong table or chain")
	}
	if len(rule.Exprs) != 6 {
		t.Fatalf("redirect expression count = %d, want 6", len(rule.Exprs))
	}

	sourcePayload, ok := rule.Exprs[0].(*expr.Payload)
	if !ok || sourcePayload.Base != expr.PayloadBaseNetworkHeader || sourcePayload.Offset != 12 || sourcePayload.Len != 4 {
		t.Fatalf("source address expression = %#v", rule.Exprs[0])
	}
	sourceCmp, ok := rule.Exprs[1].(*expr.Cmp)
	if !ok || !bytes.Equal(sourceCmp.Data, net.ParseIP(actorVethIP).To4()) {
		t.Fatalf("source address comparison = %#v", rule.Exprs[1])
	}
	protocolCmp, ok := rule.Exprs[3].(*expr.Cmp)
	if !ok || !bytes.Equal(protocolCmp.Data, []byte{unix.IPPROTO_TCP}) {
		t.Fatalf("protocol comparison = %#v", rule.Exprs[3])
	}
	port, ok := rule.Exprs[4].(*expr.Immediate)
	if !ok || port.Register != 1 || !bytes.Equal(port.Data, binaryutil.BigEndian.PutUint16(15001)) {
		t.Fatalf("redirect port expression = %#v", rule.Exprs[4])
	}
	redirect, ok := rule.Exprs[5].(*expr.Redir)
	if !ok || redirect.RegisterProtoMin != 1 {
		t.Fatalf("redirect expression = %#v", rule.Exprs[5])
	}
}

func TestActorEgressRedirectRuleDisabled(t *testing.T) {
	if rule := actorEgressRedirectRule(&nftables.Table{}, &nftables.Chain{}, 0); rule != nil {
		t.Fatalf("actorEgressRedirectRule returned %#v for disabled egress", rule)
	}
}
