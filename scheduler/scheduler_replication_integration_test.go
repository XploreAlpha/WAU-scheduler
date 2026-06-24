package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/XploreAlpha/wau-trust/engine"
	"github.com/wau/registry/registry"
)

// =====================================================================
// v0.8.0 M4-3.2: Scheduler.Replicate integration tests
// =====================================================================

// makeReplicationIntegrationRig creates a scheduler with WauTrustDataSource
// (MemoryEngine), 2 agents: Whis (warm, trust ~0.85) + Cold (no trust).
//
// Returns the scheduler, the trust engine (for direct assertions), and
// the default replication policy (already attached to the scheduler).
//
// Trust math: weight=0.7 with old=0.5 → 0.5*0.3 + 0.7 = 0.85 (above 0.8 threshold).
func makeReplicationIntegrationRig(t *testing.T) (*Scheduler, *engine.MemoryEngine, *ReplicationPolicy) {
	t.Helper()

	mockReg := &integrationMockRegistry{agents: map[string]*registry.AgentCard{
		"Whis": {
			ID: "Whis", Name: "Whis", Version: "v0.5.1",
			Skills: []string{"general"}, Universe: "default",
			FirstSeen: time.Now().Unix() - 1000, LastSeen: time.Now().Unix(),
		},
		"Cold": {
			ID: "Cold", Name: "Cold", Version: "v0.5.1",
			Skills: []string{"general"}, Universe: "default",
			FirstSeen: time.Now().Unix() - 1000, LastSeen: time.Now().Unix(),
		},
	}}

	inner := NewMemoryDataSource(mockReg)
	trustEng := engine.NewMemoryEngine()

	// Whis: warm (trust ~0.85 after one success at weight 0.7, above 0.8 threshold)
	if err := trustEng.RecordSuccess(context.Background(), "Whis", 0.7); err != nil {
		t.Fatalf("trustEng.RecordSuccess(Whis): %v", err)
	}
	// inner.SetTrustScore is irrelevant — WauTrustDataSource delegates to wau-trust
	inner.SetTrustScore("Whis", 0.85)

	// Cold: no Record* call — fresh/cold agent (no trust data)

	ds := NewWauTrustDataSource(inner, trustEng)
	scoring := NewScoringEngineWithDataSource(mockReg, ds, slog.Default())
	s := NewSchedulerWithScoring(mockReg, scoring, slog.Default())

	policy := NewReplicationPolicy(slog.Default())
	s.SetReplicationPolicy(policy)

	return s, trustEng, policy
}

// TestScheduler_Replicate_NilPolicy: Scheduler with no replication policy
// returns ErrPolicyDisabled on Replicate call.
func TestScheduler_Replicate_NilPolicy(t *testing.T) {
	mockReg := &integrationMockRegistry{agents: map[string]*registry.AgentCard{
		"Whis": {ID: "Whis", Name: "Whis", Version: "v0.5.1", FirstSeen: time.Now().Unix() - 100, LastSeen: time.Now().Unix()},
	}}
	s := NewSchedulerWithRegistry(mockReg, slog.Default())
	// Note: no SetReplicationPolicy → nil policy

	_, err := s.Replicate(context.Background(), "Whis", "Whis-Child")
	if !errors.Is(err, ErrPolicyDisabled) {
		t.Errorf("expected ErrPolicyDisabled, got %v", err)
	}
}

// TestScheduler_Replicate_ParentNotFound: parent agent missing from registry.
func TestScheduler_Replicate_ParentNotFound(t *testing.T) {
	s, _, _ := makeReplicationIntegrationRig(t)
	_, err := s.Replicate(context.Background(), "Ghost", "Ghost-Child")
	if !errors.Is(err, ErrParentNotFound) {
		t.Errorf("expected ErrParentNotFound, got %v", err)
	}
}

// TestScheduler_Replicate_ParentCold: parent has no trust data → ErrParentCold.
func TestScheduler_Replicate_ParentCold(t *testing.T) {
	s, _, _ := makeReplicationIntegrationRig(t)
	_, err := s.Replicate(context.Background(), "Cold", "Cold-Child")
	if !errors.Is(err, ErrParentCold) {
		t.Errorf("expected ErrParentCold, got %v", err)
	}
}

// TestScheduler_Replicate_EmptyChildID: empty childID → ErrInvalidChildID.
func TestScheduler_Replicate_EmptyChildID(t *testing.T) {
	s, _, _ := makeReplicationIntegrationRig(t)
	_, err := s.Replicate(context.Background(), "Whis", "")
	if !errors.Is(err, ErrInvalidChildID) {
		t.Errorf("expected ErrInvalidChildID, got %v", err)
	}
}

// TestScheduler_Replicate_EmptyParentID: empty parentID → ErrInvalidChildID.
func TestScheduler_Replicate_EmptyParentID(t *testing.T) {
	s, _, _ := makeReplicationIntegrationRig(t)
	_, err := s.Replicate(context.Background(), "", "Child")
	if !errors.Is(err, ErrInvalidChildID) {
		t.Errorf("expected ErrInvalidChildID, got %v", err)
	}
}

// TestScheduler_Replicate_HappyPath: warm parent with no children returns
// a valid ReplicateDecision. Critical assertions:
//   - ExpectedChildTrust == engine.ReplicateTrust(...) — pre-compute matches
//   - Calling actual engine.Replicate produces the same value (determinism)
//   - Scheduler.Replicate does NOT call engine.Replicate itself (no write)
//
// Whis trust = 0.85 after weight=0.7 RecordSuccess. Child trust expected in
// [0.85*0.8 - 0.05, 0.85*0.8 + 0.05] = [0.63, 0.73].
func TestScheduler_Replicate_HappyPath(t *testing.T) {
	s, trustEng, policy := makeReplicationIntegrationRig(t)

	decision, err := s.Replicate(context.Background(), "Whis", "Whis-Child")
	if err != nil {
		t.Fatalf("Replicate happy path: unexpected error %v", err)
	}
	if decision == nil {
		t.Fatal("Replicate happy path: expected non-nil decision")
	}
	if decision.ParentID != "Whis" {
		t.Errorf("decision.ParentID: expected Whis, got %q", decision.ParentID)
	}
	if decision.ChildID != "Whis-Child" {
		t.Errorf("decision.ChildID: expected Whis-Child, got %q", decision.ChildID)
	}
	if decision.CurrentChildren != 0 {
		t.Errorf("decision.CurrentChildren: expected 0, got %d", decision.CurrentChildren)
	}
	if decision.InheritanceFactor != engine.DefaultInheritanceFactor {
		t.Errorf("decision.InheritanceFactor: expected %v, got %v",
			engine.DefaultInheritanceFactor, decision.InheritanceFactor)
	}

	// Whis trust is 0.85 after weight=0.7 RecordSuccess
	const expectedParentTrust = 0.85
	if decision.ParentTrust != expectedParentTrust {
		t.Errorf("decision.ParentTrust: expected %v, got %v",
			expectedParentTrust, decision.ParentTrust)
	}

	// Verify determinism: pre-compute via pure helper must match
	expectedPreCompute := engine.ReplicateTrust(expectedParentTrust, policy.InheritanceFactor, "Whis", "Whis-Child")
	if decision.ExpectedChildTrust != expectedPreCompute {
		t.Errorf("decision.ExpectedChildTrust: expected %v (pre-compute), got %v",
			expectedPreCompute, decision.ExpectedChildTrust)
	}

	// Sanity: child trust should be in [0.63, 0.73]
	if decision.ExpectedChildTrust < 0.63 || decision.ExpectedChildTrust > 0.73 {
		t.Errorf("decision.ExpectedChildTrust out of expected range [0.63, 0.73]: %v",
			decision.ExpectedChildTrust)
	}

	// Verify Rationale is non-empty and contains numbers
	if decision.Rationale == "" {
		t.Error("decision.Rationale should not be empty")
	}

	// Verify scheduler did NOT write: child should have DefaultTrustScore (unrecorded)
	score, _ := trustEng.GetScore(context.Background(), "Whis-Child")
	if score != engine.DefaultTrustScore {
		t.Errorf("scheduler.Replicate must not write to engine, but Whis-Child already has score %v", score)
	}

	// Now call actual engine.Replicate and verify deterministic match
	actualScore, err := trustEng.Replicate(context.Background(), "Whis", "Whis-Child", policy.InheritanceFactor)
	if err != nil {
		t.Fatalf("trustEng.Replicate: %v", err)
	}
	if actualScore != decision.ExpectedChildTrust {
		t.Errorf("determinism violation: pre-compute %v != engine write %v",
			decision.ExpectedChildTrust, actualScore)
	}

	// Verify ChildSpec is inherited from parent
	if decision.ChildSpec.Version != "v0.5.1" {
		t.Errorf("decision.ChildSpec.Version: expected v0.5.1, got %q", decision.ChildSpec.Version)
	}
	if decision.ChildSpec.Universe != "default" {
		t.Errorf("decision.ChildSpec.Universe: expected default, got %q", decision.ChildSpec.Universe)
	}
	if len(decision.ChildSpec.Skills) != 1 || decision.ChildSpec.Skills[0] != "general" {
		t.Errorf("decision.ChildSpec.Skills: expected [general], got %v", decision.ChildSpec.Skills)
	}
}

// TestScheduler_Replicate_ParentLowTrust: parent with trust below threshold
// returns ErrParentLowTrust.
//
// Weight=0.1 RecordSuccess on default 0.5 → trust = 0.5*0.9 + 0.1 = 0.55,
// below 0.8 threshold. Agent is warm (not cold) so it hits the trust check.
func TestScheduler_Replicate_ParentLowTrust(t *testing.T) {
	mockReg := &integrationMockRegistry{agents: map[string]*registry.AgentCard{
		"LowTrust": {
			ID: "LowTrust", Name: "LowTrust", Version: "v0.5.1",
			Skills: []string{"general"}, Universe: "default",
			FirstSeen: time.Now().Unix() - 100, LastSeen: time.Now().Unix(),
		},
	}}
	inner := NewMemoryDataSource(mockReg)
	trustEng := engine.NewMemoryEngine()

	// Warm up at low weight → trust = 0.55 (warm but below 0.8 threshold)
	if err := trustEng.RecordSuccess(context.Background(), "LowTrust", 0.1); err != nil {
		t.Fatalf("RecordSuccess: %v", err)
	}

	ds := NewWauTrustDataSource(inner, trustEng)
	scoring := NewScoringEngineWithDataSource(mockReg, ds, slog.Default())
	s := NewSchedulerWithScoring(mockReg, scoring, slog.Default())
	s.SetReplicationPolicy(NewReplicationPolicy(slog.Default()))

	_, err := s.Replicate(context.Background(), "LowTrust", "LowTrust-Child")
	if !errors.Is(err, ErrParentLowTrust) {
		t.Errorf("expected ErrParentLowTrust, got %v", err)
	}
}

// TestScheduler_Replicate_ChildLimitReached: parent at MaxChildrenPerParent.
func TestScheduler_Replicate_ChildLimitReached(t *testing.T) {
	s, _, policy := makeReplicationIntegrationRig(t)

	// Manually bump the child count to the limit
	for i := 0; i < policy.MaxChildrenPerParent; i++ {
		policy.RecordChild("Whis")
	}

	_, err := s.Replicate(context.Background(), "Whis", "Whis-Child-6")
	if !errors.Is(err, ErrChildLimitReached) {
		t.Errorf("expected ErrChildLimitReached, got %v", err)
	}
}

// TestScheduler_Replicate_DoesNotWrite: explicit assertion that
// Scheduler.Replicate is a pure decision function — no side effects on
// registry, trust engine, or policy counters.
func TestScheduler_Replicate_DoesNotWrite(t *testing.T) {
	s, trustEng, policy := makeReplicationIntegrationRig(t)
	ctx := context.Background()

	decision, err := s.Replicate(ctx, "Whis", "Whis-Child")
	if err != nil {
		t.Fatalf("Replicate: %v", err)
	}

	// 1. Child not in trust engine
	if score, _ := trustEng.GetScore(ctx, "Whis-Child"); score != engine.DefaultTrustScore {
		t.Errorf("scheduler wrote to trust engine: Whis-Child has score %v", score)
	}

	// 2. Child count not incremented (kernel responsibility)
	if got := policy.ChildCount("Whis"); got != 0 {
		t.Errorf("scheduler incremented child count: got %d, expected 0", got)
	}

	// 3. Child not in mock registry
	_, err = s.reg.GetAgent(ctx, decision.ChildID)
	if err == nil {
		t.Errorf("scheduler wrote to registry: child %q exists", decision.ChildID)
	}
}

// TestScheduler_SetReplicationPolicy_Nil: setting nil policy disables replication.
func TestScheduler_SetReplicationPolicy_Nil(t *testing.T) {
	s, _, policy := makeReplicationIntegrationRig(t)
	// First verify it works with a policy
	_, err := s.Replicate(context.Background(), "Whis", "Whis-Child")
	if err != nil {
		t.Fatalf("Replicate with policy: %v", err)
	}

	// Disable
	s.SetReplicationPolicy(nil)
	if got := s.GetReplicationPolicy(); got != nil {
		t.Errorf("after SetReplicationPolicy(nil), GetReplicationPolicy should return nil, got %v", got)
	}
	_, err = s.Replicate(context.Background(), "Whis", "Whis-Child")
	if !errors.Is(err, ErrPolicyDisabled) {
		t.Errorf("after SetReplicationPolicy(nil), Replicate should return ErrPolicyDisabled, got %v", err)
	}

	// Re-enable (suppress unused variable warning)
	s.SetReplicationPolicy(policy)
}

// TestScheduler_GetReplicationPolicy_NilByDefault: new scheduler has nil policy.
func TestScheduler_GetReplicationPolicy_NilByDefault(t *testing.T) {
	mockReg := &integrationMockRegistry{agents: map[string]*registry.AgentCard{}}
	s := NewSchedulerWithRegistry(mockReg, slog.Default())
	if got := s.GetReplicationPolicy(); got != nil {
		t.Errorf("new scheduler should have nil replication policy, got %v", got)
	}
}