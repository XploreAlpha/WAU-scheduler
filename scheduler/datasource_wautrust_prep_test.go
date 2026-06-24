package scheduler

import (
	"context"
	"testing"

	"github.com/XploreAlpha/wau-trust/engine"
	"github.com/wau/registry/registry"
)

// TestNewDefaultWauTrustDataSource_ReplicateAccessible verifies the
// v0.8.0 M4-3.3 prep factory exposes Replicate (the WauTrustDataSource
// method WAU-core-kernel needs to write child trust).
//
// Why this test: prior to M4-3.3, *TrustDataSource (facade) was the
// kernel-friendly entry point and lacked Replicate. *WauTrustDataSource
// has Replicate but the existing constructor NewWauTrustDataSource requires
// an inner DataSource + wau-trust engine explicitly. NewDefaultWauTrustDataSource
// is a one-call factory (like NewDefaultTrustDataSource) that kernel can
// use to get a Replicate-capable DS without importing wau-trust.
func TestNewDefaultWauTrustDataSource_ReplicateAccessible(t *testing.T) {
	ds := NewDefaultWauTrustDataSource()
	if ds == nil {
		t.Fatal("NewDefaultWauTrustDataSource returned nil")
	}
	// Compile-time type check
	var _ *WauTrustDataSource = ds

	// Cold parent → engine.ErrParentCold (no RecordSuccess call yet)
	_, err := ds.Replicate(context.Background(), "Cold-Parent", "Child", engine.DefaultInheritanceFactor)
	if err == nil {
		t.Error("expected error for cold parent, got nil")
	}
}

// TestNewDefaultWauTrustDataSource_TrustScoreDelegates verifies the
// default DS delegates TrustScore to its internal engine (via the
// 0.0→0.5 coerce that WauTrustDataSource.TrustScore applies).
func TestNewDefaultWauTrustDataSource_TrustScoreDelegates(t *testing.T) {
	ds := NewDefaultWauTrustDataSource()
	ctx := context.Background()

	// Unrecorded agent → 0.5 (DefaultTrustScore via coerce)
	score, err := ds.TrustScore(ctx, "unrecorded")
	if err != nil {
		t.Fatalf("TrustScore: %v", err)
	}
	if score != 0.5 {
		t.Errorf("unrecorded agent should be 0.5, got %v", score)
	}
}

// TestSetScoringEngine verifies the v0.8.0 M4-3.3 prep setter accepts
// a custom ScoringEngine (no panic, no error). The setter is for kernel
// integration; full behavioral verification happens in the WAU-core-kernel
// M4-3.3 integration test which exercises the full Replicate flow end-to-end.
func TestSetScoringEngine(t *testing.T) {
	mockReg := &integrationMockRegistry{agents: map[string]*registry.AgentCard{}}
	// Just verify the setter exists and doesn't panic with a custom engine.
	// Behavioral coverage is in WAU-core-kernel M4-3.3 integration tests.
	s := NewSchedulerWithReplicationPolicy(mockReg, nil, nil, nil, NewReplicationPolicy(testLogger()))
	custom := NewScoringEngineWithDataSource(mockReg, NewMemoryDataSource(mockReg), testLogger())
	s.SetScoringEngine(custom)
	if s.scoring != custom {
		t.Error("SetScoringEngine did not replace the scoring field")
	}
}
