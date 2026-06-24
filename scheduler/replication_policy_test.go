package scheduler

import (
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/XploreAlpha/wau-trust/engine"
	"github.com/wau/registry/registry"
)

// =====================================================================
// v0.8.0 M4-3.2: ReplicationPolicy unit tests
// =====================================================================

func TestNewReplicationPolicy_Defaults(t *testing.T) {
	logger := slog.Default()
	p := NewReplicationPolicy(logger)

	if p.MinParentTrust != engine.MinParentTrustForReplication {
		t.Errorf("expected MinParentTrust=%v, got %v",
			engine.MinParentTrustForReplication, p.MinParentTrust)
	}
	if p.MaxChildrenPerParent != 5 {
		t.Errorf("expected MaxChildrenPerParent=5, got %v", p.MaxChildrenPerParent)
	}
	if p.InheritanceFactor != engine.DefaultInheritanceFactor {
		t.Errorf("expected InheritanceFactor=%v, got %v",
			engine.DefaultInheritanceFactor, p.InheritanceFactor)
	}
	if p.Logger == nil {
		t.Error("expected Logger to be set")
	}
	if p.childCounts == nil {
		t.Error("expected childCounts map to be initialized")
	}
}

func TestNewReplicationPolicy_NilLogger(t *testing.T) {
	p := NewReplicationPolicy(nil)
	if p.Logger == nil {
		t.Error("expected Logger to default to slog.Default() when nil passed")
	}
}

func TestReplicationPolicy_CanReplicate_NilPolicy(t *testing.T) {
	var p *ReplicationPolicy = nil
	err := p.CanReplicate(0.9, 0)
	if err != ErrPolicyDisabled {
		t.Errorf("nil policy should return ErrPolicyDisabled, got %v", err)
	}
}

func TestReplicationPolicy_CanReplicate_TrustBelow(t *testing.T) {
	p := NewReplicationPolicy(slog.Default())
	err := p.CanReplicate(0.5, 0) // 0.5 < 0.8
	if err != ErrParentLowTrust {
		t.Errorf("trust 0.5 should return ErrParentLowTrust, got %v", err)
	}
}

func TestReplicationPolicy_CanReplicate_TrustAtBoundary(t *testing.T) {
	p := NewReplicationPolicy(slog.Default())
	// MinParentTrust=0.8, parentTrust=0.8 → allowed (boundary inclusive)
	err := p.CanReplicate(engine.MinParentTrustForReplication, 0)
	if err != nil {
		t.Errorf("trust at boundary (0.8) should be allowed, got %v", err)
	}
}

func TestReplicationPolicy_CanReplicate_TrustAbove(t *testing.T) {
	p := NewReplicationPolicy(slog.Default())
	err := p.CanReplicate(0.95, 0)
	if err != nil {
		t.Errorf("trust 0.95 should be allowed, got %v", err)
	}
}

func TestReplicationPolicy_CanReplicate_ChildrenAtLimit(t *testing.T) {
	p := NewReplicationPolicy(slog.Default())
	err := p.CanReplicate(0.9, 5) // 5 >= 5 (limit)
	if err != ErrChildLimitReached {
		t.Errorf("children=5 (at limit) should return ErrChildLimitReached, got %v", err)
	}
}

func TestReplicationPolicy_CanReplicate_ChildrenAboveLimit(t *testing.T) {
	p := NewReplicationPolicy(slog.Default())
	err := p.CanReplicate(0.9, 10)
	if err != ErrChildLimitReached {
		t.Errorf("children=10 should return ErrChildLimitReached, got %v", err)
	}
}

func TestReplicationPolicy_CanReplicate_ChildrenBelowLimit(t *testing.T) {
	p := NewReplicationPolicy(slog.Default())
	err := p.CanReplicate(0.9, 4) // 4 < 5
	if err != nil {
		t.Errorf("children=4 should be allowed, got %v", err)
	}
}

func TestReplicationPolicy_RecordChild_AndCount(t *testing.T) {
	p := NewReplicationPolicy(slog.Default())

	// Initial: 0
	if got := p.ChildCount("parent-A"); got != 0 {
		t.Errorf("initial count for parent-A: expected 0, got %d", got)
	}

	// Record 3 times
	if got := p.RecordChild("parent-A"); got != 1 {
		t.Errorf("first RecordChild: expected 1, got %d", got)
	}
	if got := p.RecordChild("parent-A"); got != 2 {
		t.Errorf("second RecordChild: expected 2, got %d", got)
	}
	if got := p.RecordChild("parent-A"); got != 3 {
		t.Errorf("third RecordChild: expected 3, got %d", got)
	}

	// Verify count reflects total
	if got := p.ChildCount("parent-A"); got != 3 {
		t.Errorf("ChildCount after 3 records: expected 3, got %d", got)
	}

	// Different parent: independent
	if got := p.RecordChild("parent-B"); got != 1 {
		t.Errorf("parent-B first record: expected 1, got %d", got)
	}
	if got := p.ChildCount("parent-A"); got != 3 {
		t.Errorf("parent-A count unchanged: expected 3, got %d", got)
	}
}

func TestReplicationPolicy_RecordChild_NilPolicy(t *testing.T) {
	var p *ReplicationPolicy = nil
	got := p.RecordChild("parent")
	if got != 0 {
		t.Errorf("nil policy RecordChild should return 0, got %d", got)
	}
}

func TestReplicationPolicy_ChildCount_NilPolicy(t *testing.T) {
	var p *ReplicationPolicy = nil
	got := p.ChildCount("parent")
	if got != 0 {
		t.Errorf("nil policy ChildCount should return 0, got %d", got)
	}
}

func TestReplicationPolicy_Reset(t *testing.T) {
	p := NewReplicationPolicy(slog.Default())
	p.RecordChild("parent-A")
	p.RecordChild("parent-A")
	p.RecordChild("parent-B")

	p.Reset("parent-A")

	if got := p.ChildCount("parent-A"); got != 0 {
		t.Errorf("after Reset, parent-A count: expected 0, got %d", got)
	}
	// parent-B should be unaffected
	if got := p.ChildCount("parent-B"); got != 1 {
		t.Errorf("parent-B count after resetting A: expected 1, got %d", got)
	}
}

func TestReplicationPolicy_Reset_NilPolicy(t *testing.T) {
	var p *ReplicationPolicy = nil
	// Should not panic
	p.Reset("parent")
}

func TestReplicationPolicy_BuildChildSpec_FromParent(t *testing.T) {
	p := NewReplicationPolicy(slog.Default())
	parent := &registry.AgentCard{
		ID:       "parent-1",
		Name:     "parent-1",
		Skills:   []string{"general", "python"},
		Version:  "v0.5.1",
		Universe: "default",
	}

	spec := p.BuildChildSpec(parent, "child-1")

	if spec.ID != "child-1" {
		t.Errorf("expected ID=child-1, got %q", spec.ID)
	}
	if spec.Name != "child-1" {
		t.Errorf("expected Name=child-1, got %q", spec.Name)
	}
	if spec.Version != "v0.5.1" {
		t.Errorf("expected Version=v0.5.1, got %q", spec.Version)
	}
	if spec.Universe != "default" {
		t.Errorf("expected Universe=default, got %q", spec.Universe)
	}
	if len(spec.Skills) != 2 || spec.Skills[0] != "general" || spec.Skills[1] != "python" {
		t.Errorf("expected Skills=[general,python], got %v", spec.Skills)
	}

	// Verify skills is a COPY (mutating spec.Skills should not affect parent)
	spec.Skills[0] = "mutated"
	if parent.Skills[0] == "mutated" {
		t.Error("BuildChildSpec should copy skills, not alias")
	}
}

func TestReplicationPolicy_BuildChildSpec_NilParent(t *testing.T) {
	p := NewReplicationPolicy(slog.Default())
	spec := p.BuildChildSpec(nil, "child-x")

	if spec.ID != "child-x" {
		t.Errorf("expected ID=child-x, got %q", spec.ID)
	}
	if spec.Name != "child-x" {
		t.Errorf("expected Name=child-x, got %q", spec.Name)
	}
	if len(spec.Skills) != 0 {
		t.Errorf("expected empty Skills, got %v", spec.Skills)
	}
	if spec.Version != "" {
		t.Errorf("expected empty Version, got %q", spec.Version)
	}
}

func TestReplicationPolicy_Normalize_BadInputs(t *testing.T) {
	// Direct struct construction with bad values
	p := &ReplicationPolicy{
		MinParentTrust:       0,        // bad: ≤ 0
		MaxChildrenPerParent: -1,       // bad: ≤ 0
		InheritanceFactor:    2.0,      // bad: > 1
		Logger:               nil,      // bad: nil
		childCounts:          nil,      // bad: nil
	}
	p.normalize()

	if p.MinParentTrust != engine.MinParentTrustForReplication {
		t.Errorf("normalize MinParentTrust: expected default %v, got %v",
			engine.MinParentTrustForReplication, p.MinParentTrust)
	}
	if p.MaxChildrenPerParent != 5 {
		t.Errorf("normalize MaxChildrenPerParent: expected 5, got %d", p.MaxChildrenPerParent)
	}
	if p.InheritanceFactor != engine.DefaultInheritanceFactor {
		t.Errorf("normalize InheritanceFactor: expected default %v, got %v",
			engine.DefaultInheritanceFactor, p.InheritanceFactor)
	}
	if p.Logger == nil {
		t.Error("normalize should set Logger to slog.Default()")
	}
	if p.childCounts == nil {
		t.Error("normalize should initialize childCounts map")
	}
}

func TestReplicationPolicy_RationaleFor_Format(t *testing.T) {
	p := NewReplicationPolicy(slog.Default())
	r := p.RationaleFor("Whis", 0.92, 2)

	// Must contain actual numbers for log debugging
	if !strings.Contains(r, "0.92") {
		t.Errorf("rationale should contain parent trust 0.92, got %q", r)
	}
	if !strings.Contains(r, "0.80") {
		t.Errorf("rationale should contain threshold 0.80, got %q", r)
	}
	if !strings.Contains(r, "2") {
		t.Errorf("rationale should contain current children count 2, got %q", r)
	}
	if !strings.Contains(r, "5") {
		t.Errorf("rationale should contain max children 5, got %q", r)
	}
	// Must show ≥ since 0.92 >= 0.80
	if !strings.Contains(r, "≥") {
		t.Errorf("rationale should show ≥ since 0.92 >= 0.80, got %q", r)
	}
}

func TestReplicationPolicy_RationaleFor_NilPolicy(t *testing.T) {
	var p *ReplicationPolicy = nil
	r := p.RationaleFor("Whis", 0.92, 2)
	if r != "" {
		t.Errorf("nil policy RationaleFor should return empty string, got %q", r)
	}
}

func TestReplicationPolicy_RationaleFor_LowTrust(t *testing.T) {
	p := NewReplicationPolicy(slog.Default())
	r := p.RationaleFor("Low", 0.5, 0)
	// 0.5 < 0.80 → should show "<"
	if !strings.Contains(r, "<") {
		t.Errorf("rationale for low trust should show <, got %q", r)
	}
}

// TestReplicationPolicy_ConcurrentRecordChild verifies the mutex protects
// the childCounts map from data races under concurrent RecordChild calls.
func TestReplicationPolicy_ConcurrentRecordChild(t *testing.T) {
	p := NewReplicationPolicy(slog.Default())
	const goroutines = 50
	const recordsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < recordsPerGoroutine; j++ {
				p.RecordChild("parent-X")
			}
		}()
	}
	wg.Wait()

	expected := goroutines * recordsPerGoroutine
	if got := p.ChildCount("parent-X"); got != expected {
		t.Errorf("concurrent RecordChild: expected %d, got %d", expected, got)
	}
}