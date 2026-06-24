package scheduler

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/XploreAlpha/wau-trust/engine"
	"github.com/wau/registry/registry"
)

// ReplicationPolicy governs when agents may replicate and how (v0.8.0 M4-3.2).
//
// The policy is OPT-IN — pass nil to NewSchedulerWithReplicationPolicy (or call
// SetReplicationPolicy(nil)) to disable all replication behavior. This preserves
// backward compatibility with v0.7.x + M4-1 (cold routing) + M4-2 (sleep).
//
// Self-replication is a load-balancing optimization: when an agent has
// accumulated enough trust (≥ MinParentTrust) AND has not yet reached its
// child cap (< MaxChildrenPerParent), the system may spawn a child that
// inherits a portion (InheritanceFactor) of the parent's trust. The child
// can then take over a portion of the parent's task load.
//
// Per design memory `[[project-m4-3-design-2026-06-23]]`, this policy is the
// single source of truth for the MinParentTrust gate. wau-trust's Engine
// exposes the gate as MinParentTrustForReplication but does NOT enforce it
// (per engine godoc); the scheduler policy owns the enforcement.
//
// Target (per v0.8.0 plan §3.4): 在调度高峰期自动扩容,避免人工运维介入。
type ReplicationPolicy struct {
	// MinParentTrust — minimum parent trust score required to replicate.
	// Read by CanReplicate. Default engine.MinParentTrustForReplication (0.8).
	//
	// Rationale: only "高 trust" parents should replicate, to avoid spawning
	// many children from a poorly-performing agent and propagating low trust.
	MinParentTrust float64

	// MaxChildrenPerParent — maximum number of children one parent can spawn.
	// Once reached, further Replicate calls return ErrChildLimitReached.
	// Default 5 (per design doc §4).
	MaxChildrenPerParent int

	// InheritanceFactor — trust inheritance factor for child = parent * factor.
	// Default engine.DefaultInheritanceFactor (0.8).
	//
	// The actual child trust applies deterministic jitter (±0.05) on top of
	// this factor via engine.ReplicateTrust. The jitter is replay-safe and
	// matches between pre-compute (Scheduler.Replicate) and write
	// (kernel calling engine.Replicate).
	InheritanceFactor float64

	// Logger — optional; defaults to slog.Default().
	Logger *slog.Logger

	// mu protects childCounts from concurrent RecordChild / ChildCount / Reset.
	mu          sync.Mutex
	childCounts map[string]int
}

// NewReplicationPolicy returns a policy with safe defaults sourced from wau-trust.
//
// Default values:
//   - MinParentTrust = engine.MinParentTrustForReplication (0.8)
//   - MaxChildrenPerParent = 5
//   - InheritanceFactor = engine.DefaultInheritanceFactor (0.8)
//
// Zero/negative values are caught by normalize() and replaced with defaults,
// so callers may construct a zero-value ReplicationPolicy{} and pass it to
// normalize manually if they prefer not to use the constructor.
func NewReplicationPolicy(logger *slog.Logger) *ReplicationPolicy {
	if logger == nil {
		logger = slog.Default()
	}
	p := &ReplicationPolicy{
		MinParentTrust:       engine.MinParentTrustForReplication,
		MaxChildrenPerParent: 5,
		InheritanceFactor:    engine.DefaultInheritanceFactor,
		Logger:               logger,
		childCounts:          make(map[string]int),
	}
	p.normalize()
	return p
}

// normalize validates fields and replaces out-of-range values with defaults.
//
// Rules:
//   - MinParentTrust ≤ 0 OR > 1.0 → engine.MinParentTrustForReplication (0.8)
//   - MaxChildrenPerParent ≤ 0 → 5
//   - InheritanceFactor ∉ [0, 1] → engine.DefaultInheritanceFactor (0.8)
//
// Rationale for permissive (out-of-range → default) rather than reject: it
// matches ColdRoutingPolicy.normalize() behavior, gives callers a working
// policy even when they pass zero values, and avoids breaking deployments
// when the constants change in future versions.
func (p *ReplicationPolicy) normalize() {
	if p.MinParentTrust <= 0 || p.MinParentTrust > 1.0 {
		p.MinParentTrust = engine.MinParentTrustForReplication
	}
	if p.MaxChildrenPerParent <= 0 {
		p.MaxChildrenPerParent = 5
	}
	if p.InheritanceFactor < 0 || p.InheritanceFactor > 1.0 {
		p.InheritanceFactor = engine.DefaultInheritanceFactor
	}
	if p.Logger == nil {
		p.Logger = slog.Default()
	}
	if p.childCounts == nil {
		p.childCounts = make(map[string]int)
	}
}

// CanReplicate returns nil if parent may replicate, or a specific error.
//
// Pure function — no I/O. Callers pass parentTrust (from scoring engine) and
// currentChildren (from ChildCount). Both are read at decision time.
//
// Behavior when p == nil: returns ErrPolicyDisabled (treats nil policy as
// explicit "no replication"). This mirrors SleepPolicy.ShouldWake behavior
// for nil-safe API.
//
// Errors:
//   - ErrParentLowTrust: parentTrust < MinParentTrust
//   - ErrChildLimitReached: currentChildren ≥ MaxChildrenPerParent
func (p *ReplicationPolicy) CanReplicate(parentTrust float64, currentChildren int) error {
	if p == nil {
		return ErrPolicyDisabled
	}
	if parentTrust < p.MinParentTrust {
		return ErrParentLowTrust
	}
	if currentChildren >= p.MaxChildrenPerParent {
		return ErrChildLimitReached
	}
	return nil
}

// BuildChildSpec derives a ChildSpec from the parent's AgentCard.
//
// Skills are copied (not aliased) so callers may mutate the spec safely
// without affecting the parent's AgentCard. If parent is nil, the spec
// contains only the ID/Name (used as a fallback / defensive default).
//
// Fields NOT inherited (kept empty / default):
//   - FirstSeen / LastSeen: kernel sets these on registration
//   - Online: kernel sets this on Heartbeat
//   - Endpoints / Protocols: parent-specific, kernel decides
//   - URL: kernel decides
//
// M4-3.3 (kernel) may extend this when registry.AgentCard gains more
// replication-relevant fields (e.g. parent_agent_id).
func (p *ReplicationPolicy) BuildChildSpec(parent *registry.AgentCard, childID string) ChildSpec {
	spec := ChildSpec{ID: childID, Name: childID}
	if parent == nil {
		return spec
	}
	// Copy skills so caller can mutate without affecting parent
	if len(parent.Skills) > 0 {
		spec.Skills = make([]string, len(parent.Skills))
		copy(spec.Skills, parent.Skills)
	}
	spec.Version = parent.Version
	spec.Universe = parent.Universe
	return spec
}

// RecordChild increments the in-memory child count for the parent.
//
// Called by the kernel (WAU-core-kernel M4-3.3) AFTER successful
// engine.Replicate + registry.Heartbeat. Returns the new count.
//
// Why scheduler does NOT call this from Scheduler.Replicate:
//   - Library boundary: Scheduler.Replicate is a pure decision function
//   - Kernel is the executor; if engine.Replicate fails, no count should
//     be incremented (avoids rollback complexity)
//   - Keeps the policy a "stateless gate" + "post-success counter"
//
// Single-kernel deployments (the v0.8.0 target) can rely on the in-memory
// map. M4-4+ may replace this with Redis INCR for multi-kernel deployments.
func (p *ReplicationPolicy) RecordChild(parentID string) int {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.childCounts[parentID]++
	return p.childCounts[parentID]
}

// ChildCount returns the current child count (0 if none recorded).
//
// Safe for concurrent reads alongside RecordChild.
func (p *ReplicationPolicy) ChildCount(parentID string) int {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.childCounts[parentID]
}

// Reset clears the count for one parent (test helper + kernel reset).
//
// Useful for tests that share a policy across cases, and for kernel logic
// that wants to reset counts after, e.g., a parent Reset.
func (p *ReplicationPolicy) Reset(parentID string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.childCounts, parentID)
}

// RationaleFor builds a human-readable rationale string for the decision.
//
// Includes actual numbers so log debugging is direct (no need to grep for
// separate context lines). Example output:
//
//   "parent trust 0.92 ≥ 0.80, children 2 < 5"
//
// Returns an empty string if p is nil.
func (p *ReplicationPolicy) RationaleFor(parentID string, parentTrust float64, currentChildren int) string {
	if p == nil {
		return ""
	}
	return fmt.Sprintf("parent trust %.2f %s %.2f, children %d < %d",
		parentTrust,
		thresholdOp(parentTrust, p.MinParentTrust),
		p.MinParentTrust,
		currentChildren,
		p.MaxChildrenPerParent,
	)
}

// thresholdOp returns "≥" if value ≥ threshold, else "<". Used by RationaleFor.
func thresholdOp(value, threshold float64) string {
	if value >= threshold {
		return "≥"
	}
	return "<"
}