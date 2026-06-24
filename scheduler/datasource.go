package scheduler

import (
	"context"

	"github.com/wau/registry/registry"
)

// AgentMeta holds extended metadata for an agent (v0.7.0 fields).
//
// These fields are added on top of registry.AgentCard to support
// the 15-dimension scoring framework. They are typically populated
// by the registry at registration time, but can be extended via
// runtime updates.
type AgentMeta struct {
	// AuthLevel (0.0 - 1.0) — verified trust in the agent's identity
	// 1.0 = signed cert + out-of-band verified
	// 0.5 = self-signed only
	// 0.0 = no auth
	AuthLevel float64

	// SupportedInterfaces — list of protocols the agent accepts
	// e.g. ["A2A", "AFP", "MCP"]
	SupportedInterfaces []string

	// Region — geographic region code, e.g. "us-west-1", "eu-central-1"
	Region string

	// KernelVersion — minimum kernel version required (semver)
	KernelVersion string
}

// DefaultAgentMeta returns sensible defaults when fields are missing.
func DefaultAgentMeta() AgentMeta {
	return AgentMeta{
		AuthLevel:           1.0, // assume trusted by default
		SupportedInterfaces: []string{"A2A"},
		Region:              "default",
		KernelVersion:       "v0.5.0",
	}
}

// DataSource provides all 15-dim scoring data to the ScoringEngine.
//
// v0.7.0 W1: This abstraction lets the scheduler work with multiple
// backends (Redis / in-memory / wau-trust for trust) without hard-coding
// any specific store.
type DataSource interface {
	// ===== v0.7.0 W1: 7 维真算 =====

	// LatencyScore returns p99 latency score (0-1, lower is better).
	// Backed by Redis key `latency:{name}:p99` (rolling 5min window).
	// Returns 1.0 (perfect) if no data.
	LatencyScore(ctx context.Context, agentID string) (float64, error)

	// SuccessRate returns recent success rate (0-1).
	// Backed by Redis key `success:{name}:last100` (last 100 tasks).
	// Returns 0.5 (neutral) if no data.
	SuccessRate(ctx context.Context, agentID string) (float64, error)

	// BandwidthScore returns available bandwidth score (0-1).
	// Backed by Redis key `bandwidth:{name}:mbps` (from heartbeat).
	// Returns 1.0 if no data.
	BandwidthScore(ctx context.Context, agentID string) (float64, error)

	// HistoryCount returns log-normalized interaction count (0-1).
	// Backed by Redis key `tasks:{name}:total` (cumulative).
	// Returns 0.5 if no data.
	HistoryCount(ctx context.Context, agentID string) (float64, error)

	// ErrorRate returns recent error rate (0-1, lower is better).
	// Backed by Redis key `errors:{name}:last100`.
	// Returns 1.0 if no data.
	ErrorRate(ctx context.Context, agentID string) (float64, error)

	// ===== v0.7.0 W2: 4 维真算 =====

	// Availability returns uptime ratio (0-1).
	// Backed by Registry.FirstSeen / LastSeen.
	Availability(ctx context.Context, agentID string) (float64, error)

	// VersionCompat returns kernel↔agent semver compatibility (0-1).
	// Backed by Registry.AgentCard.Version / Kernel.Version.
	VersionCompat(ctx context.Context, agentID string) (float64, error)

	// GeoPenalty returns cross-region penalty (0.9-1.0).
	// Backed by AgentMeta.Region / source region.
	GeoPenalty(ctx context.Context, agentID string, sourceRegion string) (float64, error)

	// TrustScore returns the dynamic trust score (0-1).
	// Backed by wau-trust Engine. (W2 integration)
	// Returns 0.5 if no data.
	TrustScore(ctx context.Context, agentID string) (float64, error)

	// IsCold reports whether the agent has NO trust history (v0.8.0 M4-1).
	//
	// Returns true when:
	//   - The agent has never had any trust-relevant data recorded
	//     (no RecordSuccess / RecordFailure / Reset in wau-trust, and
	//     no latency / success / errors / bandwidth in scheduler's metrics)
	//
	// Used by cold routing (v0.8.0 M4-1.3) to identify "fresh" agents
	// that should be explored to accumulate data, vs "warm" agents
	// that can be ranked normally on the 15-dim scoring framework.
	IsCold(ctx context.Context, agentID string) (bool, error)

	// IsAsleep reports whether the agent is currently marked asleep (v0.8.0 M4-2).
	//
	// Returns true when:
	//   - The agent has been put to sleep via wau-trust Engine.Sleep
	//     and has not been woken yet (either via Wake or via Reset).
	//
	// Distinct from IsCold: a cold agent (no trust data) is NOT asleep —
	// it just has no data. An asleep agent has trust data but is currently
	// excluded from scheduling to save resources (low utilization window).
	//
	// Used by sleep policy (v0.8.0 M4-2.2) to filter asleep agents out of
	// the scheduling pool and to identify wake candidates during demand spikes.
	IsAsleep(ctx context.Context, agentID string) (bool, error)

	// Replicate creates a child trust entry via the underlying trust engine (v0.8.0 M4-3.2).
	//
	// Returns the actual computed child trust score after the engine applies
	// deterministic jitter + clamp. The engine writes the child score on success.
	//
	// Errors:
	//   - ErrReplicateNotImplemented: DataSource doesn't support Replicate
	//     (legacy mode or stub impl). Production callers must wrap with
	//     WauTrustDataSource (which delegates to wau-trust Engine.Replicate).
	//   - engine.ErrParentCold: parent has no trust data — caller should warm
	//     up via cold routing first.
	//   - engine.ErrInvalidFactor: inheritanceFactor out of [0.0, 1.0].
	//
	// Scheduler-level policy gates (MinParentTrust, MaxChildrenPerParent) are
	// NOT enforced here — those live in ReplicationPolicy.CanReplicate. The
	// engine itself also does not enforce MinParentTrust (per engine godoc);
	// the scheduler policy is the single source of truth for that gate.
	Replicate(ctx context.Context, parent, child string, inheritanceFactor float64) (float64, error)

	// ===== Agent metadata =====

	// GetMeta returns extended metadata for the agent.
	GetMeta(ctx context.Context, agentID string) (AgentMeta, error)
}

// DataSourceOption is a functional option for the DataSource.
type DataSourceOption func(*DataSourceConfig)

// DataSourceConfig holds DataSource configuration.
type DataSourceConfig struct {
	// PreferredProtocols — kernel's preferred protocols (intersection
	// with agent's SupportedInterfaces is what we score).
	PreferredProtocols []string

	// KernelVersion — current kernel version, used for VersionCompat.
	KernelVersion string
}

// WithPreferredProtocols sets the kernel's preferred protocols.
func WithPreferredProtocols(protocols []string) DataSourceOption {
	return func(c *DataSourceConfig) {
		c.PreferredProtocols = protocols
	}
}

// WithKernelVersion sets the kernel version for VersionCompat.
func WithKernelVersion(version string) DataSourceOption {
	return func(c *DataSourceConfig) {
		c.KernelVersion = version
	}
}

// Helper: pull AgentMeta fields from a registry.AgentCard.
//
// v0.7.0 W1: extended fields are not yet in registry.AgentCard,
// so we return DefaultAgentMeta() with Universe → Region mapping
// (universe name is treated as region for now).
func agentMetaFromCard(card *registry.AgentCard) AgentMeta {
	if card == nil {
		return DefaultAgentMeta()
	}
	meta := DefaultAgentMeta()
	if card.Universe != "" {
		meta.Region = card.Universe // temporary: universe = region
	}
	if card.Version != "" {
		meta.KernelVersion = card.Version
	}
	// AuthLevel + SupportedInterfaces are not in v0.6.0 AgentCard.
	// W1.5 / W2 work: extend registry.AgentCard with these fields
	// so agents can self-declare auth_level and supported_interfaces.
	return meta
}
