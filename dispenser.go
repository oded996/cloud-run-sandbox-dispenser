// Package main implements the Cloud Run Sandbox Dispenser:
//   - hostd: Session-isolated Sandbox Pool Host Supervisor (sandbox-pool)
//   - controld: Horizontally scalable Control Plane & Optimizer (sandbox-control-plane)
//   - ui: IAP-compatible Web Console & BFF (sandbox-ui)
//   - sbx: Decentralized High-Throughput CLI (gcloud run sandbox)
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Common errors returned by the store, host supervisor, and control plane.
var (
	ErrNotFound           = errors.New("object not found")
	ErrPreconditionFailed = errors.New("precondition failed (generation mismatch)")
	ErrHostFull           = errors.New("host session is at maximum sandbox density")
	ErrHostDraining       = errors.New("host session is draining and not accepting new sandboxes")
	ErrHostCrashed        = errors.New("host session has crashed")
	ErrInvalidID          = errors.New("invalid identifier: must match ^[a-zA-Z0-9][a-zA-Z0-9._-]{0,62}$")
	ErrPathTraversal      = errors.New("path traversal outside allowed directory boundary")
)

// SandboxStatus represents the lifecycle state of a sandbox.
type SandboxStatus string

// Supported SandboxStatus values.
const (
	StatusCreating  SandboxStatus = "CREATING"
	StatusRunning   SandboxStatus = "RUNNING"
	StatusMigrating SandboxStatus = "MIGRATING"
	StatusSuspended SandboxStatus = "SUSPENDED"
	StatusStopped   SandboxStatus = "STOPPED"
	StatusFailed    SandboxStatus = "FAILED"
	StatusDeleted   SandboxStatus = "DELETED"
)

// HostStatus represents the lifecycle state of a host clone session.
type HostStatus string

// Supported HostStatus values.
const (
	HostActive   HostStatus = "ACTIVE"
	HostDraining HostStatus = "DRAINING"
	HostDead     HostStatus = "DEAD"
	HostCrashed  HostStatus = "CRASHED"
)

// Dispenser protocol and client/server version constants.
const (
	DispenserVersion        = "1.2.0"
	DefaultMinClientVersion = "1.0.0"
)

// SandboxRecord is the strongly consistent ownership and configuration record
// stored at sandboxes/<sandbox_id>.json.
type SandboxRecord struct {
	SandboxID              string            `json:"sandbox_id"`
	Name                   string            `json:"name,omitempty"`
	HostID                 string            `json:"host_id"`
	PreviousHostID         string            `json:"previous_host_id,omitempty"`
	HostRevision           string            `json:"host_revision,omitempty"`
	EngineType             string            `json:"engine_type,omitempty"`
	PinnedHost             bool              `json:"pinned_host,omitempty"`
	Epoch                  int64             `json:"epoch"`
	Status                 SandboxStatus     `json:"status"`
	Template               string            `json:"template"`
	Persistent             bool              `json:"persistent"`
	CPU                    string            `json:"cpu"`
	Memory                 string            `json:"memory"`
	AllocatedCPUMillicores int               `json:"allocated_cpu_millicores,omitempty"`
	AllocatedMemoryBytes   int64             `json:"allocated_memory_bytes,omitempty"`
	UsedMemoryBytes        int64             `json:"used_memory_bytes,omitempty"`
	UsedMemoryPct          float64           `json:"used_memory_pct,omitempty"`
	PeakMemoryBytes        int64             `json:"peak_memory_bytes,omitempty"`
	CPUUsagePct            float64           `json:"cpu_usage_pct,omitempty"`
	PeakCPUUsagePct        float64           `json:"peak_cpu_usage_pct,omitempty"`
	AllowEgress            bool              `json:"allow_egress"`
	Env                    map[string]string `json:"env,omitempty"`
	Metadata               map[string]string `json:"metadata,omitempty"`
	BackgroundCommands     []string          `json:"background_commands,omitempty"`
	IdleTimeoutSec         int               `json:"idle_timeout_sec"`
	DisableAutoSuspend     bool              `json:"disable_auto_suspend,omitempty"`
	OverlayPath            string            `json:"overlay_path,omitempty"`
	BaseSnapshotID         string            `json:"base_snapshot_id,omitempty"`
	SandboxIP              string            `json:"sandbox_ip,omitempty"`
	PostgresPort           int               `json:"postgres_port,omitempty"`
	MigrationCount         int               `json:"migration_count"`
	CrashRecoveries        int               `json:"crash_recoveries"`
	TotalDowntimeMs        int64             `json:"total_downtime_ms"`
	// LastMigrationFreezeMs is the freeze (cutover) window of the most recent live migration:
	// the time the sandbox was not serving requests (source sbxMu held, status MIGRATING).
	LastMigrationFreezeMs int64 `json:"last_migration_freeze_ms,omitempty"`
	// LastMigrationTotalMs is the end-to-end duration of the most recent live migration,
	// including the zero-downtime Phase 1 pre-copy.
	LastMigrationTotalMs int64 `json:"last_migration_total_ms,omitempty"`
	// LastMigrationMode is MigrationModePrestagedDelta or MigrationModeGCSFull.
	LastMigrationMode string    `json:"last_migration_mode,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
	LastActiveAt      time.Time `json:"last_active_at"`
	LastSnapshotAt    time.Time `json:"last_snapshot_at,omitempty"`
}

// HostRecord represents a single session-isolated Cloud Run clone in sandbox-pool,
// stored at hosts/<host_id>.json (single-writer per host).
type HostRecord struct {
	HostID                  string                   `json:"host_id"`
	InstanceID              string                   `json:"instance_id"`
	Revision                string                   `json:"revision"`
	EngineType              string                   `json:"engine_type,omitempty"`
	HostCPU                 string                   `json:"host_cpu,omitempty"`
	HostMemory              string                   `json:"host_memory,omitempty"`
	TotalMemoryBytes        int64                    `json:"total_memory_bytes,omitempty"`
	UsedMemoryBytes         int64                    `json:"used_memory_bytes,omitempty"`
	AvailableMemoryBytes    int64                    `json:"available_memory_bytes,omitempty"`
	UsedMemoryPct           float64                  `json:"used_memory_pct,omitempty"`
	PeakMemoryPct           float64                  `json:"peak_memory_pct,omitempty"`
	AllocatedMemoryBytes    int64                    `json:"allocated_memory_bytes,omitempty"`
	MemoryOvercommitRatio   float64                  `json:"memory_overcommit_ratio,omitempty"`
	TotalCPUMillicores      int                      `json:"total_cpu_millicores,omitempty"`
	AllocatedCPUMillicores  int                      `json:"allocated_cpu_millicores,omitempty"`
	CPUOvercommitRatio      float64                  `json:"cpu_overcommit_ratio,omitempty"`
	CPUUsagePct             float64                  `json:"cpu_usage_pct,omitempty"`
	PeakCPUUsagePct         float64                  `json:"peak_cpu_usage_pct,omitempty"`
	OverloadState           string                   `json:"overload_state,omitempty"`
	RejectedCreationsMemory int                      `json:"rejected_creations_memory,omitempty"`
	HotMigrationsTriggered  int                      `json:"hot_migrations_triggered,omitempty"`
	Status                  HostStatus               `json:"status"`
	MaxDensity              int                      `json:"max_density"`
	ActiveSandboxes         int                      `json:"active_sandboxes"`
	FreeSlots               int                      `json:"free_slots"`
	Sandboxes               map[string]SandboxRecord `json:"sandboxes"`
	InstanceState           string                   `json:"instance_state"` // "busy" or "idle"
	StartedAt               time.Time                `json:"started_at"`
	LastHeartbeatAt         time.Time                `json:"last_heartbeat_at"`
	RecentEvents            []ActivityEvent          `json:"recent_events,omitempty"`
}

// HostResourceSample captures a point-in-time CPU, memory, and overcommit sample for a host.
type HostResourceSample struct {
	Timestamp              time.Time `json:"timestamp"`
	ActiveSandboxes        int       `json:"active_sandboxes"`
	UsedMemoryBytes        int64     `json:"used_memory_bytes"`
	AvailableMemoryBytes   int64     `json:"available_memory_bytes"`
	TotalMemoryBytes       int64     `json:"total_memory_bytes"`
	UsedMemoryPct          float64   `json:"used_memory_pct"`
	AllocatedMemoryBytes   int64     `json:"allocated_memory_bytes"`
	MemoryOvercommitRatio  float64   `json:"memory_overcommit_ratio"`
	AllocatedCPUMillicores int       `json:"allocated_cpu_millicores"`
	CPUUsagePct            float64   `json:"cpu_usage_pct"`
	CPUOvercommitRatio     float64   `json:"cpu_overcommit_ratio"`
	OverloadState          string    `json:"overload_state"`
}

// HostMetricsHistory stores rolling historical resource samples at metrics/hosts/<host_id>.json.
type HostMetricsHistory struct {
	HostID    string               `json:"host_id"`
	Samples   []HostResourceSample `json:"samples"`
	UpdatedAt time.Time            `json:"updated_at"`
}

// ActivityEvent captures an observability event in the fleet.
type ActivityEvent struct {
	EventID   string    `json:"event_id"`
	Timestamp time.Time `json:"timestamp"`
	Type      string    `json:"type"` // CREATE, EXEC, MIGRATE, CRASH_RECOVERED, DRAIN, COMPACT, SUSPEND, RESUME, SNAPSHOT, DELETE
	SandboxID string    `json:"sandbox_id,omitempty"`
	HostID    string    `json:"host_id,omitempty"`
	TargetID  string    `json:"target_id,omitempty"`
	Duration  string    `json:"duration,omitempty"`
	Message   string    `json:"message"`
}

// TemplateSpec defines a sandbox template (either a pre-warmed container template
// in service.yaml or a snapshot-backed template in GCS).
type TemplateSpec struct {
	Name         string            `json:"name"`
	Type         string            `json:"type"` // "prewarmed" or "snapshot"
	Image        string            `json:"image"`
	BaseTemplate string            `json:"base_template,omitempty"`
	SnapshotID   string            `json:"snapshot_id,omitempty"`
	FromSandbox  string            `json:"from_sandbox,omitempty"`
	Force        bool              `json:"force,omitempty"`
	OverlayPath  string            `json:"overlay_path,omitempty"`
	DefaultCmd   []string          `json:"default_cmd,omitempty"`
	DefaultEnv   map[string]string `json:"default_env,omitempty"`
	Description  string            `json:"description,omitempty"`
	CreatedAt    time.Time         `json:"created_at"`
}

// SnapshotRecord describes a saved filesystem snapshot in snapshots/<snapshot_id>.json.
type SnapshotRecord struct {
	SnapshotID    string    `json:"snapshot_id"`
	SourceSandbox string    `json:"source_sandbox"`
	Template      string    `json:"template"`
	TarPath       string    `json:"tar_path"`
	SizeBytes     int64     `json:"size_bytes"`
	CreatedAt     time.Time `json:"created_at"`
}

// FleetConfig holds fleet-wide dispenser settings stored at fleet/config.json.
type FleetConfig struct {
	MaxDensity               int       `json:"max_density"`
	HostCPU                  string    `json:"host_cpu"`
	HostMemory               string    `json:"host_memory"`
	MinFreeMemoryMB          int       `json:"min_free_memory_mb,omitempty"`
	MinFreeMemoryPct         float64   `json:"min_free_memory_pct,omitempty"`
	MaxMemoryOvercommitRatio float64   `json:"max_memory_overcommit_ratio,omitempty"`
	HotMemoryThresholdPct    float64   `json:"hot_memory_threshold_pct,omitempty"`
	HotCPUThresholdPct       float64   `json:"hot_cpu_threshold_pct,omitempty"`
	TargetRevision           string    `json:"target_revision"`
	PoolServiceURL           string    `json:"pool_service_url"`
	ControlPlaneURL          string    `json:"control_plane_url"`
	UIServiceURL             string    `json:"ui_service_url,omitempty"`
	MinClientVersion         string    `json:"min_client_version,omitempty"`
	ServerVersion            string    `json:"server_version,omitempty"`
	PoolRolloutTriggered     bool      `json:"pool_rollout_triggered,omitempty"`
	PeriodicSyncSec          int       `json:"periodic_sync_sec"`
	DefaultIdleTimeout       int       `json:"default_idle_timeout_sec"`
	HeartbeatTimeoutSec      int       `json:"heartbeat_timeout_sec"`
	GracePeriodSec           int       `json:"grace_period_sec"`
	AutoBinPack              bool      `json:"auto_bin_pack"`
	BinPackHysteresis        int       `json:"bin_pack_hysteresis"`
	UpdatedAt                time.Time `json:"updated_at"`
}

// DefaultFleetConfig returns default production settings for the dispenser.
func DefaultFleetConfig() FleetConfig {
	return FleetConfig{
		MaxDensity:               10,
		HostCPU:                  "4",
		HostMemory:               "8Gi",
		MinFreeMemoryMB:          512,
		MinFreeMemoryPct:         15.0,
		MaxMemoryOvercommitRatio: 2.0,
		HotMemoryThresholdPct:    85.0,
		HotCPUThresholdPct:       90.0,
		MinClientVersion:         DefaultMinClientVersion,
		ServerVersion:            DispenserVersion,
		PeriodicSyncSec:          10,
		DefaultIdleTimeout:       0,
		HeartbeatTimeoutSec:      15,
		GracePeriodSec:           600,
		AutoBinPack:              true,
		BinPackHysteresis:        1,
		UpdatedAt:                time.Now().UTC(),
	}
}

// CandidateHost is returned by GET /v1/candidates to let CLIs route directly to hosts.
type CandidateHost struct {
	HostID          string  `json:"host_id"`
	Revision        string  `json:"revision"`
	ActiveSandboxes int     `json:"active_sandboxes"`
	FreeSlots       int     `json:"free_slots"`
	MaxDensity      int     `json:"max_density"`
	UsedMemoryPct   float64 `json:"used_memory_pct,omitempty"`
	CPUUsagePct     float64 `json:"cpu_usage_pct,omitempty"`
}

// CandidatesResponse is the payload of GET /v1/candidates.
type CandidatesResponse struct {
	PoolServiceURL     string          `json:"pool_service_url"`
	TargetRevision     string          `json:"target_revision"`
	MaxDensity         int             `json:"max_density"`
	HostCPU            string          `json:"host_cpu,omitempty"`
	HostMemory         string          `json:"host_memory,omitempty"`
	MinClientVersion   string          `json:"min_client_version,omitempty"`
	ServerVersion      string          `json:"server_version,omitempty"`
	RefreshIntervalSec int             `json:"refresh_interval_sec"`
	Candidates         []CandidateHost `json:"candidates"`
	UpdatedAt          time.Time       `json:"updated_at"`
}

// FleetSummary is the compacted view stored at fleet/summary.json every 3s by the leader
// so horizontally scaled control-plane instances and the UI can sync in 1 GET.
type FleetSummary struct {
	LeaderID                   string                   `json:"leader_id"`
	Config                     FleetConfig              `json:"config"`
	Hosts                      map[string]HostRecord    `json:"hosts"`
	Sandboxes                  map[string]SandboxRecord `json:"sandboxes"`
	TotalHosts                 int                      `json:"total_hosts"`
	ActiveHosts                int                      `json:"active_hosts"`
	DrainingHosts              int                      `json:"draining_hosts"`
	DeadHosts                  int                      `json:"dead_hosts"`
	TotalSandboxes             int                      `json:"total_sandboxes"`
	RunningSandboxes           int                      `json:"running_sandboxes"`
	SuspendedSandboxes         int                      `json:"suspended_boxes"`
	TotalCapacitySlots         int                      `json:"total_capacity_slots"`
	UsedCapacitySlots          int                      `json:"used_capacity_slots"`
	BinPackEfficiencyPct       float64                  `json:"bin_pack_efficiency_pct"`
	FleetMemoryOvercommitRatio float64                  `json:"fleet_memory_overcommit_ratio"`
	FleetCPUOvercommitRatio    float64                  `json:"fleet_cpu_overcommit_ratio"`
	TotalMigrations            int                      `json:"total_migrations"`
	TotalHotMigrations         int                      `json:"total_hot_migrations"`
	TotalMemoryRejections      int                      `json:"total_memory_rejections"`
	TotalCrashRecoveries       int                      `json:"total_crash_recoveries"`
	FleetUptimeSLAPct          float64                  `json:"fleet_uptime_sla_pct"`
	RecentEvents               []ActivityEvent          `json:"recent_events"`
	UpdatedAt                  time.Time                `json:"updated_at"`
}

// SecurityPolicy governs Cloud IAP access to `sandbox-ui` and Cloud Run Invoker access
// to `sandbox-control-plane` and `sandbox-pool`, persisted at `fleet/security-policy.json`.
type SecurityPolicy struct {
	IAPEnabled           bool      `json:"iap_enabled"`
	DeployerEmail        string    `json:"deployer_email,omitempty"`
	CurrentUser          string    `json:"current_user,omitempty"`
	CurrentMember        string    `json:"current_member,omitempty"`
	UIAccessMembers      []string  `json:"ui_access_members"`
	BackendAccessMembers []string  `json:"backend_access_members"`
	ProtectedMembers     []string  `json:"protected_members,omitempty"`
	UpdatedAt            time.Time `json:"updated_at"`
	UpdatedBy            string    `json:"updated_by,omitempty"`
}

// NormalizeIAMMember normalizes a user/group/serviceAccount string into a canonical IAM member string.
func NormalizeIAMMember(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return ""
	}
	if strings.HasPrefix(s, "accounts.google.com:") {
		s = strings.TrimPrefix(s, "accounts.google.com:")
	}
	for _, p := range []string{"user:", "group:", "serviceAccount:", "domain:"} {
		if strings.HasPrefix(s, p) {
			return p + strings.ToLower(strings.TrimSpace(strings.TrimPrefix(s, p)))
		}
	}
	lower := strings.ToLower(s)
	if strings.HasSuffix(lower, ".gserviceaccount.com") {
		return "serviceAccount:" + lower
	}
	return "user:" + lower
}

// ExtractCallerEmail extracts the authenticated caller email from Cloud IAP (`X-Goog-Authenticated-User-Email`),
// BFF forwarded header (`X-Sbx-User-Email`), OIDC bearer JWT payload, or `DEPLOYER_EMAIL` fallback.
func ExtractCallerEmail(r *http.Request, fallbackEmail string) string {
	if r != nil {
		if iapHdr := strings.TrimSpace(r.Header.Get("X-Goog-Authenticated-User-Email")); iapHdr != "" {
			iapHdr = strings.TrimPrefix(iapHdr, "accounts.google.com:")
			if idx := strings.IndexByte(iapHdr, ':'); idx >= 0 {
				iapHdr = iapHdr[idx+1:]
			}
			if iapHdr != "" {
				return strings.ToLower(strings.TrimSpace(iapHdr))
			}
		}
		if usrHdr := strings.TrimSpace(r.Header.Get("X-Sbx-User-Email")); usrHdr != "" {
			usrHdr = strings.TrimPrefix(usrHdr, "user:")
			if usrHdr != "" {
				return strings.ToLower(strings.TrimSpace(usrHdr))
			}
		}
		if auth := strings.TrimSpace(r.Header.Get("Authorization")); strings.HasPrefix(auth, "Bearer ") {
			tok := strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
			parts := strings.Split(tok, ".")
			if len(parts) == 3 {
				if payload, err := base64.RawURLEncoding.DecodeString(parts[1]); err == nil {
					var claims struct {
						Email string `json:"email"`
					}
					if json.Unmarshal(payload, &claims) == nil && claims.Email != "" {
						return strings.ToLower(strings.TrimSpace(claims.Email))
					}
				}
			}
		}
	}
	if envEmail := strings.TrimSpace(os.Getenv("DEPLOYER_EMAIL")); envEmail != "" {
		return strings.ToLower(strings.TrimPrefix(envEmail, "user:"))
	}
	if fallbackEmail != "" {
		return strings.ToLower(strings.TrimPrefix(fallbackEmail, "user:"))
	}
	return "admin@example.com"
}

// DefaultGCPProject resolves the default Google Cloud project ID from environment variables
// (`GOOGLE_CLOUD_PROJECT`, `GCLOUD_PROJECT`, `DEVSHELL_PROJECT_ID`, `CLOUDSDK_CORE_PROJECT`) or falls back to `"my-gcp-project"`.
func DefaultGCPProject() string {
	for _, env := range []string{"GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT", "DEVSHELL_PROJECT_ID", "CLOUDSDK_CORE_PROJECT"} {
		if v := strings.TrimSpace(os.Getenv(env)); v != "" {
			return v
		}
	}
	return "my-gcp-project"
}

// LeaderLock is stored at fleet/leader-lock.json and updated via CAS.
type LeaderLock struct {
	LeaderID   string    `json:"leader_id"`
	AcquiredAt time.Time `json:"acquired_at"`
	ExpiresAt  time.Time `json:"expires_at"`
}

// MigrationTargetsRequest is sent by a draining host to POST /v1/migration-targets.
type MigrationTargetsRequest struct {
	SourceHostID string   `json:"source_host_id"`
	SandboxIDs   []string `json:"sandbox_ids"`
	Reason       string   `json:"reason"`
}

// MigrationTargetsResponse maps each sandbox_id to its destination host_session_id.
type MigrationTargetsResponse struct {
	Targets        map[string]string `json:"targets"` // sandbox_id -> target_host_id
	PoolServiceURL string            `json:"pool_service_url"`
}

// MisdirectedSandboxError is returned with HTTP 409 when a sandbox is on another host.
type MisdirectedSandboxError struct {
	ErrorCode     string        `json:"error"`
	SandboxID     string        `json:"sandbox_id"`
	HostSessionID string        `json:"host_session_id"`
	Epoch         int64         `json:"epoch"`
	Status        SandboxStatus `json:"status"`
}

// CreateSandboxRequest is the payload for POST /__session/create.
type CreateSandboxRequest struct {
	SandboxID          string            `json:"sandbox_id,omitempty"`
	Name               string            `json:"name,omitempty"`
	Template           string            `json:"template"`
	Persistent         bool              `json:"persistent"`
	PinnedHost         bool              `json:"pinned_host,omitempty"`
	TargetHostID       string            `json:"target_host_id,omitempty"`
	Force              bool              `json:"force,omitempty"`
	CPU                string            `json:"cpu"`
	Memory             string            `json:"memory"`
	AllowEgress        bool              `json:"allow_egress"`
	FromSnapshotID     string            `json:"from_snapshot_id,omitempty"`
	Env                map[string]string `json:"env,omitempty"`
	Metadata           map[string]string `json:"metadata,omitempty"`
	IdleTimeoutSec     int               `json:"idle_timeout_sec,omitempty"`
	DisableAutoSuspend bool              `json:"disable_auto_suspend,omitempty"`
}

// ExecRequest is the payload for POST /__session/exec and POST /__session/exec-stream.
type ExecRequest struct {
	SandboxID   string            `json:"sandbox_id"`
	Command     []string          `json:"command"`
	Workdir     string            `json:"workdir,omitempty"`
	Env         map[string]string `json:"env,omitempty"`
	AllowEgress *bool             `json:"allow_egress,omitempty"`
	Background  bool              `json:"background,omitempty"`
	TimeoutSec  int               `json:"timeout_sec,omitempty"`
	Stream      bool              `json:"stream,omitempty"`
}

// ExecResponse is the response from POST /__session/exec.
type ExecResponse struct {
	SandboxID  string `json:"sandbox_id"`
	HostID     string `json:"host_id"`
	Epoch      int64  `json:"epoch"`
	ExitCode   int    `json:"exit_code"`
	Stdout     string `json:"stdout"`
	Stderr     string `json:"stderr"`
	PID        int    `json:"pid,omitempty"`
	DurationMs int64  `json:"duration_ms"`
}

// ExecStreamEvent is an incremental Server-Sent Events (SSE) payload emitted by POST /__session/exec-stream.
type ExecStreamEvent struct {
	Type       string `json:"type"` // "start", "stdout", "stderr", "exit", "error"
	Data       string `json:"data,omitempty"`
	SandboxID  string `json:"sandbox_id,omitempty"`
	HostID     string `json:"host_id,omitempty"`
	Epoch      int64  `json:"epoch,omitempty"`
	ExitCode   int    `json:"exit_code,omitempty"`
	DurationMs int64  `json:"duration_ms,omitempty"`
	Error      string `json:"error,omitempty"`
}

// FileCopyRequest is the payload for POST /__session/cp (upload or download).
type FileCopyRequest struct {
	SandboxID string `json:"sandbox_id"`
	Direction string `json:"direction"` // "upload" or "download"
	Path      string `json:"path"`
	Content   []byte `json:"content,omitempty"`
	Mode      uint32 `json:"mode,omitempty"`
}

// FileCopyResponse is the response from POST /__session/cp.
type FileCopyResponse struct {
	SandboxID string `json:"sandbox_id"`
	Path      string `json:"path"`
	Content   []byte `json:"content,omitempty"`
	SizeBytes int64  `json:"size_bytes"`
}

// CopySandboxRequest is the payload for POST /__session/copy (CoW fork or cross-host clone).
type CopySandboxRequest struct {
	SourceSandboxID string `json:"source_sandbox_id"`
	TargetSandboxID string `json:"target_sandbox_id"`
	Name            string `json:"name,omitempty"`
	TargetHostID    string `json:"target_host_id,omitempty"`
	Persistent      bool   `json:"persistent,omitempty"`
	Force           bool   `json:"force,omitempty"`
}

// MigrateSandboxRequest is the payload for POST /__session/migrate.
type MigrateSandboxRequest struct {
	SandboxID    string `json:"sandbox_id"`
	TargetHostID string `json:"target_host_id,omitempty"`
	PinnedHost   bool   `json:"pinned_host,omitempty"`
}

// HydrateRequest is the payload for POST /__internal/hydrate on a target host.
//
// For Two-Phase Pre-Copy live migration (Phase 2), Prestaged is true: the target already holds a
// warm copy of the base filesystem (from POST /__internal/prestage), so the source sends only the
// delta of files modified since the pre-copy (DeltaTar inline, or DeltaTarBlob in the Store when
// the delta exceeds the inline request-size limit) plus DeletedPaths. No GCS round trip is needed
// during the cutover.
type HydrateRequest struct {
	Record     SandboxRecord `json:"record"`
	OverlayTar string        `json:"overlay_tar,omitempty"`
	Reason     string        `json:"reason"`

	Prestaged    bool     `json:"prestaged,omitempty"`
	DeltaTar     []byte   `json:"delta_tar,omitempty"`
	DeltaTarBlob string   `json:"delta_tar_blob,omitempty"`
	DeletedPaths []string `json:"deleted_paths,omitempty"`
	// MigrationStartUnixMs / FreezeStartUnixMs are the source host's wall-clock timestamps for
	// the start of Phase 1 (pre-copy) and Phase 2 (freeze), used to report total_ms / freeze_ms.
	MigrationStartUnixMs int64 `json:"migration_start_unix_ms,omitempty"`
	FreezeStartUnixMs    int64 `json:"freeze_start_unix_ms,omitempty"`
}

// PrestageRequest is Phase 1 of Two-Phase Pre-Copy live migration (POST /__internal/prestage,
// alias /__session/prestage). The source host streams the base filesystem tar of a RUNNING
// sandbox to the target host, which unpacks it and warms the container without claiming
// ownership. The body is multipart/form-data with a "record" JSON part (this struct, without
// BaseTarBytes) followed by a "base_tar" binary part; application/json with inline BaseTarBytes
// (or BaseTarBlob, a Store path for tars above the inline request-size limit) is also accepted.
type PrestageRequest struct {
	Record       SandboxRecord `json:"record"`
	Reason       string        `json:"reason,omitempty"`
	SourceHostID string        `json:"source_host_id,omitempty"`
	BaseTarBytes []byte        `json:"base_tar_bytes,omitempty"`
	BaseTarBlob  string        `json:"base_tar_blob,omitempty"`
}

// PrestageResponse is returned by POST /__internal/prestage.
type PrestageResponse struct {
	SandboxID  string `json:"sandbox_id"`
	HostID     string `json:"host_id"`
	BaseBytes  int64  `json:"base_bytes"`
	PrestageMs int64  `json:"prestage_ms"`
}

// Live migration modes recorded in SandboxRecord.LastMigrationMode.
const (
	// MigrationModePrestagedDelta: Two-Phase Pre-Copy (warm base copy + direct delta cutover).
	MigrationModePrestagedDelta = "prestaged_delta"
	// MigrationModeGCSFull: stop-and-copy of the full overlay through the Store (fallback).
	MigrationModeGCSFull = "gcs_full"
)

// DeployFleetRequest is the payload for POST /v1/deploy on sandbox-control-plane.
type DeployFleetRequest struct {
	Project    string `json:"project"`
	Region     string `json:"region"`
	Bucket     string `json:"bucket"`
	Image      string `json:"image,omitempty"`
	HostCPU    string `json:"host_cpu,omitempty"`
	HostMemory string `json:"host_memory,omitempty"`
	MaxDensity int    `json:"max_density,omitempty"`
	DeployUI   *bool  `json:"deploy_ui,omitempty"`
}

// DeployFleetResponse is returned by POST /v1/deploy after sandbox-control-plane
// provisions or reconciles sandbox-pool and sandbox-ui.
type DeployFleetResponse struct {
	Status          string `json:"status"`
	Project         string `json:"project"`
	Region          string `json:"region"`
	Bucket          string `json:"bucket"`
	Image           string `json:"image"`
	ControlPlaneURL string `json:"control_plane_url"`
	PoolServiceURL  string `json:"pool_service_url"`
	UIServiceURL    string `json:"ui_service_url,omitempty"`
	HostCPU         string `json:"host_cpu"`
	HostMemory      string `json:"host_memory"`
	MaxDensity      int    `json:"max_density"`
	ServerVersion   string `json:"server_version"`
}

// CompareSemver compares two semantic version strings (e.g. "1.2.0" vs "1.0.0").
// It returns -1 if v1 < v2, 0 if v1 == v2, and +1 if v1 > v2.
func CompareSemver(v1, v2 string) int {
	parse := func(s string) [3]int {
		s = strings.TrimPrefix(strings.TrimSpace(s), "v")
		if idx := strings.IndexAny(s, "-+"); idx >= 0 {
			s = s[:idx]
		}
		parts := strings.Split(s, ".")
		var out [3]int
		for i := 0; i < 3 && i < len(parts); i++ {
			out[i], _ = strconv.Atoi(parts[i])
		}
		return out
	}
	a := parse(v1)
	b := parse(v2)
	for i := 0; i < 3; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return 0
}

// CheckClientVersion inspects the X-Sbx-Client-Version request header against minVersion.
// If the header is present and older than minVersion, it returns an error.
func CheckClientVersion(r *http.Request, minVersion string) error {
	if r == nil {
		return nil
	}
	clientVer := strings.TrimSpace(r.Header.Get("X-Sbx-Client-Version"))
	if clientVer == "" || strings.TrimSpace(minVersion) == "" {
		return nil
	}
	if CompareSemver(clientVer, minVersion) < 0 {
		return fmt.Errorf("client version %s is below minimum required version %s; please upgrade your sbx client", clientVer, minVersion)
	}
	return nil
}

// WriteUpgradeRequired writes an HTTP 426 Upgrade Required JSON response.
func WriteUpgradeRequired(w http.ResponseWriter, r *http.Request, minVersion string) {
	clientVer := ""
	if r != nil {
		clientVer = strings.TrimSpace(r.Header.Get("X-Sbx-Client-Version"))
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Sbx-Server-Version", DispenserVersion)
	w.Header().Set("X-Sbx-Min-Client-Version", minVersion)
	w.WriteHeader(http.StatusUpgradeRequired)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error":              "client_upgrade_required",
		"client_version":     clientVer,
		"min_client_version": minVersion,
		"server_version":     DispenserVersion,
		"message":            fmt.Sprintf("Client version %s is below minimum required version %s; please upgrade your sbx client.", clientVer, minVersion),
	})
}

var (
	validIDRegex       = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,62}$`)
	validCPURegex      = regexp.MustCompile(`^([0-9]+(\.[0-9]+)?|[1-9][0-9]*m)$`)
	validMemoryRegex   = regexp.MustCompile(`^[1-9][0-9]*(\.[0-9]+)?(Mi|Gi|M|G|Ki|K)$`)
	distributedIDRegex = regexp.MustCompile(`^sbx-([a-zA-Z0-9][a-zA-Z0-9._-]*)-[0-9a-f]{8}$`)
)

// GenerateDistributedSandboxID generates a globally unique sandbox identifier on the creating host
// in the format `sbx-<hostID>-<8_random_hex_chars>` (e.g., `sbx-host-742e5bf0-a1b2c3d4`).
// This allows lock-free distributed creation across warm pool hosts even when callers pass duplicate names,
// while embedding the initial host session ID for zero-lookup direct routing.
func GenerateDistributedSandboxID(hostID string) string {
	cleanHost := strings.TrimSpace(hostID)
	if cleanHost == "" {
		cleanHost = "host"
	}
	if len(cleanHost) > 45 {
		cleanHost = cleanHost[:45]
	}
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("sbx-%s-%08x", cleanHost, time.Now().UnixNano()&0xffffffff)
	}
	return fmt.Sprintf("sbx-%s-%s", cleanHost, hex.EncodeToString(b))
}

// ExtractInitialHostFromSandboxID extracts the embedded initial host session ID from a distributed
// sandbox ID of the form `sbx-<hostID>-<8hex>`, or returns "" if the ID does not match that format.
func ExtractInitialHostFromSandboxID(sandboxID string) string {
	m := distributedIDRegex.FindStringSubmatch(strings.TrimSpace(sandboxID))
	if len(m) == 2 {
		return m[1]
	}
	return ""
}

// ValidateID strictly validates an identifier (sandbox_id, host_id, template, snapshot_id)
// against an allow-list regex to prevent command injection and path traversal.
func ValidateID(id string) error {
	if !validIDRegex.MatchString(id) || strings.Contains(id, "..") {
		return fmt.Errorf("%w: %q", ErrInvalidID, id)
	}
	return nil
}

// ValidateCPUMemory validates CPU (positive number or millicores) and Memory (e.g. 512Mi, 2Gi).
func ValidateCPUMemory(cpu, memory string) error {
	if cpu != "" {
		if !validCPURegex.MatchString(cpu) {
			return fmt.Errorf("invalid cpu %q (expected positive number or millicores like '1', '2', '500m')", cpu)
		}
		if !strings.HasSuffix(cpu, "m") {
			if v, err := strconv.ParseFloat(cpu, 64); err != nil || v <= 0 {
				return fmt.Errorf("invalid cpu %q: must be > 0", cpu)
			}
		}
	}
	if memory != "" {
		if !validMemoryRegex.MatchString(memory) {
			return fmt.Errorf("invalid memory %q (expected format like '512Mi' or '2Gi')", memory)
		}
	}
	return nil
}

// ParseMemoryBytes parses a memory string like "512Mi", "1Gi", "8Gi" into bytes.
func ParseMemoryBytes(memStr string) int64 {
	s := strings.TrimSpace(memStr)
	if s == "" {
		return 1 << 30 // 1 GiB default
	}
	type suffixMult struct {
		suffix string
		mult   float64
	}
	for _, sm := range []suffixMult{
		{"Gi", float64(1 << 30)},
		{"Mi", float64(1 << 20)},
		{"Ki", float64(1 << 10)},
		{"G", 1e9},
		{"M", 1e6},
		{"K", 1e3},
	} {
		if strings.HasSuffix(s, sm.suffix) {
			numStr := strings.TrimSpace(strings.TrimSuffix(s, sm.suffix))
			if v, err := strconv.ParseFloat(numStr, 64); err == nil && v > 0 {
				return int64(v * sm.mult)
			}
		}
	}
	if v, err := strconv.ParseInt(s, 10, 64); err == nil && v > 0 {
		return v
	}
	return 1 << 30
}

// ParseCPUMillicores parses a CPU string like "1", "4", "500m", "0.5" into millicores.
func ParseCPUMillicores(cpuStr string) int {
	s := strings.TrimSpace(cpuStr)
	if s == "" {
		return 1000
	}
	if strings.HasSuffix(s, "m") {
		if v, err := strconv.Atoi(strings.TrimSuffix(s, "m")); err == nil && v > 0 {
			return v
		}
	}
	if v, err := strconv.ParseFloat(s, 64); err == nil && v > 0 {
		return int(v * 1000.0)
	}
	return 1000
}

// SafeResolvePath resolves a target path inside rootDir and enforces that it stays
// strictly within rootDir (with trailing slash boundary check).
func SafeResolvePath(rootDir, relPath string) (string, error) {
	cleanRoot := filepath.Clean(rootDir)
	joined := filepath.Clean(filepath.Join(cleanRoot, filepath.Clean("/"+relPath)))
	if joined != cleanRoot && !strings.HasPrefix(joined, cleanRoot+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: %q is outside %q", ErrPathTraversal, relPath, cleanRoot)
	}
	return joined, nil
}

// NewRandomID generates a cryptographically random identifier with the given prefix.
func NewRandomID(prefix string) string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano()%1000000)
	}
	if prefix == "" {
		return hex.EncodeToString(b)
	}
	return fmt.Sprintf("%s-%s", prefix, hex.EncodeToString(b))
}

// Store defines the persistence and state coordination interface for the dispenser.
// It supports strongly consistent CAS operations (ifGenerationMatch) as well as
// blob storage for filesystem overlay tarballs.
type Store interface {
	// ReadJSON reads an object and returns its current generation (0 if not found).
	ReadJSON(ctx context.Context, path string, dest any) (gen int64, exists bool, err error)
	// WriteJSONCAS writes an object only if its generation matches expectedGen.
	// If expectedGen == 0, the object must not exist (create-only).
	// If expectedGen < 0, the write is unconditional (last-writer-wins).
	WriteJSONCAS(ctx context.Context, path string, data any, expectedGen int64) (newGen int64, err error)
	// DeleteObject deletes an object if it exists.
	DeleteObject(ctx context.Context, path string) error
	// ListObjects returns all object paths matching the given prefix.
	ListObjects(ctx context.Context, prefix string) ([]string, error)
	// WriteBlob writes binary data (such as a sandbox overlay tarball) to the store.
	WriteBlob(ctx context.Context, path string, r io.Reader) (int64, error)
	// ReadBlob opens a reader for binary data from the store.
	ReadBlob(ctx context.Context, path string) (io.ReadCloser, bool, error)
	// LocalBlobPath returns a local filesystem path (e.g. on /mnt/gcs) if available,
	// or materializes/syncs it so `/usr/local/gcp/bin/sandbox` can access it directly.
	LocalBlobPath(ctx context.Context, blobPath string, forWrite bool) (localPath string, commit func() error, err error)
}

type memEntry struct {
	data []byte
	gen  int64
}

// MemoryStore is a thread-safe, in-memory implementation of Store with full CAS
// generation semantics and temporary local blob staging for hermetic unit tests.
type MemoryStore struct {
	mu      sync.RWMutex
	objects map[string]memEntry
	blobDir string
}

// NewMemoryStore creates a new hermetic MemoryStore backed by a temp dir for blob files.
func NewMemoryStore(blobDir string) *MemoryStore {
	return &MemoryStore{
		objects: make(map[string]memEntry),
		blobDir: blobDir,
	}
}

// ReadJSON reads and unmarshals JSON from memory along with its generation number.
func (m *MemoryStore) ReadJSON(_ context.Context, path string, dest any) (int64, bool, error) {
	m.mu.RLock()
	entry, ok := m.objects[path]
	m.mu.RUnlock()
	if !ok {
		return 0, false, nil
	}
	if dest != nil {
		if err := json.Unmarshal(entry.data, dest); err != nil {
			return 0, false, err
		}
	}
	return entry.gen, true, nil
}

// WriteJSONCAS atomically writes JSON if expectedGen matches.
func (m *MemoryStore) WriteJSONCAS(_ context.Context, path string, data any, expectedGen int64) (int64, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, exists := m.objects[path]
	if expectedGen == 0 && exists {
		return cur.gen, ErrPreconditionFailed
	}
	if expectedGen > 0 && (!exists || cur.gen != expectedGen) {
		return cur.gen, ErrPreconditionFailed
	}
	newGen := cur.gen + 1
	if newGen <= 0 {
		newGen = 1
	}
	m.objects[path] = memEntry{data: b, gen: newGen}
	return newGen, nil
}

// DeleteObject removes path from memory.
func (m *MemoryStore) DeleteObject(_ context.Context, path string) error {
	m.mu.Lock()
	delete(m.objects, path)
	m.mu.Unlock()
	return nil
}

// ListObjects returns sorted keys with the given prefix.
func (m *MemoryStore) ListObjects(_ context.Context, prefix string) ([]string, error) {
	m.mu.RLock()
	var out []string
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	m.mu.RUnlock()
	sort.Strings(out)
	return out, nil
}

// WriteBlob writes binary data into the memory store's map.
func (m *MemoryStore) WriteBlob(_ context.Context, path string, r io.Reader) (int64, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	m.mu.Lock()
	cur := m.objects[path]
	m.objects[path] = memEntry{data: b, gen: cur.gen + 1}
	m.mu.Unlock()
	return int64(len(b)), nil
}

// ReadBlob returns a reader for binary data from the memory store.
func (m *MemoryStore) ReadBlob(_ context.Context, path string) (io.ReadCloser, bool, error) {
	m.mu.RLock()
	entry, ok := m.objects[path]
	m.mu.RUnlock()
	if !ok {
		return nil, false, nil
	}
	return io.NopCloser(bytes.NewReader(entry.data)), true, nil
}

// LocalBlobPath materializes a blob to a local file in m.blobDir so commands can read/write it.
func (m *MemoryStore) LocalBlobPath(ctx context.Context, blobPath string, forWrite bool) (string, func() error, error) {
	base := m.blobDir
	if base == "" {
		base = os.TempDir()
	}
	resolved, err := SafeResolvePath(base, blobPath)
	if err != nil {
		return "", nil, err
	}
	if err := os.MkdirAll(filepath.Dir(resolved), 0755); err != nil {
		return "", nil, err
	}
	if !forWrite {
		rc, exists, err := m.ReadBlob(ctx, blobPath)
		if err != nil {
			return "", nil, err
		}
		if !exists {
			return resolved, func() error { return nil }, ErrNotFound
		}
		defer rc.Close()
		b, err := io.ReadAll(rc)
		if err != nil {
			return "", nil, err
		}
		if err := os.WriteFile(resolved, b, 0644); err != nil {
			return "", nil, err
		}
		return resolved, func() error { return nil }, nil
	}
	commit := func() error {
		f, err := os.Open(resolved)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = m.WriteBlob(ctx, blobPath, f)
		return err
	}
	return resolved, commit, nil
}

// GCSStore implements Store using the Google Cloud Storage JSON HTTP API for
// atomic Compare-And-Swap (`ifGenerationMatch`) on JSON state files, and uses
// either GCSFuse (`/mnt/gcs`) or GCS media streaming for large overlay tarballs.
type GCSStore struct {
	BucketName string
	MountRoot  string // e.g., "/mnt/gcs" when GCSFuse is mounted
	HTTPClient *http.Client
	BaseURL    string // defaults to "https://storage.googleapis.com"
	TokenFunc  func(ctx context.Context) (string, error)

	// Fallback local directory if BucketName == "" (local filesystem mode)
	localMu sync.Mutex
}

type gcsStoredEnvelope struct {
	Gen  int64           `json:"_gen"`
	Data json.RawMessage `json:"data"`
}

// NewGCSStore constructs a GCSStore for the given bucket and optional GCSFuse mount directory.
func NewGCSStore(bucketName, mountRoot string) *GCSStore {
	return &GCSStore{
		BucketName: bucketName,
		MountRoot:  mountRoot,
		HTTPClient: &http.Client{Timeout: 30 * time.Second},
		BaseURL:    "https://storage.googleapis.com",
		TokenFunc:  DefaultAccessToken,
	}
}

func (g *GCSStore) baseURL() string {
	if g.BaseURL != "" {
		return strings.TrimRight(g.BaseURL, "/")
	}
	return "https://storage.googleapis.com"
}

func (g *GCSStore) doReq(ctx context.Context, method, rawURL string, body io.Reader, contentType string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, rawURL, body)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if g.TokenFunc != nil {
		if tok, err := g.TokenFunc(ctx); err == nil && tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
	}
	client := g.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	return client.Do(req)
}

// ReadJSON reads a JSON object from GCS (or local MountRoot if BucketName is empty)
// and returns its generation number.
func (g *GCSStore) ReadJSON(ctx context.Context, objectPath string, dest any) (int64, bool, error) {
	if g.BucketName == "" {
		return g.readJSONLocal(objectPath, dest)
	}
	escapedObj := url.PathEscape(strings.TrimPrefix(objectPath, "/"))
	u := fmt.Sprintf("%s/storage/v1/b/%s/o/%s?alt=media", g.baseURL(), url.PathEscape(g.BucketName), escapedObj)
	resp, err := g.doReq(ctx, http.MethodGet, u, nil, "")
	if err != nil {
		return 0, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return 0, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return 0, false, fmt.Errorf("GCS GET %s failed with status %d: %s", objectPath, resp.StatusCode, string(b))
	}
	gen, _ := strconv.ParseInt(resp.Header.Get("X-Goog-Generation"), 10, 64)
	if gen == 0 {
		gen = 1
	}
	if dest != nil {
		if err := json.NewDecoder(resp.Body).Decode(dest); err != nil {
			return 0, false, err
		}
	}
	return gen, true, nil
}

// WriteJSONCAS writes a JSON object to GCS with `ifGenerationMatch` when expectedGen >= 0.
func (g *GCSStore) WriteJSONCAS(ctx context.Context, objectPath string, data any, expectedGen int64) (int64, error) {
	if g.BucketName == "" {
		return g.writeJSONCASLocal(objectPath, data, expectedGen)
	}
	b, err := json.Marshal(data)
	if err != nil {
		return 0, err
	}
	cleanPath := strings.TrimPrefix(objectPath, "/")
	q := url.Values{}
	q.Set("uploadType", "media")
	q.Set("name", cleanPath)
	if expectedGen >= 0 {
		q.Set("ifGenerationMatch", strconv.FormatInt(expectedGen, 10))
	}
	u := fmt.Sprintf("%s/upload/storage/v1/b/%s/o?%s", g.baseURL(), url.PathEscape(g.BucketName), q.Encode())

	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(300*(1<<(attempt-1))) * time.Millisecond)
		}
		resp, err := g.doReq(ctx, http.MethodPost, u, bytes.NewReader(b), "application/json")
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode == http.StatusPreconditionFailed {
			resp.Body.Close()
			return 0, ErrPreconditionFailed
		}
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			lastErr = fmt.Errorf("GCS POST %s failed with status %d: %s", objectPath, resp.StatusCode, string(errBody))
			continue
		}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
			errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			return 0, fmt.Errorf("GCS POST %s failed with status %d: %s", objectPath, resp.StatusCode, string(errBody))
		}
		var meta struct {
			Generation string `json:"generation"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&meta)
		resp.Body.Close()
		newGen, _ := strconv.ParseInt(meta.Generation, 10, 64)
		if newGen == 0 {
			newGen = 1
		}
		return newGen, nil
	}
	return 0, lastErr
}

// DeleteObject deletes an object from GCS.
func (g *GCSStore) DeleteObject(ctx context.Context, objectPath string) error {
	if g.BucketName == "" {
		resolved, err := SafeResolvePath(g.MountRoot, objectPath)
		if err != nil {
			return err
		}
		g.localMu.Lock()
		defer g.localMu.Unlock()
		err = os.Remove(resolved)
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	escapedObj := url.PathEscape(strings.TrimPrefix(objectPath, "/"))
	u := fmt.Sprintf("%s/storage/v1/b/%s/o/%s", g.baseURL(), url.PathEscape(g.BucketName), escapedObj)
	resp, err := g.doReq(ctx, http.MethodDelete, u, nil, "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK {
		return nil
	}
	errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return fmt.Errorf("GCS DELETE %s failed with status %d: %s", objectPath, resp.StatusCode, string(errBody))
}

// ListObjects lists all object names matching prefix in GCS.
func (g *GCSStore) ListObjects(ctx context.Context, prefix string) ([]string, error) {
	if g.BucketName == "" {
		return g.listObjectsLocal(prefix)
	}
	var items []string
	pageToken := ""
	cleanPrefix := strings.TrimPrefix(prefix, "/")
	for {
		q := url.Values{}
		if cleanPrefix != "" {
			q.Set("prefix", cleanPrefix)
		}
		if pageToken != "" {
			q.Set("pageToken", pageToken)
		}
		u := fmt.Sprintf("%s/storage/v1/b/%s/o?%s", g.baseURL(), url.PathEscape(g.BucketName), q.Encode())
		resp, err := g.doReq(ctx, http.MethodGet, u, nil, "")
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			return nil, fmt.Errorf("GCS LIST %s failed with status %d: %s", prefix, resp.StatusCode, string(errBody))
		}
		var page struct {
			Items []struct {
				Name string `json:"name"`
			} `json:"items"`
			NextPageToken string `json:"nextPageToken"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
			resp.Body.Close()
			return nil, err
		}
		resp.Body.Close()
		for _, it := range page.Items {
			items = append(items, it.Name)
		}
		if page.NextPageToken == "" {
			break
		}
		pageToken = page.NextPageToken
	}
	sort.Strings(items)
	return items, nil
}

// WriteBlob uploads binary data to GCS (and/or MountRoot).
func (g *GCSStore) WriteBlob(ctx context.Context, objectPath string, r io.Reader) (int64, error) {
	if g.BucketName == "" {
		resolved, err := SafeResolvePath(g.MountRoot, objectPath)
		if err != nil {
			return 0, err
		}
		if err := os.MkdirAll(filepath.Dir(resolved), 0755); err != nil {
			return 0, err
		}
		tmp := resolved + ".tmp." + NewRandomID("")
		f, err := os.Create(tmp)
		if err != nil {
			return 0, err
		}
		n, copyErr := io.Copy(f, r)
		closeErr := f.Close()
		if copyErr != nil {
			os.Remove(tmp)
			return 0, copyErr
		}
		if closeErr != nil {
			os.Remove(tmp)
			return 0, closeErr
		}
		if err := os.Rename(tmp, resolved); err != nil {
			os.Remove(tmp)
			return 0, err
		}
		return n, nil
	}
	cleanPath := strings.TrimPrefix(objectPath, "/")
	q := url.Values{}
	q.Set("uploadType", "media")
	q.Set("name", cleanPath)
	u := fmt.Sprintf("%s/upload/storage/v1/b/%s/o?%s", g.baseURL(), url.PathEscape(g.BucketName), q.Encode())
	payload, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(300*(1<<(attempt-1))) * time.Millisecond)
		}
		resp, err := g.doReq(ctx, http.MethodPost, u, bytes.NewReader(payload), "application/octet-stream")
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			lastErr = fmt.Errorf("GCS WriteBlob %s failed with status %d: %s", objectPath, resp.StatusCode, string(errBody))
			continue
		}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
			errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			return 0, fmt.Errorf("GCS WriteBlob %s failed with status %d: %s", objectPath, resp.StatusCode, string(errBody))
		}
		var meta struct {
			Size string `json:"size"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&meta)
		resp.Body.Close()
		sz, _ := strconv.ParseInt(meta.Size, 10, 64)
		if sz == 0 {
			sz = int64(len(payload))
		}
		return sz, nil
	}
	return 0, lastErr
}

// ReadBlob streams binary data from GCS (or MountRoot).
func (g *GCSStore) ReadBlob(ctx context.Context, objectPath string) (io.ReadCloser, bool, error) {
	if g.BucketName == "" {
		resolved, err := SafeResolvePath(g.MountRoot, objectPath)
		if err != nil {
			return nil, false, err
		}
		f, err := os.Open(resolved)
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		return f, true, nil
	}
	escapedObj := url.PathEscape(strings.TrimPrefix(objectPath, "/"))
	u := fmt.Sprintf("%s/storage/v1/b/%s/o/%s?alt=media", g.baseURL(), url.PathEscape(g.BucketName), escapedObj)
	resp, err := g.doReq(ctx, http.MethodGet, u, nil, "")
	if err != nil {
		return nil, false, err
	}
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, false, fmt.Errorf("GCS ReadBlob %s failed with status %d: %s", objectPath, resp.StatusCode, string(errBody))
	}
	return resp.Body, true, nil
}

func shouldStageBlobOnMount(mountRoot string) bool {
	if mountRoot == "" {
		return false
	}
	if os.Getenv("SBX_STAGE_TO_MOUNT") == "1" {
		return true
	}
	curBytes, err1 := os.ReadFile("/sys/fs/cgroup/memory.current")
	maxBytes, err2 := os.ReadFile("/sys/fs/cgroup/memory.max")
	if err1 == nil && err2 == nil {
		cur, e1 := strconv.ParseInt(strings.TrimSpace(string(curBytes)), 10, 64)
		lim, e2 := strconv.ParseInt(strings.TrimSpace(string(maxBytes)), 10, 64)
		if e1 == nil && e2 == nil && lim > 0 {
			if float64(cur)/float64(lim) >= 0.65 || (lim-cur) < (768<<20) {
				return true
			}
		}
	}
	return false
}

// LocalBlobPath stages a blob in /tmp (or under MountRoot/.staging when memory is elevated)
// so `/usr/local/gcp/bin/sandbox tar --file` or `--import-tar` can operate on a file
// and commits it back to GCS (and /mnt/gcs if mounted).
func (g *GCSStore) LocalBlobPath(ctx context.Context, blobPath string, forWrite bool) (string, func() error, error) {
	tmpDir := os.TempDir()
	if g.MountRoot != "" && (g.BucketName == "" || shouldStageBlobOnMount(g.MountRoot)) {
		stageDir := filepath.Join(g.MountRoot, ".staging")
		if os.MkdirAll(stageDir, 0755) == nil {
			tmpDir = stageDir
		}
	}
	tmpFile := filepath.Join(tmpDir, "sbx-blob-"+NewRandomID("")+".tar")
	if !forWrite {
		// Try reading via GCS API first (strongly consistent across hosts), falling back to MountRoot.
		rc, exists, err := g.ReadBlob(ctx, blobPath)
		if err == nil && exists {
			f, createErr := os.Create(tmpFile)
			if createErr != nil {
				rc.Close()
				return "", nil, createErr
			}
			_, copyErr := io.Copy(f, rc)
			rc.Close()
			f.Close()
			if copyErr != nil {
				os.Remove(tmpFile)
				return "", nil, copyErr
			}
			return tmpFile, func() error { return os.Remove(tmpFile) }, nil
		}
		if g.MountRoot != "" {
			if mountedPath, pathErr := SafeResolvePath(g.MountRoot, blobPath); pathErr == nil {
				if _, statErr := os.Stat(mountedPath); statErr == nil {
					return mountedPath, func() error { return nil }, nil
				}
			}
		}
		return tmpFile, func() error { return os.Remove(tmpFile) }, ErrNotFound
	}

	commit := func() error {
		defer os.Remove(tmpFile)
		f, err := os.Open(tmpFile)
		if err != nil {
			return err
		}
		defer f.Close()
		if _, err := g.WriteBlob(ctx, blobPath, f); err != nil {
			return err
		}
		return nil
	}
	return tmpFile, commit, nil
}

func (g *GCSStore) readJSONLocal(objectPath string, dest any) (int64, bool, error) {
	resolved, err := SafeResolvePath(g.MountRoot, objectPath)
	if err != nil {
		return 0, false, err
	}
	g.localMu.Lock()
	defer g.localMu.Unlock()
	b, err := os.ReadFile(resolved)
	if os.IsNotExist(err) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	var env gcsStoredEnvelope
	if err := json.Unmarshal(b, &env); err == nil && env.Gen > 0 && len(env.Data) > 0 {
		if dest != nil {
			if err := json.Unmarshal(env.Data, dest); err != nil {
				return 0, false, err
			}
		}
		return env.Gen, true, nil
	}
	if dest != nil {
		if err := json.Unmarshal(b, dest); err != nil {
			return 0, false, err
		}
	}
	return 1, true, nil
}

func (g *GCSStore) writeJSONCASLocal(objectPath string, data any, expectedGen int64) (int64, error) {
	resolved, err := SafeResolvePath(g.MountRoot, objectPath)
	if err != nil {
		return 0, err
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return 0, err
	}
	g.localMu.Lock()
	defer g.localMu.Unlock()
	if err := os.MkdirAll(filepath.Dir(resolved), 0755); err != nil {
		return 0, err
	}
	var curGen int64
	exists := false
	if b, err := os.ReadFile(resolved); err == nil {
		exists = true
		var env gcsStoredEnvelope
		if json.Unmarshal(b, &env) == nil && env.Gen > 0 {
			curGen = env.Gen
		} else {
			curGen = 1
		}
	}
	if expectedGen == 0 && exists {
		return curGen, ErrPreconditionFailed
	}
	if expectedGen > 0 && (!exists || curGen != expectedGen) {
		return curGen, ErrPreconditionFailed
	}
	newGen := curGen + 1
	if newGen <= 0 {
		newGen = 1
	}
	envBytes, err := json.MarshalIndent(gcsStoredEnvelope{Gen: newGen, Data: raw}, "", "  ")
	if err != nil {
		return 0, err
	}
	tmp := resolved + ".tmp." + NewRandomID("")
	if err := os.WriteFile(tmp, envBytes, 0644); err != nil {
		return 0, err
	}
	if err := os.Rename(tmp, resolved); err != nil {
		os.Remove(tmp)
		return 0, err
	}
	return newGen, nil
}

func (g *GCSStore) listObjectsLocal(prefix string) ([]string, error) {
	g.localMu.Lock()
	defer g.localMu.Unlock()
	if g.MountRoot == "" {
		return nil, nil
	}
	var out []string
	cleanPrefix := strings.TrimPrefix(prefix, "/")
	_ = filepath.Walk(g.MountRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(g.MountRoot, path)
		if err != nil {
			return nil
		}
		rel = filepath.ToSlash(rel)
		if strings.HasPrefix(rel, cleanPrefix) && !strings.Contains(rel, ".tmp.") {
			out = append(out, rel)
		}
		return nil
	})
	sort.Strings(out)
	return out, nil
}

// Token caching for Cloud Run metadata server and gcloud auth.
var (
	tokenCacheMu        sync.Mutex
	cliCacheDirOverride string
	accessTok           string
	accessExp           time.Time
	idTokens            = make(map[string]cachedToken)
)

// SetCLICacheDir sets the cache directory override used by token_cache.json.
func SetCLICacheDir(dir string) {
	tokenCacheMu.Lock()
	cliCacheDirOverride = dir
	tokenCacheMu.Unlock()
}

type cachedToken struct {
	Token   string    `json:"token"`
	Expires time.Time `json:"expires"`
}

type diskTokenCache struct {
	AccessToken cachedToken            `json:"access_token"`
	IDTokens    map[string]cachedToken `json:"id_tokens"`
}

func tokenCacheFilePath() string {
	dir := cliCacheDirOverride
	if dir == "" {
		dir = os.Getenv("SBX_CACHE_DIR")
	}
	if dir == "" {
		dir = os.Getenv("SBX_CONFIG_DIR")
	}
	if dir == "" {
		home, _ := os.UserHomeDir()
		if home == "" {
			home = os.TempDir()
		}
		dir = filepath.Join(home, ".cache", "sbx")
	}
	_ = os.MkdirAll(dir, 0755)
	return filepath.Join(dir, "token_cache.json")
}

func loadDiskTokenCacheLocked() diskTokenCache {
	var dtc diskTokenCache
	if b, err := os.ReadFile(tokenCacheFilePath()); err == nil {
		_ = json.Unmarshal(b, &dtc)
	}
	if dtc.IDTokens == nil {
		dtc.IDTokens = make(map[string]cachedToken)
	}
	return dtc
}

func saveDiskTokenCacheLocked(dtc diskTokenCache) {
	if b, err := json.Marshal(dtc); err == nil {
		p := tokenCacheFilePath()
		tmp := p + ".tmp." + NewRandomID("")
		if os.WriteFile(tmp, b, 0600) == nil {
			_ = os.Rename(tmp, p)
		}
	}
}

// DefaultAccessToken fetches an OAuth2 access token from the GCE Metadata Server
// (on Cloud Run) or falls back to `SBX_ACCESS_TOKEN` / `gcloud auth print-access-token`.
func DefaultAccessToken(ctx context.Context) (string, error) {
	if envTok := os.Getenv("SBX_ACCESS_TOKEN"); envTok != "" {
		return envTok, nil
	}
	now := time.Now()
	tokenCacheMu.Lock()
	if accessTok != "" && now.Before(accessExp) {
		tok := accessTok
		tokenCacheMu.Unlock()
		return tok, nil
	}
	if os.Getenv("K_SERVICE") == "" {
		dtc := loadDiskTokenCacheLocked()
		if dtc.AccessToken.Token != "" && now.Before(dtc.AccessToken.Expires) {
			accessTok = dtc.AccessToken.Token
			accessExp = dtc.AccessToken.Expires
			tokenCacheMu.Unlock()
			return accessTok, nil
		}
	}
	tokenCacheMu.Unlock()

	// When running locally (outside Cloud Run), prefer user gcloud credentials over Cloudtop VM metadata SA.
	if os.Getenv("K_SERVICE") == "" {
		cmdCtx, cmdCancel := context.WithTimeout(ctx, 10*time.Second)
		out, err := exec.CommandContext(cmdCtx, "gcloud", "auth", "print-access-token").Output()
		cmdCancel()
		if err == nil {
			if tok := strings.TrimSpace(string(out)); tok != "" {
				exp := time.Now().Add(45 * time.Minute)
				tokenCacheMu.Lock()
				accessTok = tok
				accessExp = exp
				dtc := loadDiskTokenCacheLocked()
				dtc.AccessToken = cachedToken{Token: tok, Expires: exp}
				saveDiskTokenCacheLocked(dtc)
				tokenCacheMu.Unlock()
				return tok, nil
			}
		}
	}

	// Try GCE / Cloud Run Metadata server
	reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet,
		"http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token", nil)
	if err == nil {
		req.Header.Set("Metadata-Flavor", "Google")
		if resp, err := http.DefaultClient.Do(req); err == nil {
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				var tokResp struct {
					AccessToken string `json:"access_token"`
					ExpiresIn   int    `json:"expires_in"`
				}
				if json.NewDecoder(resp.Body).Decode(&tokResp) == nil && tokResp.AccessToken != "" {
					tokenCacheMu.Lock()
					accessTok = tokResp.AccessToken
					accessExp = time.Now().Add(time.Duration(max(tokResp.ExpiresIn-60, 60)) * time.Second)
					tokenCacheMu.Unlock()
					return tokResp.AccessToken, nil
				}
			}
		}
	}

	// Fallback to gcloud auth print-access-token
	cmdCtx, cmdCancel := context.WithTimeout(ctx, 10*time.Second)
	defer cmdCancel()
	out, err := exec.CommandContext(cmdCtx, "gcloud", "auth", "print-access-token").Output()
	if err != nil {
		return "", err
	}
	tok := strings.TrimSpace(string(out))
	if tok != "" {
		exp := time.Now().Add(45 * time.Minute)
		tokenCacheMu.Lock()
		accessTok = tok
		accessExp = exp
		if os.Getenv("K_SERVICE") == "" {
			dtc := loadDiskTokenCacheLocked()
			dtc.AccessToken = cachedToken{Token: tok, Expires: exp}
			saveDiskTokenCacheLocked(dtc)
		}
		tokenCacheMu.Unlock()
	}
	return tok, nil
}

// GetIDToken fetches an OIDC Identity Token for Cloud Run IAM service-to-service
// or CLI-to-service authentication, caching it in memory and on disk (`token_cache.json`).
func GetIDToken(ctx context.Context, audience string) (string, error) {
	if envTok := os.Getenv("SBX_ID_TOKEN"); envTok != "" {
		return envTok, nil
	}
	if parsed, err := url.Parse(audience); err == nil && parsed.Scheme != "" && parsed.Host != "" {
		audience = parsed.Scheme + "://" + parsed.Host
	}
	now := time.Now()
	tokenCacheMu.Lock()
	if ct, ok := idTokens[audience]; ok && now.Before(ct.Expires) {
		tokenCacheMu.Unlock()
		return ct.Token, nil
	}
	if os.Getenv("K_SERVICE") == "" {
		dtc := loadDiskTokenCacheLocked()
		if ct, ok := dtc.IDTokens[audience]; ok && ct.Token != "" && now.Before(ct.Expires) {
			idTokens[audience] = ct
			tokenCacheMu.Unlock()
			return ct.Token, nil
		}
		if ct, ok := dtc.IDTokens["__default__"]; ok && ct.Token != "" && now.Before(ct.Expires) {
			idTokens[audience] = ct
			tokenCacheMu.Unlock()
			return ct.Token, nil
		}
	}
	tokenCacheMu.Unlock()

	// When running locally (outside Cloud Run), prefer user gcloud credentials over Cloudtop VM metadata SA.
	if os.Getenv("K_SERVICE") == "" {
		cmdCtx, cmdCancel := context.WithTimeout(ctx, 10*time.Second)
		out, err := exec.CommandContext(cmdCtx, "gcloud", "auth", "print-identity-token").Output()
		cmdCancel()
		if err == nil {
			if tok := strings.TrimSpace(string(out)); tok != "" {
				exp := time.Now().Add(45 * time.Minute)
				ct := cachedToken{Token: tok, Expires: exp}
				tokenCacheMu.Lock()
				idTokens[audience] = ct
				idTokens["__default__"] = ct
				dtc := loadDiskTokenCacheLocked()
				dtc.IDTokens[audience] = ct
				dtc.IDTokens["__default__"] = ct
				saveDiskTokenCacheLocked(dtc)
				tokenCacheMu.Unlock()
				return tok, nil
			}
		}
	}

	// Try GCE / Cloud Run Metadata server identity endpoint
	if audience != "" {
		reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		u := "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity?audience=" + url.QueryEscape(audience)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, u, nil)
		if err == nil {
			req.Header.Set("Metadata-Flavor", "Google")
			if resp, err := http.DefaultClient.Do(req); err == nil {
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				tok := strings.TrimSpace(string(b))
				if resp.StatusCode == http.StatusOK && tok != "" {
					tokenCacheMu.Lock()
					idTokens[audience] = cachedToken{Token: tok, Expires: time.Now().Add(45 * time.Minute)}
					tokenCacheMu.Unlock()
					return tok, nil
				}
			}
		}
	}

	// Fallback to gcloud auth print-identity-token
	cmdCtx, cmdCancel := context.WithTimeout(ctx, 10*time.Second)
	defer cmdCancel()
	out, err := exec.CommandContext(cmdCtx, "gcloud", "auth", "print-identity-token").Output()
	if err != nil {
		return "", err
	}
	tok := strings.TrimSpace(string(out))
	if tok != "" {
		exp := time.Now().Add(45 * time.Minute)
		ct := cachedToken{Token: tok, Expires: exp}
		tokenCacheMu.Lock()
		idTokens[audience] = ct
		if os.Getenv("K_SERVICE") == "" {
			dtc := loadDiskTokenCacheLocked()
			dtc.IDTokens[audience] = ct
			dtc.IDTokens["__default__"] = ct
			saveDiskTokenCacheLocked(dtc)
		}
		tokenCacheMu.Unlock()
	}
	return tok, nil
}

// ---------------------------------------------------------------------------------------------
// Preview-URL proxy header hygiene (sandbox-ui -> sandbox-control-plane -> sandbox-pool -> sandbox)
// ---------------------------------------------------------------------------------------------

// proxyStrippedHeaders are hop/identity headers that must never cross a Cloud Run service
// boundary or reach the sandbox app.
var proxyStrippedHeaders = map[string]bool{
	"Host":              true,
	"X-Forwarded-Host":  true,
	"X-Forwarded-For":   true,
	"X-Forwarded-Proto": true,
	"Forwarded":         true,
}

// isProxyStrippedHeader reports whether header key (any case) must be dropped when forwarding a
// preview/proxy request to the next hop:
//   - X-Serverless-* (Cloud Run / GFE evaluates X-Serverless-Authorization BEFORE Authorization, so an
//     IAP token minted for sandbox-ui makes sandbox-control-plane reject the hop with 401)
//   - X-Goog-Iap-* and X-Goog-Authenticated-User-* (IAP identity assertions)
//   - Authorization / Proxy-Authorization when stripAuth (each hop sets its own ID token)
//   - Host / X-Forwarded-* / Forwarded
func isProxyStrippedHeader(key string, stripAuth bool) bool {
	canon := http.CanonicalHeaderKey(key)
	lower := strings.ToLower(canon)
	switch {
	case strings.HasPrefix(lower, "x-serverless-"),
		strings.HasPrefix(lower, "x-goog-iap-"),
		strings.HasPrefix(lower, "x-goog-authenticated-user-"):
		return true
	case stripAuth && (canon == "Authorization" || canon == "Proxy-Authorization"):
		return true
	}
	return proxyStrippedHeaders[canon]
}

// isProxyStrippedCookie reports whether a cookie belongs to IAP or the sandbox-ui session (and must
// not be forwarded to the sandbox app).
func isProxyStrippedCookie(name string) bool {
	return strings.HasPrefix(name, "GCP_IAAP_") || strings.HasPrefix(name, "GCP_IAP_") ||
		name == "__Host-sbx_csrf" || name == "sbx_csrf"
}

// filterProxyCookieHeader removes IAP and sandbox-ui cookies from a Cookie header value while
// preserving application cookies meant for the previewed sandbox web app.
func filterProxyCookieHeader(v string) string {
	var kept []string
	for _, part := range strings.Split(v, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name := part
		if i := strings.IndexByte(part, '='); i >= 0 {
			name = strings.TrimSpace(part[:i])
		}
		if isProxyStrippedCookie(name) {
			continue
		}
		kept = append(kept, part)
	}
	return strings.Join(kept, "; ")
}

// sanitizeProxyForwardHeaders copies src into dst, dropping identity/hop headers and IAP/UI cookies
// (see isProxyStrippedHeader). The caller then sets the hop's own Authorization and
// X-Serverless-Session-Id.
func sanitizeProxyForwardHeaders(dst, src http.Header, stripAuth bool) {
	for k, vals := range src {
		if isProxyStrippedHeader(k, stripAuth) {
			continue
		}
		if http.CanonicalHeaderKey(k) == "Cookie" {
			for _, v := range vals {
				if fv := filterProxyCookieHeader(v); fv != "" {
					dst.Add("Cookie", fv)
				}
			}
			continue
		}
		for _, v := range vals {
			dst.Add(k, v)
		}
	}
}

// sanitizeProxyHeadersInPlace applies sanitizeProxyForwardHeaders to h itself (for
// httputil.ReverseProxy Rewrite hooks operating on pr.Out.Header).
func sanitizeProxyHeadersInPlace(h http.Header, stripAuth bool) {
	clean := make(http.Header, len(h))
	sanitizeProxyForwardHeaders(clean, h, stripAuth)
	for k := range h {
		delete(h, k)
	}
	for k, v := range clean {
		h[k] = v
	}
}

// rewriteProxyLocation keeps redirects issued by a previewed app under its proxy prefix: a
// root-relative `Location: /login` becomes `/proxy/<sandbox>/<port>/login`.
func rewriteProxyLocation(h http.Header, sandboxIDOrName string, port int) {
	loc := h.Get("Location")
	if loc == "" || !strings.HasPrefix(loc, "/") || strings.HasPrefix(loc, "//") || strings.HasPrefix(loc, "/proxy/") {
		return
	}
	h.Set("Location", fmt.Sprintf("/proxy/%s/%d%s", sandboxIDOrName, port, loc))
}
