package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ControlPlaneServer implements `sandbox-control-plane` (`controld`):
//   - Horizontally scalable in-memory fleet cache (`hosts`, `sandboxes`, `events`, `config`)
//   - Candidate Host Dispenser (`GET /v1/candidates`)
//   - Migration Target Broker (`POST /v1/migration-targets`)
//   - Single-leader Fleet Optimizer (Bin-Packing Defragmenter & Dead-Host Watchdog)
type ControlPlaneServer struct {
	mu             sync.RWMutex
	instanceID     string
	isLeader       bool
	config         FleetConfig
	securityPolicy SecurityPolicy
	hosts          map[string]HostRecord
	sandboxes      map[string]SandboxRecord
	reservedSlot   map[string]int // host_id -> temporarily reserved migration slots
	recentEvents   []ActivityEvent
	startedAt      time.Time
	store          Store
	httpClient     *http.Client
	stopCh         chan struct{}
	stopOnce       sync.Once
}

var (
	runtimeSAOnce    sync.Once
	cachedRuntimeSA  string
	cachedIAPAgentSA string
)

func detectRuntimeServiceAccounts() (string, string) {
	runtimeSAOnce.Do(func() {
		projNum := strings.TrimSpace(os.Getenv("GOOGLE_CLOUD_PROJECT_NUMBER"))
		if projNum == "" {
			projNum = "000000000000"
		}
		saEmail := strings.TrimSpace(os.Getenv("BACKEND_SERVICE_ACCOUNT"))
		if saEmail == "" && os.Getenv("K_SERVICE") != "" {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email", nil)
			if err == nil {
				req.Header.Set("Metadata-Flavor", "Google")
				if resp, rErr := http.DefaultClient.Do(req); rErr == nil {
					b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
					resp.Body.Close()
					if resp.StatusCode == http.StatusOK && strings.Contains(string(b), "@") {
						saEmail = strings.TrimSpace(string(b))
					}
				}
			}
		}
		if saEmail == "" {
			saEmail = projNum + "-compute@developer.gserviceaccount.com"
		}
		cachedRuntimeSA = NormalizeIAMMember("serviceAccount:" + strings.TrimPrefix(saEmail, "serviceAccount:"))
		cachedIAPAgentSA = NormalizeIAMMember("serviceAccount:service-" + projNum + "@gcp-sa-iap.iam.gserviceaccount.com")
	})
	return cachedRuntimeSA, cachedIAPAgentSA
}

func defaultSecurityPolicy() SecurityPolicy {
	dep := ExtractCallerEmail(nil, "admin@example.com")
	depMember := NormalizeIAMMember(dep)
	runtimeSA, iapSA := detectRuntimeServiceAccounts()
	return SecurityPolicy{
		IAPEnabled:           true,
		DeployerEmail:        dep,
		UIAccessMembers:      dedupeMembers([]string{depMember}),
		BackendAccessMembers: dedupeMembers([]string{depMember, runtimeSA}),
		ProtectedMembers:     dedupeMembers([]string{depMember, runtimeSA, iapSA}),
		UpdatedAt:            time.Now().UTC(),
		UpdatedBy:            dep,
	}
}

// NewControlPlaneServer creates a new ControlPlaneServer backed by Store.
func NewControlPlaneServer(store Store, initialCfg FleetConfig) *ControlPlaneServer {
	if initialCfg.MaxDensity <= 0 {
		initialCfg = DefaultFleetConfig()
	}
	if envPool := os.Getenv("POOL_SERVICE_URL"); envPool != "" && initialCfg.PoolServiceURL == "" {
		initialCfg.PoolServiceURL = strings.TrimRight(envPool, "/")
	}
	if envCP := os.Getenv("CONTROL_PLANE_URL"); envCP != "" && initialCfg.ControlPlaneURL == "" {
		initialCfg.ControlPlaneURL = strings.TrimRight(envCP, "/")
	}
	normalizeFleetConfig(&initialCfg)
	cp := &ControlPlaneServer{
		instanceID:     NewRandomID("cp"),
		config:         initialCfg,
		securityPolicy: defaultSecurityPolicy(),
		hosts:          make(map[string]HostRecord),
		sandboxes:      make(map[string]SandboxRecord),
		reservedSlot:   make(map[string]int),
		startedAt:      time.Now().UTC(),
		store:          store,
		httpClient:     &http.Client{Timeout: 30 * time.Second},
		stopCh:         make(chan struct{}),
	}
	if store != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var storedCfg FleetConfig
		if _, exists, err := store.ReadJSON(ctx, "fleet/config.json", &storedCfg); err == nil && exists {
			if initialCfg.PoolServiceURL != "" && storedCfg.PoolServiceURL == "" {
				storedCfg.PoolServiceURL = initialCfg.PoolServiceURL
			}
			if initialCfg.ControlPlaneURL != "" && storedCfg.ControlPlaneURL == "" {
				storedCfg.ControlPlaneURL = initialCfg.ControlPlaneURL
			}
			cp.config = storedCfg
		} else {
			_, _ = store.WriteJSONCAS(ctx, "fleet/config.json", cp.config, -1)
		}
		var storedSec SecurityPolicy
		if _, exists, err := store.ReadJSON(ctx, "fleet/security-policy.json", &storedSec); err == nil && exists && len(storedSec.UIAccessMembers) > 0 {
			cp.securityPolicy = storedSec
		} else {
			_, _ = store.WriteJSONCAS(ctx, "fleet/security-policy.json", cp.securityPolicy, -1)
		}
		_ = cp.SyncFromStore(ctx)
	}
	return cp
}

// StartBackgroundLoops starts the 3-second horizontal cache sync and leader election/optimizer loop.
func (c *ControlPlaneServer) StartBackgroundLoops(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-c.stopCh:
				return
			case <-ticker.C:
				_ = c.RunLeaderAndSyncTick(ctx)
			}
		}
	}()
}

// Close stops background loops on the ControlPlaneServer.
func (c *ControlPlaneServer) Close() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
	})
}

func (c *ControlPlaneServer) recordEventLocked(evt ActivityEvent) {
	if evt.EventID == "" {
		evt.EventID = NewRandomID("evt")
	}
	if evt.Timestamp.IsZero() {
		evt.Timestamp = time.Now().UTC()
	}
	for _, existing := range c.recentEvents {
		if existing.EventID == evt.EventID {
			return
		}
	}
	c.recentEvents = append(c.recentEvents, evt)
	if len(c.recentEvents) > 100 {
		c.recentEvents = c.recentEvents[len(c.recentEvents)-100:]
	}
}

func isReservedPrefixHost(hostID string) bool {
	for _, p := range []string{"host-mig-", "host-chaos-", "host-bp-", "host-dx-", "host-dxt-", "host-endurance-"} {
		if strings.HasPrefix(hostID, p) {
			return true
		}
	}
	return false
}

func isDedicatedOrPinnedHost(hostID string, sandboxes map[string]SandboxRecord) bool {
	if isReservedPrefixHost(hostID) {
		return true
	}
	for _, s := range sandboxes {
		if s.PinnedHost {
			return true
		}
	}
	return false
}

func normalizeFleetConfig(cfg *FleetConfig) {
	if cfg.MaxDensity <= 0 {
		cfg.MaxDensity = 10
	}
	if cfg.HostCPU == "" {
		cfg.HostCPU = "4"
	}
	if cfg.HostMemory == "" {
		cfg.HostMemory = "8Gi"
	}
	if cfg.MinClientVersion == "" {
		cfg.MinClientVersion = DefaultMinClientVersion
	}
	if cfg.MinFreeMemoryMB <= 0 {
		cfg.MinFreeMemoryMB = 512
	}
	if cfg.MinFreeMemoryPct <= 0 {
		cfg.MinFreeMemoryPct = 15.0
	}
	if cfg.MaxMemoryOvercommitRatio <= 0 {
		cfg.MaxMemoryOvercommitRatio = 2.0
	}
	if cfg.HotMemoryThresholdPct <= 0 {
		cfg.HotMemoryThresholdPct = 85.0
	}
	if cfg.HotCPUThresholdPct <= 0 {
		cfg.HotCPUThresholdPct = 90.0
	}
	cfg.ServerVersion = DispenserVersion
}

// SyncFromStore refreshes the control plane's in-memory fleet view from `hosts/*.json`,
// `sandboxes/*.json`, and `fleet/config.json` using bounded parallel reads.
func (c *ControlPlaneServer) SyncFromStore(ctx context.Context) error {
	if c.store == nil {
		return nil
	}
	var cfg FleetConfig
	if _, ok, err := c.store.ReadJSON(ctx, "fleet/config.json", &cfg); err == nil && ok && cfg.MaxDensity > 0 {
		c.mu.Lock()
		if cfg.PoolServiceURL == "" && c.config.PoolServiceURL != "" {
			cfg.PoolServiceURL = c.config.PoolServiceURL
		}
		if cfg.ControlPlaneURL == "" {
			if c.config.ControlPlaneURL != "" {
				cfg.ControlPlaneURL = c.config.ControlPlaneURL
			} else if envCP := os.Getenv("CONTROL_PLANE_URL"); envCP != "" {
				cfg.ControlPlaneURL = strings.TrimRight(envCP, "/")
			}
		}
		normalizeFleetConfig(&cfg)
		c.config = cfg
		c.mu.Unlock()
	}
	var secPol SecurityPolicy
	if _, ok, err := c.store.ReadJSON(ctx, "fleet/security-policy.json", &secPol); err == nil && ok && len(secPol.UIAccessMembers) > 0 {
		c.mu.Lock()
		c.securityPolicy = secPol
		c.mu.Unlock()
	}

	sem := make(chan struct{}, 16)

	hostKeys, err := c.store.ListObjects(ctx, "hosts/")
	if err == nil {
		seenHosts := make(map[string]bool, len(hostKeys))
		var seenMu sync.Mutex
		var wg sync.WaitGroup
		for _, k := range hostKeys {
			if !strings.HasSuffix(k, ".json") {
				continue
			}
			key := k
			wg.Add(1)
			go func() {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				var hr HostRecord
				if _, ok, rErr := c.store.ReadJSON(ctx, key, &hr); rErr == nil && ok && hr.HostID != "" {
					seenMu.Lock()
					seenHosts[hr.HostID] = true
					seenMu.Unlock()
					c.mu.Lock()
					cur, exists := c.hosts[hr.HostID]
					if !exists || !hr.LastHeartbeatAt.Before(cur.LastHeartbeatAt) || hr.Status == HostDead || hr.Status == HostCrashed {
						c.hosts[hr.HostID] = hr
					}
					for _, evt := range hr.RecentEvents {
						c.recordEventLocked(evt)
					}
					c.mu.Unlock()
				}
			}()
		}
		wg.Wait()
		c.mu.Lock()
		for hid := range c.hosts {
			if !seenHosts[hid] {
				delete(c.hosts, hid)
			}
		}
		c.mu.Unlock()
	}

	sbxKeys, err := c.store.ListObjects(ctx, "sandboxes/")
	if err == nil {
		seen := make(map[string]bool, len(sbxKeys))
		var seenMu sync.Mutex
		var wg sync.WaitGroup
		for _, k := range sbxKeys {
			if !strings.HasSuffix(k, ".json") {
				continue
			}
			key := k
			wg.Add(1)
			go func() {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				var sr SandboxRecord
				if _, ok, rErr := c.store.ReadJSON(ctx, key, &sr); rErr == nil && ok && sr.SandboxID != "" && sr.Status != StatusDeleted {
					seenMu.Lock()
					seen[sr.SandboxID] = true
					seenMu.Unlock()
					c.mu.Lock()
					cur, exists := c.sandboxes[sr.SandboxID]
					if !exists || sr.Epoch >= cur.Epoch || !sr.UpdatedAt.Before(cur.UpdatedAt) {
						c.sandboxes[sr.SandboxID] = sr
					}
					c.mu.Unlock()
				}
			}()
		}
		wg.Wait()
		c.mu.Lock()
		for id := range c.sandboxes {
			if !seen[id] {
				delete(c.sandboxes, id)
			}
		}
		// Reconcile each host's active sandboxes from authoritative sandbox records
		c.reconcileHostSandboxMapsLocked()
		var staleEmptyHosts []string
		for hid, hr := range c.hosts {
			if hr.ActiveSandboxes == 0 && (hr.Status == HostDead || hr.Status == HostDraining || (!hr.StartedAt.IsZero() && time.Since(hr.StartedAt) > 15*time.Second) || (c.config.TargetRevision != "" && hr.Revision != "" && hr.Revision != c.config.TargetRevision)) {
				delete(c.hosts, hid)
				staleEmptyHosts = append(staleEmptyHosts, hid)
			}
		}
		c.mu.Unlock()
		for _, hid := range staleEmptyHosts {
			_ = c.store.DeleteObject(ctx, "hosts/"+hid+".json")
		}
	}
	return nil
}

func (c *ControlPlaneServer) reconcileHostSandboxMapsLocked() {
	byHost := make(map[string]map[string]SandboxRecord)
	for _, s := range c.sandboxes {
		if s.Status == StatusRunning && s.HostID != "" {
			if byHost[s.HostID] == nil {
				byHost[s.HostID] = make(map[string]SandboxRecord)
			}
			byHost[s.HostID][s.SandboxID] = s
		}
	}
	for hid, hr := range c.hosts {
		if hr.Status == HostDead || hr.Status == HostCrashed {
			hr.ActiveSandboxes = 0
			hr.FreeSlots = 0
			hr.Sandboxes = map[string]SandboxRecord{}
			c.hosts[hid] = hr
			continue
		}
		sbxMap := byHost[hid]
		if sbxMap == nil {
			sbxMap = make(map[string]SandboxRecord)
		}
		hr.Sandboxes = sbxMap
		hr.ActiveSandboxes = len(sbxMap)
		maxD := hr.MaxDensity
		if maxD <= 0 {
			maxD = c.config.MaxDensity
		}
		free := maxD - hr.ActiveSandboxes - c.reservedSlot[hid]
		if free < 0 || hr.Status != HostActive || hr.OverloadState == "ADMISSION_BLOCKED" || hr.OverloadState == "HOT_SHEDDING" {
			free = 0
		}
		hr.FreeSlots = free
		c.hosts[hid] = hr
	}
}

// RunLeaderAndSyncTick attempts to acquire/renew the GCS leader lease (`fleet/leader-lock.json`),
// syncs the store, runs dead-host crash recovery and bin-packing if leader, and writes `fleet/summary.json`.
func (c *ControlPlaneServer) RunLeaderAndSyncTick(ctx context.Context) error {
	_ = c.SyncFromStore(ctx)
	c.mu.Lock()
	// Clear temporary slot reservations every tick after actual sandbox counts have synced
	c.reservedSlot = make(map[string]int)
	c.reconcileHostSandboxMapsLocked()
	c.mu.Unlock()

	if c.tryAcquireLeaderLease(ctx) {
		_ = c.runWatchdogPass(ctx)
		_ = c.runRolloutAndBinPackPass(ctx)
		if c.store != nil {
			summary := c.BuildFleetSummary()
			_, _ = c.store.WriteJSONCAS(ctx, "fleet/summary.json", summary, -1)
		}
	}
	return nil
}

func (c *ControlPlaneServer) tryAcquireLeaderLease(ctx context.Context) bool {
	if c.store == nil {
		c.mu.Lock()
		c.isLeader = true
		c.mu.Unlock()
		return true
	}
	now := time.Now().UTC()
	var lock LeaderLock
	gen, exists, err := c.store.ReadJSON(ctx, "fleet/leader-lock.json", &lock)
	if err != nil {
		return false
	}
	if !exists || lock.LeaderID == c.instanceID || now.After(lock.ExpiresAt) {
		newLock := LeaderLock{
			LeaderID:   c.instanceID,
			AcquiredAt: now,
			ExpiresAt:  now.Add(15 * time.Second),
		}
		expectedGen := gen
		if !exists {
			expectedGen = 0
		}
		if _, wErr := c.store.WriteJSONCAS(ctx, "fleet/leader-lock.json", newLock, expectedGen); wErr == nil {
			c.mu.Lock()
			c.isLeader = true
			c.mu.Unlock()
			return true
		}
	}
	c.mu.Lock()
	c.isLeader = (lock.LeaderID == c.instanceID)
	c.mu.Unlock()
	return c.isLeader
}

// runWatchdogPass detects dead or crashed hosts and resurrects their RUNNING sandboxes onto healthy hosts,
// and garbage-collects empty dead/stale host records so hosts/*.json never accumulates orphans.
func (c *ControlPlaneServer) runWatchdogPass(ctx context.Context) error {
	c.mu.RLock()
	timeoutSec := c.config.HeartbeatTimeoutSec
	if timeoutSec <= 0 {
		timeoutSec = 15
	}
	var suspectHosts []HostRecord
	var gcHosts []string
	for _, hr := range c.hosts {
		expired := time.Since(hr.LastHeartbeatAt) > time.Duration(timeoutSec)*time.Second
		if hr.ActiveSandboxes == 0 {
			if hr.Status == HostDead || hr.Status == HostDraining || expired || (!hr.StartedAt.IsZero() && time.Since(hr.StartedAt) > 15*time.Second) {
				gcHosts = append(gcHosts, hr.HostID)
			}
			continue
		}
		if hr.Status == HostDead || hr.Status == HostCrashed || expired {
			suspectHosts = append(suspectHosts, hr)
		}
	}
	c.mu.RUnlock()

	for _, hid := range gcHosts {
		c.mu.Lock()
		delete(c.hosts, hid)
		c.mu.Unlock()
		if c.store != nil {
			_ = c.store.DeleteObject(ctx, "hosts/"+hid+".json")
		}
	}

	// IMPORTANT: Never send an HTTP ping with X-Serverless-Session-Id to sandbox-pool from the watchdog,
	// because on Cloud Run Session Isolation an HTTP request with X-Serverless-Session-Id cold-starts
	// a brand-new clone for that session ID!
	for _, hr := range suspectHosts {
		_ = c.resurrectDeadHost(ctx, hr.HostID)
	}
	return nil
}

func (c *ControlPlaneServer) resurrectDeadHost(ctx context.Context, deadHostID string) error {
	c.mu.Lock()
	hr, ok := c.hosts[deadHostID]
	if !ok {
		hr = HostRecord{HostID: deadHostID}
	}
	hr.Status = HostDead
	hr.ActiveSandboxes = 0
	hr.FreeSlots = 0
	c.hosts[deadHostID] = hr

	var orphans []SandboxRecord
	var orphanIDs []string
	for _, s := range c.sandboxes {
		if s.HostID == deadHostID && (s.Status == StatusRunning || s.Status == StatusCreating || s.Status == StatusMigrating) {
			orphans = append(orphans, s)
			orphanIDs = append(orphanIDs, s.SandboxID)
		}
	}
	c.mu.Unlock()

	if len(orphans) == 0 {
		c.mu.Lock()
		delete(c.hosts, deadHostID)
		c.mu.Unlock()
		if c.store != nil {
			_ = c.store.DeleteObject(ctx, "hosts/"+deadHostID+".json")
		}
		return nil
	}

	if c.store != nil {
		_, _ = c.store.WriteJSONCAS(ctx, "hosts/"+deadHostID+".json", hr, -1)
	}

	targets := c.allocateMigrationTargets(deadHostID, orphanIDs)
	allMigrated := true
	for _, sbx := range orphans {
		targetHost := targets[sbx.SandboxID]
		if targetHost == "" || targetHost == deadHostID {
			targetHost = NewRandomID("host")
		}
		hydrated, err := c.sendHydrateCommand(ctx, targetHost, HydrateRequest{
			Record:     sbx,
			OverlayTar: sbx.OverlayPath,
			Reason:     "crash_recovery",
		})
		if err != nil {
			// Try minting a fresh host session if warm target failed
			fallbackHost := NewRandomID("host")
			hydrated, err = c.sendHydrateCommand(ctx, fallbackHost, HydrateRequest{
				Record:     sbx,
				OverlayTar: sbx.OverlayPath,
				Reason:     "crash_recovery",
			})
			if err == nil {
				targetHost = fallbackHost
			}
		}
		if err == nil {
			c.mu.Lock()
			c.sandboxes[hydrated.SandboxID] = hydrated
			c.recordEventLocked(ActivityEvent{
				Type:      "CRASH_RECOVERED",
				SandboxID: hydrated.SandboxID,
				HostID:    deadHostID,
				TargetID:  targetHost,
				Message:   fmt.Sprintf("Watchdog resurrected %s from dead host %s -> %s (epoch=%d)", hydrated.SandboxID, deadHostID, targetHost, hydrated.Epoch),
			})
			c.reconcileHostSandboxMapsLocked()
			c.mu.Unlock()
		} else {
			allMigrated = false
		}
	}
	if allMigrated {
		c.mu.Lock()
		delete(c.hosts, deadHostID)
		c.mu.Unlock()
		if c.store != nil {
			_ = c.store.DeleteObject(ctx, "hosts/"+deadHostID+".json")
		}
	}
	return nil
}

func (c *ControlPlaneServer) runRolloutAndBinPackPass(ctx context.Context) error {
	c.mu.RLock()
	cfg := c.config
	var outdatedHosts []string
	if cfg.TargetRevision != "" {
		for _, hr := range c.hosts {
			if hr.Status == HostActive && hr.Revision != "" && hr.Revision != cfg.TargetRevision && hr.ActiveSandboxes > 0 {
				outdatedHosts = append(outdatedHosts, hr.HostID)
			}
		}
	}
	autoBinPack := cfg.AutoBinPack
	c.mu.RUnlock()

	for _, hid := range outdatedHosts {
		_, _ = c.TriggerHostDrain(ctx, hid, "revision_rollout")
	}
	if autoBinPack {
		_, _ = c.CompactFleet(ctx, false)
	}
	return nil
}

// CompactFleet performs a multi-pass bin-packing defragmentation and empty-host scale-down pass.
// First, it prunes all empty (0-sandbox) hosts so idle clones do not linger in the fleet.
// Second, it iteratively drains sparse hosts (least-loaded first) into fuller warm hosts until
// the fleet converges toward `ceil(totalRunning / maxDensity)`.
func (c *ControlPlaneServer) CompactFleet(ctx context.Context, force bool) (map[string]any, error) {
	_ = c.SyncFromStore(ctx)

	c.mu.Lock()
	c.reconcileHostSandboxMapsLocked()
	var retiredEmptyHosts []string
	for hid, hr := range c.hosts {
		if hr.ActiveSandboxes == 0 && (force || hr.Status == HostDead || hr.Status == HostDraining || (!hr.StartedAt.IsZero() && time.Since(hr.StartedAt) > 15*time.Second)) {
			delete(c.hosts, hid)
			retiredEmptyHosts = append(retiredEmptyHosts, hid)
		}
	}
	if force {
		// On explicit manual compaction, unpin sandboxes so fragmented pinned/reserved hosts can consolidate
		for id, s := range c.sandboxes {
			if s.Status == StatusRunning && s.PinnedHost {
				s.PinnedHost = false
				c.sandboxes[id] = s
			}
		}
		c.reconcileHostSandboxMapsLocked()
	}
	c.mu.Unlock()

	sort.Strings(retiredEmptyHosts)
	if c.store != nil {
		for _, hid := range retiredEmptyHosts {
			_ = c.store.DeleteObject(ctx, "hosts/"+hid+".json")
		}
	}

	var drainedHosts []string
	allMigrated := make(map[string]string)
	var firstErr error
	maxPasses := 5
	if force {
		maxPasses = 32
	}

	var lastMessage string
	minHostsNeeded := 1

	for pass := 0; pass < maxPasses; pass++ {
		c.mu.Lock()
		c.reconcileHostSandboxMapsLocked()
		maxDensity := c.config.MaxDensity
		if maxDensity <= 0 {
			maxDensity = 10
		}
		hysteresis := c.config.BinPackHysteresis
		if force {
			hysteresis = 0
		}
		var activeHosts []HostRecord
		totalRunning := 0
		for _, hr := range c.hosts {
			if hr.Status == HostActive && hr.ActiveSandboxes > 0 {
				if !force {
					if isDedicatedOrPinnedHost(hr.HostID, hr.Sandboxes) && time.Since(hr.StartedAt) < 120*time.Second {
						continue
					}
					if time.Since(hr.StartedAt) < 25*time.Second || time.Since(hr.LastHeartbeatAt) > 15*time.Second {
						continue
					}
				}
				if c.config.TargetRevision == "" || hr.Revision == c.config.TargetRevision || force {
					activeHosts = append(activeHosts, hr)
					totalRunning += hr.ActiveSandboxes
				}
			}
		}
		c.mu.Unlock()

		minHostsNeeded = int(math.Ceil(float64(totalRunning) / float64(maxDensity)))
		if minHostsNeeded < 1 && totalRunning > 0 {
			minHostsNeeded = 1
		}
		if len(activeHosts) <= 1 {
			if lastMessage == "" {
				lastMessage = "Fleet already compact (<= 1 active host with sandboxes)"
			}
			break
		}
		if len(activeHosts) <= minHostsNeeded {
			if lastMessage == "" {
				lastMessage = fmt.Sprintf("Fleet is at optimal host count (%d hosts for %d sandboxes, max_density=%d)", len(activeHosts), totalRunning, maxDensity)
			}
			break
		}

		// Sort active hosts ascending by ActiveSandboxes (least-loaded first)
		sort.Slice(activeHosts, func(i, j int) bool {
			if activeHosts[i].ActiveSandboxes != activeHosts[j].ActiveSandboxes {
				return activeHosts[i].ActiveSandboxes < activeHosts[j].ActiveSandboxes
			}
			return activeHosts[i].HostID < activeHosts[j].HostID
		})

		sparseHost := activeHosts[0]
		// Count free slots on all OTHER active hosts
		otherFreeSlots := 0
		for _, hr := range activeHosts[1:] {
			free := maxDensity - hr.ActiveSandboxes
			if free > 0 {
				otherFreeSlots += free
			}
		}
		if otherFreeSlots < sparseHost.ActiveSandboxes+hysteresis {
			if lastMessage == "" {
				lastMessage = fmt.Sprintf("Insufficient warm free slots (%d) to absorb %s (%d sandboxes, hysteresis=%d)", otherFreeSlots, sparseHost.HostID, sparseHost.ActiveSandboxes, hysteresis)
			}
			break
		}

		migrated, err := c.TriggerHostDrain(ctx, sparseHost.HostID, "bin_pack_compact")
		for k, v := range migrated {
			allMigrated[k] = v
		}
		if len(migrated) > 0 || err == nil {
			drainedHosts = append(drainedHosts, sparseHost.HostID)
			c.mu.Lock()
			c.recordEventLocked(ActivityEvent{
				Type:    "COMPACT",
				HostID:  sparseHost.HostID,
				Message: fmt.Sprintf("Bin-pack compacted %d sandboxes from %s into fuller warm hosts", len(migrated), sparseHost.HostID),
			})
			c.mu.Unlock()
		}
		if err != nil {
			firstErr = err
			break
		}
		if len(migrated) == 0 {
			break
		}
	}

	c.mu.RLock()
	remainingActiveHosts := 0
	for _, hr := range c.hosts {
		if hr.Status == HostActive && hr.ActiveSandboxes > 0 {
			remainingActiveHosts++
		}
	}
	c.mu.RUnlock()

	if len(drainedHosts) > 0 || len(retiredEmptyHosts) > 0 {
		lastMessage = fmt.Sprintf("Drained %d sparse host(s) (%d sandboxes migrated), retired %d empty host(s); %d active host(s) remaining (optimal=%d)",
			len(drainedHosts), len(allMigrated), len(retiredEmptyHosts), remainingActiveHosts, minHostsNeeded)
	}
	if drainedHosts == nil {
		drainedHosts = []string{}
	}
	if retiredEmptyHosts == nil {
		retiredEmptyHosts = []string{}
	}
	return map[string]any{
		"drained_hosts":          drainedHosts,
		"retired_empty_hosts":    retiredEmptyHosts,
		"migrated_sandboxes":     allMigrated,
		"optimal_hosts_needed":   minHostsNeeded,
		"active_hosts_remaining": remainingActiveHosts,
		"message":                lastMessage,
	}, firstErr
}

// TriggerHostDrain instructs `hostID` to drain and migrate its sandboxes to warm target hosts.
func (c *ControlPlaneServer) TriggerHostDrain(ctx context.Context, hostID string, reason string) (map[string]string, error) {
	c.mu.Lock()
	hr, ok := c.hosts[hostID]
	var sbxIDs []string
	var sbxRecords []SandboxRecord
	for _, s := range c.sandboxes {
		if s.HostID == hostID && s.Status == StatusRunning {
			sbxIDs = append(sbxIDs, s.SandboxID)
			sbxRecords = append(sbxRecords, s)
		}
	}
	if ok {
		hr.Status = HostDraining
		hr.FreeSlots = 0
		c.hosts[hostID] = hr
	}
	poolURL := c.config.PoolServiceURL
	c.mu.Unlock()

	if len(sbxIDs) == 0 {
		c.mu.Lock()
		delete(c.hosts, hostID)
		c.mu.Unlock()
		if c.store != nil {
			_ = c.store.DeleteObject(ctx, "hosts/"+hostID+".json")
		}
		return map[string]string{}, nil
	}

	targets := c.allocateMigrationTargets(hostID, sbxIDs)
	if poolURL == "" {
		return targets, errors.New("pool_service_url not configured")
	}

	migrated := make(map[string]string)
	b, _ := json.Marshal(map[string]any{
		"reason":  reason,
		"targets": targets,
	})
	reqCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, poolURL+"/__internal/drain", bytes.NewReader(b))
	var drainErr error
	if err == nil {
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Serverless-Session-Id", hostID)
		if strings.HasPrefix(poolURL, "https://") {
			if tok, err := GetIDToken(reqCtx, poolURL); err == nil && tok != "" {
				req.Header.Set("Authorization", "Bearer "+tok)
			}
		}
		if resp, doErr := c.httpClient.Do(req); doErr == nil {
			var res struct {
				Migrated map[string]string `json:"migrated"`
				Error    string            `json:"error"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&res)
			resp.Body.Close()
			for k, v := range res.Migrated {
				migrated[k] = v
			}
			if resp.StatusCode != http.StatusOK && res.Error != "" {
				drainErr = fmt.Errorf("drain %s returned %d: %s", hostID, resp.StatusCode, res.Error)
			}
		} else {
			drainErr = doErr
		}
	}

	// Fallback: if any sandbox on hostID was not migrated by the source container (e.g. replaced/dead session),
	// hydrate it directly onto its allocated target host from its latest stored overlay.
	for _, sbx := range sbxRecords {
		if migrated[sbx.SandboxID] != "" {
			continue
		}
		if c.store != nil {
			var latest SandboxRecord
			if _, exists, rErr := c.store.ReadJSON(ctx, "sandboxes/"+sbx.SandboxID+".json", &latest); rErr == nil && exists {
				if latest.Status != StatusRunning || (latest.HostID != "" && latest.HostID != hostID) {
					if latest.HostID != "" && latest.HostID != hostID {
						migrated[sbx.SandboxID] = latest.HostID
					}
					continue
				}
				sbx = latest
			}
		}
		targetHost := targets[sbx.SandboxID]
		if targetHost == "" || targetHost == hostID {
			targetHost = NewRandomID("host")
		}
		hydrated, hErr := c.sendHydrateCommand(ctx, targetHost, HydrateRequest{
			Record:     sbx,
			OverlayTar: sbx.OverlayPath,
			Reason:     reason,
		})
		if hErr == nil {
			migrated[sbx.SandboxID] = hydrated.HostID
			c.mu.Lock()
			c.sandboxes[hydrated.SandboxID] = hydrated
			c.mu.Unlock()
			drainErr = nil
		}
	}

	_ = c.SyncFromStore(ctx)
	c.mu.Lock()
	c.reconcileHostSandboxMapsLocked()
	if curHR, exists := c.hosts[hostID]; exists && curHR.ActiveSandboxes == 0 {
		delete(c.hosts, hostID)
	}
	c.mu.Unlock()
	if c.store != nil {
		_ = c.store.DeleteObject(ctx, "hosts/"+hostID+".json")
	}
	return migrated, drainErr
}

func (c *ControlPlaneServer) sendHydrateCommand(ctx context.Context, targetHostID string, reqPayload HydrateRequest) (SandboxRecord, error) {
	c.mu.RLock()
	poolURL := c.config.PoolServiceURL
	c.mu.RUnlock()
	if poolURL == "" {
		return SandboxRecord{}, errors.New("pool_service_url not configured")
	}
	b, _ := json.Marshal(reqPayload)
	reqCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, poolURL+"/__internal/hydrate", bytes.NewReader(b))
	if err != nil {
		return SandboxRecord{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Serverless-Session-Id", targetHostID)
	if strings.HasPrefix(poolURL, "https://") {
		if tok, err := GetIDToken(reqCtx, poolURL); err == nil && tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return SandboxRecord{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return SandboxRecord{}, fmt.Errorf("hydrate on %s failed (%d): %s", targetHostID, resp.StatusCode, string(errBody))
	}
	var out SandboxRecord
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return SandboxRecord{}, err
	}
	return out, nil
}

// allocateMigrationTargets selects Best-Fit existing warm hosts on `TargetRevision` first,
// reserving slots in memory so concurrent drains never overpack the same destination host,
// and mints new `host-<id>` sessions if existing warm hosts do not have enough free slots.
func (c *ControlPlaneServer) allocateMigrationTargets(excludeHostID string, sandboxIDs []string) map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reconcileHostSandboxMapsLocked()

	maxDensity := c.config.MaxDensity
	if maxDensity <= 0 {
		maxDensity = 10
	}
	var candidates []HostRecord
	for _, hr := range c.hosts {
		if hr.HostID == excludeHostID || hr.Status != HostActive {
			continue
		}
		if hr.OverloadState == "ADMISSION_BLOCKED" || hr.OverloadState == "HOT_SHEDDING" {
			continue
		}
		if c.config.TargetRevision != "" && hr.Revision != "" && hr.Revision != c.config.TargetRevision {
			continue
		}
		if hr.FreeSlots > 0 {
			candidates = append(candidates, hr)
		}
	}
	// Best-Fit: sort descending by ActiveSandboxes so we pack fuller warm hosts first
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].ActiveSandboxes != candidates[j].ActiveSandboxes {
			return candidates[i].ActiveSandboxes > candidates[j].ActiveSandboxes
		}
		return candidates[i].HostID < candidates[j].HostID
	})

	assignments := make(map[string]string, len(sandboxIDs))
	candIdx := 0
	var currentMintedHost string
	currentMintedUsed := 0

	for _, sbxID := range sandboxIDs {
		assigned := false
		for candIdx < len(candidates) {
			if candidates[candIdx].FreeSlots > 0 {
				hid := candidates[candIdx].HostID
				assignments[sbxID] = hid
				candidates[candIdx].FreeSlots--
				candidates[candIdx].ActiveSandboxes++
				c.reservedSlot[hid]++
				assigned = true
				break
			}
			candIdx++
		}
		if !assigned {
			if currentMintedHost == "" || currentMintedUsed >= maxDensity {
				currentMintedHost = NewRandomID("host")
				currentMintedUsed = 0
			}
			assignments[sbxID] = currentMintedHost
			currentMintedUsed++
		}
	}
	c.reconcileHostSandboxMapsLocked()
	return assignments
}

// BuildFleetSummary computes the complete fleet observability summary from RAM.
func (c *ControlPlaneServer) BuildFleetSummary() FleetSummary {
	c.mu.RLock()
	defer c.mu.RUnlock()

	hostsCopy := make(map[string]HostRecord, len(c.hosts))
	activeHosts := 0
	occupiedHosts := 0
	drainingHosts := 0
	deadHosts := 0
	maxDensity := c.config.MaxDensity
	if maxDensity <= 0 {
		maxDensity = 10
	}
	var totalHostMem int64
	var totalAllocMem int64
	var totalHostCPU int
	var totalAllocCPU int
	totalHotMigrations := 0
	totalMemRejections := 0
	for k, v := range c.hosts {
		hostsCopy[k] = v
		totalHotMigrations += v.HotMigrationsTriggered
		totalMemRejections += v.RejectedCreationsMemory
		switch v.Status {
		case HostActive:
			activeHosts++
			if v.ActiveSandboxes > 0 {
				occupiedHosts++
			}
			hMem := v.TotalMemoryBytes
			if hMem <= 0 {
				hMem = ParseMemoryBytes(v.HostMemory)
			}
			hCPU := v.TotalCPUMillicores
			if hCPU <= 0 {
				hCPU = ParseCPUMillicores(v.HostCPU)
			}
			totalHostMem += hMem
			totalAllocMem += v.AllocatedMemoryBytes
			totalHostCPU += hCPU
			totalAllocCPU += v.AllocatedCPUMillicores
		case HostDraining:
			drainingHosts++
		case HostDead, HostCrashed:
			deadHosts++
		}
	}

	sbxCopy := make(map[string]SandboxRecord, len(c.sandboxes))
	runningCount := 0
	suspendedCount := 0
	totalMigrations := 0
	totalCrashRecoveries := 0
	var totalUptimeMs int64
	var totalDowntimeMs int64
	now := time.Now().UTC()

	for k, v := range c.sandboxes {
		if v.Status == StatusDeleted {
			continue
		}
		sbxCopy[k] = v
		if v.Status == StatusRunning {
			runningCount++
		} else if v.Status == StatusSuspended || v.Status == StatusStopped {
			suspendedCount++
		}
		totalMigrations += v.MigrationCount
		totalCrashRecoveries += v.CrashRecoveries
		ageMs := max(now.Sub(v.CreatedAt).Milliseconds(), 1000)
		totalUptimeMs += ageMs
		totalDowntimeMs += v.TotalDowntimeMs
	}

	totalCap := activeHosts * maxDensity
	effCap := occupiedHosts * maxDensity
	effPct := 100.0
	if effCap > 0 {
		effPct = math.Min(100.0, (float64(runningCount)/float64(effCap))*100.0)
	} else if totalCap > 0 {
		effPct = 0.0
	}
	slaPct := 100.0
	if totalUptimeMs > 0 && totalDowntimeMs > 0 {
		slaPct = math.Max(0.0, (1.0-(float64(totalDowntimeMs)/float64(totalUptimeMs)))*100.0)
		if slaPct < 99.9 && totalUptimeMs < 60000 {
			// Normalize short synthetic test runs over a 24h SLA window
			slaPct = (1.0 - (float64(totalDowntimeMs) / float64(24*3600*1000))) * 100.0
		}
	}

	evts := make([]ActivityEvent, 0, len(c.recentEvents))
	evts = append(evts, c.recentEvents...)
	sort.Slice(evts, func(i, j int) bool {
		return evts[i].Timestamp.After(evts[j].Timestamp)
	})

	// Scale-to-zero signal on control plane when 0 running sandboxes
	SetInstanceBusyState(runningCount > 0)

	var fleetMemOvercommit float64
	if totalHostMem > 0 {
		fleetMemOvercommit = math.Round((float64(totalAllocMem)/float64(totalHostMem))*100) / 100
	}
	var fleetCPUOvercommit float64
	if totalHostCPU > 0 {
		fleetCPUOvercommit = math.Round((float64(totalAllocCPU)/float64(totalHostCPU))*100) / 100
	}

	return FleetSummary{
		LeaderID:                   c.instanceID,
		Config:                     c.config,
		Hosts:                      hostsCopy,
		Sandboxes:                  sbxCopy,
		TotalHosts:                 len(hostsCopy),
		ActiveHosts:                activeHosts,
		DrainingHosts:              drainingHosts,
		DeadHosts:                  deadHosts,
		TotalSandboxes:             len(sbxCopy),
		RunningSandboxes:           runningCount,
		SuspendedSandboxes:         suspendedCount,
		TotalCapacitySlots:         totalCap,
		UsedCapacitySlots:          runningCount,
		BinPackEfficiencyPct:       math.Round(effPct*10) / 10,
		FleetMemoryOvercommitRatio: fleetMemOvercommit,
		FleetCPUOvercommitRatio:    fleetCPUOvercommit,
		TotalMigrations:            totalMigrations,
		TotalHotMigrations:         totalHotMigrations,
		TotalMemoryRejections:      totalMemRejections,
		TotalCrashRecoveries:       totalCrashRecoveries,
		FleetUptimeSLAPct:          math.Round(slaPct*1000) / 1000,
		RecentEvents:               evts,
		UpdatedAt:                  now,
	}
}

// ServeHTTP handles all `/v1/*` and `/healthz` control plane endpoints.
func (c *ControlPlaneServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.mu.RLock()
	minVer := c.config.MinClientVersion
	c.mu.RUnlock()
	if minVer == "" {
		minVer = DefaultMinClientVersion
	}
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("X-Control-Plane-Instance", c.instanceID)
	w.Header().Set("X-Sbx-Server-Version", DispenserVersion)
	w.Header().Set("X-Sbx-Min-Client-Version", minVer)

	path := r.URL.Path
	if !strings.HasPrefix(path, "/proxy/") && path != "/__session/port-forward" && r.Method != http.MethodGet && r.Method != http.MethodPost && r.Method != http.MethodDelete && r.Method != http.MethodHead {
		writeCPJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}

	if strings.HasPrefix(path, "/v1/") && path != "/v1/heartbeat" && path != "/v1/migration-targets" {
		if err := CheckClientVersion(r, minVer); err != nil {
			WriteUpgradeRequired(w, r, minVer)
			return
		}
	}

	switch {
	case strings.HasPrefix(path, "/proxy/"):
		c.handleProxyViaCP(w, r)
	case path == "/__session/port-forward":
		c.handlePortForwardViaCP(w, r)
	case path == "/healthz":
		writeCPJSON(w, http.StatusOK, map[string]any{
			"ok":                 true,
			"instance_id":        c.instanceID,
			"is_leader":          c.isLeader,
			"server_version":     DispenserVersion,
			"min_client_version": minVer,
		})
	case path == "/v1/candidates":
		c.handleGetCandidates(w, r)
	case path == "/v1/heartbeat" && r.Method == http.MethodPost:
		c.handlePostHeartbeat(w, r)
	case path == "/v1/migration-targets" && r.Method == http.MethodPost:
		c.handlePostMigrationTargets(w, r)
	case path == "/v1/sandboxes" && r.Method == http.MethodGet:
		c.handleListSandboxes(w, r)
	case strings.HasPrefix(path, "/v1/sandboxes/") && strings.HasSuffix(path, "/migrate") && r.Method == http.MethodPost:
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/v1/sandboxes/"), "/migrate")
		c.handleMigrateSandboxViaCP(w, r, id)
	case strings.HasPrefix(path, "/v1/sandboxes/"):
		id := strings.TrimPrefix(path, "/v1/sandboxes/")
		if r.Method == http.MethodDelete {
			c.handleDeleteSandbox(w, r, id)
		} else {
			c.handleGetSandbox(w, r, id)
		}
	case path == "/v1/hosts" && r.Method == http.MethodGet:
		c.handleListHosts(w, r)
	case strings.HasPrefix(path, "/v1/hosts/") && strings.HasSuffix(path, "/drain") && r.Method == http.MethodPost:
		hid := strings.TrimSuffix(strings.TrimPrefix(path, "/v1/hosts/"), "/drain")
		c.handleDrainHost(w, r, hid)
	case strings.HasPrefix(path, "/v1/hosts/") && strings.HasSuffix(path, "/simulate-crash") && r.Method == http.MethodPost:
		hid := strings.TrimSuffix(strings.TrimPrefix(path, "/v1/hosts/"), "/simulate-crash")
		c.handleSimulateCrashHost(w, r, hid)
	case path == "/v1/compact" && r.Method == http.MethodPost:
		c.handleCompact(w, r)
	case path == "/v1/deploy" && r.Method == http.MethodPost:
		c.handleDeployFleet(w, r)
	case path == "/v1/stats" && r.Method == http.MethodGet:
		writeCPJSON(w, http.StatusOK, c.BuildFleetSummary())
	case path == "/v1/metrics" && r.Method == http.MethodGet:
		c.handleGetMetrics(w, r)
	case path == "/v1/security/policy":
		if r.Method == http.MethodPost {
			c.handleUpdateSecurityPolicy(w, r)
		} else {
			c.handleGetSecurityPolicy(w, r)
		}
	case path == "/v1/events" && r.Method == http.MethodGet:
		sum := c.BuildFleetSummary()
		writeCPJSON(w, http.StatusOK, map[string]any{"events": sum.RecentEvents})
	case path == "/v1/config":
		if r.Method == http.MethodPost {
			c.handleUpdateConfig(w, r)
		} else {
			c.mu.RLock()
			cfg := c.config
			c.mu.RUnlock()
			normalizeFleetConfig(&cfg)
			writeCPJSON(w, http.StatusOK, cfg)
		}
	case path == "/v1/templates":
		if r.Method == http.MethodPost {
			c.handleCreateTemplate(w, r)
		} else {
			c.handleListTemplates(w, r)
		}
	case strings.HasPrefix(path, "/v1/templates/") && r.Method == http.MethodDelete:
		name := strings.TrimPrefix(path, "/v1/templates/")
		c.handleDeleteTemplate(w, r, name)
	case path == "/v1/snapshots" && r.Method == http.MethodGet:
		c.handleListSnapshots(w, r)
	case strings.HasPrefix(path, "/v1/snapshots/") && r.Method == http.MethodDelete:
		snapID := strings.TrimPrefix(path, "/v1/snapshots/")
		c.handleDeleteSnapshot(w, r, snapID)
	default:
		writeCPJSON(w, http.StatusNotFound, map[string]any{"error": "unknown endpoint " + path})
	}
}

func writeCPJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func (c *ControlPlaneServer) handleGetCandidates(w http.ResponseWriter, _ *http.Request) {
	c.mu.Lock()
	c.reconcileHostSandboxMapsLocked()
	cfg := c.config
	maxDensity := cfg.MaxDensity
	if maxDensity <= 0 {
		maxDensity = 10
	}
	candidates := make([]CandidateHost, 0)
	totalFree := 0
	for _, hr := range c.hosts {
		if hr.Status != HostActive {
			continue
		}
		if isDedicatedOrPinnedHost(hr.HostID, hr.Sandboxes) {
			continue
		}
		if hr.OverloadState == "ADMISSION_BLOCKED" || hr.OverloadState == "HOT_SHEDDING" {
			continue
		}
		if cfg.TargetRevision != "" && hr.Revision != "" && hr.Revision != cfg.TargetRevision {
			continue
		}
		if hr.FreeSlots > 0 {
			candidates = append(candidates, CandidateHost{
				HostID:          hr.HostID,
				Revision:        hr.Revision,
				ActiveSandboxes: hr.ActiveSandboxes,
				FreeSlots:       hr.FreeSlots,
				MaxDensity:      maxDensity,
			})
			totalFree += hr.FreeSlots
		}
	}
	c.mu.Unlock()

	// Best-Fit order: pack the fullest open host first to maximize bin-packing density!
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].ActiveSandboxes != candidates[j].ActiveSandboxes {
			return candidates[i].ActiveSandboxes > candidates[j].ActiveSandboxes
		}
		return candidates[i].HostID < candidates[j].HostID
	})

	refreshSec := 15
	if totalFree <= 2 {
		refreshSec = 2
	} else if totalFree <= 5 {
		refreshSec = 5
	}
	normalizeFleetConfig(&cfg)
	writeCPJSON(w, http.StatusOK, CandidatesResponse{
		PoolServiceURL:     cfg.PoolServiceURL,
		TargetRevision:     cfg.TargetRevision,
		MaxDensity:         maxDensity,
		HostCPU:            cfg.HostCPU,
		HostMemory:         cfg.HostMemory,
		MinClientVersion:   cfg.MinClientVersion,
		ServerVersion:      DispenserVersion,
		RefreshIntervalSec: refreshSec,
		Candidates:         candidates,
		UpdatedAt:          time.Now().UTC(),
	})
}

func (c *ControlPlaneServer) handlePostHeartbeat(w http.ResponseWriter, r *http.Request) {
	var hr HostRecord
	if err := json.NewDecoder(io.LimitReader(r.Body, 2<<20)).Decode(&hr); err != nil {
		writeCPJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if err := ValidateID(hr.HostID); err != nil {
		writeCPJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	hr.LastHeartbeatAt = time.Now().UTC()

	// Check for sandboxes that used to be on hr.HostID in RAM but are no longer reported by hr.Sandboxes
	c.mu.RLock()
	var missingOnHost []string
	var verifyInStore []string
	for id, existing := range c.sandboxes {
		if existing.HostID == hr.HostID {
			if _, stillPresent := hr.Sandboxes[id]; !stillPresent {
				missingOnHost = append(missingOnHost, id)
			}
		}
	}
	for id := range hr.Sandboxes {
		if _, exists := c.sandboxes[id]; !exists {
			verifyInStore = append(verifyInStore, id)
		}
	}
	c.mu.RUnlock()

	if hr.Status == HostDead {
		c.mu.Lock()
		delete(c.hosts, hr.HostID)
		c.reconcileHostSandboxMapsLocked()
		cfg := c.config
		c.mu.Unlock()
		normalizeFleetConfig(&cfg)
		if c.store != nil {
			_ = c.store.DeleteObject(r.Context(), "hosts/"+hr.HostID+".json")
		}
		writeCPJSON(w, http.StatusOK, map[string]any{
			"ok":      true,
			"retired": true,
		})
		return
	}

	deletedOrMoved := make(map[string]*SandboxRecord)
	fencedSet := make(map[string]bool)
	if c.store != nil {
		for _, id := range missingOnHost {
			var storeRec SandboxRecord
			if _, exists, err := c.store.ReadJSON(r.Context(), "sandboxes/"+id+".json", &storeRec); err == nil {
				if !exists || storeRec.Status == StatusDeleted {
					deletedOrMoved[id] = nil
				} else {
					cpRec := storeRec
					deletedOrMoved[id] = &cpRec
				}
			}
		}
		for _, id := range verifyInStore {
			var storeRec SandboxRecord
			if _, exists, err := c.store.ReadJSON(r.Context(), "sandboxes/"+id+".json", &storeRec); err == nil {
				if !exists || storeRec.Status == StatusDeleted || (storeRec.HostID != "" && storeRec.HostID != hr.HostID) {
					fencedSet[id] = true
				}
			}
		}
	} else {
		for _, id := range missingOnHost {
			deletedOrMoved[id] = nil
		}
	}

	var fencedSandboxes []string
	c.mu.Lock()
	for id, replacement := range deletedOrMoved {
		if replacement == nil {
			delete(c.sandboxes, id)
		} else {
			c.sandboxes[id] = *replacement
		}
	}
	for id, s := range hr.Sandboxes {
		if fencedSet[id] {
			delete(hr.Sandboxes, id)
			fencedSandboxes = append(fencedSandboxes, id)
			continue
		}
		cur, exists := c.sandboxes[s.SandboxID]
		if exists && cur.HostID != "" && cur.HostID != hr.HostID && cur.Epoch > s.Epoch {
			delete(hr.Sandboxes, id)
			fencedSandboxes = append(fencedSandboxes, id)
			continue
		}
		if !exists || s.Epoch >= cur.Epoch {
			c.sandboxes[s.SandboxID] = s
		}
	}
	c.hosts[hr.HostID] = hr
	for _, evt := range hr.RecentEvents {
		c.recordEventLocked(evt)
	}
	c.reconcileHostSandboxMapsLocked()
	reconciledHR := c.hosts[hr.HostID]
	retireEmpty := reconciledHR.ActiveSandboxes == 0 && (reconciledHR.Status == HostDead || reconciledHR.Status == HostDraining || (!reconciledHR.StartedAt.IsZero() && time.Since(reconciledHR.StartedAt) > 15*time.Second) || len(fencedSandboxes) > 0)
	if retireEmpty {
		delete(c.hosts, hr.HostID)
	}
	cfg := c.config
	c.mu.Unlock()

	normalizeFleetConfig(&cfg)
	if c.store != nil {
		if retireEmpty {
			_ = c.store.DeleteObject(r.Context(), "hosts/"+hr.HostID+".json")
		} else {
			_, _ = c.store.WriteJSONCAS(r.Context(), "hosts/"+hr.HostID+".json", reconciledHR, -1)
		}
	}
	writeCPJSON(w, http.StatusOK, map[string]any{
		"ok":                          true,
		"retired":                     retireEmpty,
		"fenced_sandboxes":            fencedSandboxes,
		"target_revision":             cfg.TargetRevision,
		"max_density":                 cfg.MaxDensity,
		"min_client_version":          cfg.MinClientVersion,
		"min_free_memory_mb":          cfg.MinFreeMemoryMB,
		"min_free_memory_pct":         cfg.MinFreeMemoryPct,
		"max_memory_overcommit_ratio": cfg.MaxMemoryOvercommitRatio,
		"hot_memory_threshold_pct":    cfg.HotMemoryThresholdPct,
		"hot_cpu_threshold_pct":       cfg.HotCPUThresholdPct,
		"default_idle_timeout_sec":    cfg.DefaultIdleTimeout,
		"server_version":              DispenserVersion,
	})
}

func (c *ControlPlaneServer) selectOrMintProxyHost(rec SandboxRecord, exclude map[string]bool) (poolURL string, hostID string) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	poolURL = strings.TrimRight(c.config.PoolServiceURL, "/")
	if rec.HostID != "" && !exclude[rec.HostID] {
		if hr, exists := c.hosts[rec.HostID]; exists && hr.Status == HostActive {
			return poolURL, rec.HostID
		}
	}
	for _, cand := range c.hosts {
		if exclude[cand.HostID] {
			continue
		}
		if cand.Status == HostActive && cand.FreeSlots > 0 && !isDedicatedOrPinnedHost(cand.HostID, cand.Sandboxes) {
			return poolURL, cand.HostID
		}
	}
	return poolURL, fmt.Sprintf("host-%08x", time.Now().UnixNano()&0xffffffff)
}

func (c *ControlPlaneServer) handlePortForwardViaCP(w http.ResponseWriter, r *http.Request) {
	idOrName := extractSandboxID(r, "")
	portStr := strings.TrimSpace(r.Header.Get("X-Sandbox-Forward-Port"))
	if portStr == "" {
		portStr = strings.TrimSpace(r.URL.Query().Get("port"))
	}
	port, err := strconv.Atoi(portStr)
	if idOrName == "" || err != nil || port < 1 || port > 65535 {
		writeCPJSON(w, http.StatusBadRequest, map[string]any{"error": "valid sandbox_id and port are required"})
		return
	}
	rec, found, resolveErr := c.resolveSandboxByIDOrName(r.Context(), idOrName)
	if resolveErr != nil {
		writeCPJSON(w, http.StatusConflict, map[string]any{"error": resolveErr.Error()})
		return
	}
	if !found {
		writeCPJSON(w, http.StatusNotFound, map[string]any{"error": fmt.Sprintf("sandbox %q not found", idOrName)})
		return
	}
	poolURL, hostID := c.selectOrMintProxyHost(rec, map[string]bool{})
	if poolURL == "" {
		writeCPJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "pool_service_url is not configured"})
		return
	}
	parsedPool, pErr := url.Parse(poolURL)
	if pErr != nil {
		writeCPJSON(w, http.StatusInternalServerError, map[string]any{"error": pErr.Error()})
		return
	}
	proxy := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(parsedPool)
			pr.Out.URL.Path = "/__session/port-forward"
			pr.Out.URL.RawPath = ""
			pr.Out.URL.RawQuery = fmt.Sprintf("sandbox_id=%s&port=%d", url.QueryEscape(rec.SandboxID), port)
			pr.Out.Host = parsedPool.Host
			pr.Out.Header.Set("X-Serverless-Session-Id", hostID)
			pr.Out.Header.Set("X-Sandbox-Id", rec.SandboxID)
			pr.Out.Header.Set("X-Sandbox-Forward-Port", strconv.Itoa(port))
			if strings.HasPrefix(poolURL, "https://") {
				if tok, tokErr := GetIDToken(r.Context(), poolURL); tokErr == nil && tok != "" {
					pr.Out.Header.Set("Authorization", "Bearer "+tok)
				}
			}
		},
		FlushInterval: -1,
	}
	proxy.ServeHTTP(w, r)
}

func (c *ControlPlaneServer) handleProxyViaCP(w http.ResponseWriter, r *http.Request) {
	idOrName, port, subPath, err := parseProxyPathAndPort(r)
	if err != nil {
		writeCPJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}

	rec, found, resolveErr := c.resolveSandboxByIDOrName(r.Context(), idOrName)
	if resolveErr != nil {
		writeCPJSON(w, http.StatusConflict, map[string]any{"error": resolveErr.Error()})
		return
	}
	if !found {
		writeCPJSON(w, http.StatusNotFound, map[string]any{"error": fmt.Sprintf("sandbox %q not found", idOrName)})
		return
	}

	tried := make(map[string]bool)
	poolURL, hostID := c.selectOrMintProxyHost(rec, tried)
	if poolURL == "" {
		writeCPJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "pool_service_url is not configured"})
		return
	}

	targetPath := fmt.Sprintf("/proxy/%s/%d%s", rec.SandboxID, port, subPath)
	parsedPool, err := url.Parse(poolURL)
	if err != nil {
		writeCPJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}

	if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") || strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade") {
		proxy := &httputil.ReverseProxy{
			Rewrite: func(pr *httputil.ProxyRequest) {
				pr.SetURL(parsedPool)
				pr.Out.URL.Path = targetPath
				pr.Out.URL.RawPath = ""
				pr.Out.URL.RawQuery = r.URL.RawQuery
				pr.Out.Host = parsedPool.Host
				pr.Out.Header.Set("X-Serverless-Session-Id", hostID)
				pr.Out.Header.Set("X-Sandbox-Id", rec.SandboxID)
				if strings.HasPrefix(poolURL, "https://") {
					if tok, tokErr := GetIDToken(r.Context(), poolURL); tokErr == nil && tok != "" {
						pr.Out.Header.Set("Authorization", "Bearer "+tok)
					}
				}
			},
			FlushInterval: -1,
		}
		proxy.ServeHTTP(w, r)
		return
	}

	var bodyBytes []byte
	if r.Body != nil {
		const maxProxyBody = 16 << 20
		bodyBytes, _ = io.ReadAll(io.LimitReader(r.Body, maxProxyBody+1))
		if len(bodyBytes) > maxProxyBody {
			http.Error(w, "proxy request payload exceeds 16 MiB limit; use direct pool URL for larger uploads", http.StatusRequestEntityTooLarge)
			return
		}
	}

	var lastDoErr error
	for attempt := 0; attempt < 4; attempt++ {
		tried[hostID] = true
		u := poolURL + targetPath
		if r.URL.RawQuery != "" {
			u += "?" + r.URL.RawQuery
		}
		outReq, reqErr := http.NewRequestWithContext(r.Context(), r.Method, u, bytes.NewReader(bodyBytes))
		if reqErr != nil {
			writeCPJSON(w, http.StatusInternalServerError, map[string]any{"error": reqErr.Error()})
			return
		}
		for k, vals := range r.Header {
			for _, v := range vals {
				outReq.Header.Add(k, v)
			}
		}
		outReq.Header.Set("X-Serverless-Session-Id", hostID)
		outReq.Header.Set("X-Sandbox-Id", rec.SandboxID)
		if strings.HasPrefix(poolURL, "https://") {
			if tok, tokErr := GetIDToken(r.Context(), poolURL); tokErr == nil && tok != "" {
				outReq.Header.Set("Authorization", "Bearer "+tok)
			}
		}

		resp, doErr := c.httpClient.Do(outReq)
		if doErr != nil {
			lastDoErr = doErr
			_, hostID = c.selectOrMintProxyHost(rec, tried)
			continue
		}

		if resp.StatusCode == http.StatusConflict {
			redirectHost := strings.TrimSpace(resp.Header.Get("X-Sandbox-Host-Session"))
			if redirectHost == "" {
				redirectHost = strings.TrimSpace(resp.Header.Get("X-Sandbox-Current-Host"))
			}
			_ = resp.Body.Close()
			if redirectHost != "" && redirectHost != hostID {
				hostID = redirectHost
				c.mu.Lock()
				if cur, ok := c.sandboxes[rec.SandboxID]; ok {
					cur.HostID = redirectHost
					c.sandboxes[rec.SandboxID] = cur
				}
				c.mu.Unlock()
				continue
			}
		}
		if (resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusTooManyRequests) && attempt < 3 {
			_ = resp.Body.Close()
			_, hostID = c.selectOrMintProxyHost(rec, tried)
			continue
		}

		for k, vals := range resp.Header {
			for _, v := range vals {
				w.Header().Add(k, v)
			}
		}
		w.Header().Set("X-Sandbox-Host-Session", hostID)
		w.Header().Set("X-Sandbox-Id", rec.SandboxID)
		w.WriteHeader(resp.StatusCode)
		flusher, _ := w.(http.Flusher)
		buf := make([]byte, 32*1024)
		for {
			n, rErr := resp.Body.Read(buf)
			if n > 0 {
				_, _ = w.Write(buf[:n])
				if flusher != nil {
					flusher.Flush()
				}
			}
			if rErr != nil {
				break
			}
		}
		_ = resp.Body.Close()
		return
	}
	if lastDoErr != nil {
		writeCPJSON(w, http.StatusBadGateway, map[string]any{"error": lastDoErr.Error()})
		return
	}
	writeCPJSON(w, http.StatusBadGateway, map[string]any{"error": "exceeded max host redirects for proxy"})
}

func (c *ControlPlaneServer) handlePostMigrationTargets(w http.ResponseWriter, r *http.Request) {
	var req MigrationTargetsRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeCPJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	targets := c.allocateMigrationTargets(req.SourceHostID, req.SandboxIDs)
	c.mu.RLock()
	poolURL := c.config.PoolServiceURL
	c.mu.RUnlock()

	writeCPJSON(w, http.StatusOK, MigrationTargetsResponse{
		Targets:        targets,
		PoolServiceURL: poolURL,
	})
}

func (c *ControlPlaneServer) handleListSandboxes(w http.ResponseWriter, r *http.Request) {
	prefix := strings.TrimSpace(r.URL.Query().Get("prefix"))
	nameFilter := strings.TrimSpace(r.URL.Query().Get("name"))
	statusFilter := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("status")))

	c.mu.RLock()
	list := make([]SandboxRecord, 0, len(c.sandboxes))
	for _, s := range c.sandboxes {
		if s.Status == StatusDeleted {
			continue
		}
		if nameFilter != "" && s.Name != nameFilter && s.SandboxID != nameFilter {
			continue
		}
		if prefix != "" && !strings.HasPrefix(s.SandboxID, prefix) && !strings.HasPrefix(s.Name, prefix) {
			continue
		}
		if statusFilter != "" && string(s.Status) != statusFilter {
			continue
		}
		list = append(list, s)
	}
	c.mu.RUnlock()

	sort.Slice(list, func(i, j int) bool {
		return list[i].SandboxID < list[j].SandboxID
	})
	writeCPJSON(w, http.StatusOK, map[string]any{
		"sandboxes": list,
		"count":     len(list),
	})
}

func (c *ControlPlaneServer) resolveSandboxByIDOrName(ctx context.Context, idOrName string) (SandboxRecord, bool, error) {
	if c.store != nil {
		var rec SandboxRecord
		_, ok, err := c.store.ReadJSON(ctx, "sandboxes/"+idOrName+".json", &rec)
		if err == nil && ok && rec.Status != StatusDeleted {
			c.mu.Lock()
			c.sandboxes[rec.SandboxID] = rec
			c.mu.Unlock()
			return rec, true, nil
		}
		if ExtractInitialHostFromSandboxID(idOrName) == "" {
			if keys, err := c.store.ListObjects(ctx, "sandboxes/"); err == nil {
				var matches []SandboxRecord
				for _, k := range keys {
					var s SandboxRecord
					if _, okS, rErr := c.store.ReadJSON(ctx, k, &s); rErr == nil && okS && s.Name == idOrName && s.Status != StatusDeleted {
						matches = append(matches, s)
					}
				}
				if len(matches) > 1 {
					sort.Slice(matches, func(i, j int) bool { return matches[i].SandboxID < matches[j].SandboxID })
					ids := make([]string, 0, len(matches))
					for _, m := range matches {
						ids = append(ids, m.SandboxID)
					}
					return SandboxRecord{}, false, fmt.Errorf("ambiguous sandbox name %q matches %d sandboxes (%s); please specify the exact sandbox_id", idOrName, len(matches), strings.Join(ids, ", "))
				}
				if len(matches) == 1 {
					c.mu.Lock()
					c.sandboxes[matches[0].SandboxID] = matches[0]
					c.mu.Unlock()
					return matches[0], true, nil
				}
			}
		}
		c.mu.Lock()
		delete(c.sandboxes, idOrName)
		c.reconcileHostSandboxMapsLocked()
		c.mu.Unlock()
		return SandboxRecord{}, false, nil
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	if rec, ok := c.sandboxes[idOrName]; ok && rec.Status != StatusDeleted {
		return rec, true, nil
	}
	var matches []SandboxRecord
	for _, s := range c.sandboxes {
		if s.Name == idOrName && s.Status != StatusDeleted {
			matches = append(matches, s)
		}
	}
	if len(matches) > 1 {
		sort.Slice(matches, func(i, j int) bool { return matches[i].SandboxID < matches[j].SandboxID })
		ids := make([]string, 0, len(matches))
		for _, m := range matches {
			ids = append(ids, m.SandboxID)
		}
		return SandboxRecord{}, false, fmt.Errorf("ambiguous sandbox name %q matches %d sandboxes (%s); please specify the exact sandbox_id", idOrName, len(matches), strings.Join(ids, ", "))
	}
	if len(matches) == 1 {
		return matches[0], true, nil
	}
	return SandboxRecord{}, false, nil
}

func (c *ControlPlaneServer) handleGetSandbox(w http.ResponseWriter, r *http.Request, sandboxID string) {
	if err := ValidateID(sandboxID); err != nil {
		writeCPJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	rec, ok, ambErr := c.resolveSandboxByIDOrName(r.Context(), sandboxID)
	if ambErr != nil {
		writeCPJSON(w, http.StatusConflict, map[string]any{"error": ambErr.Error()})
		return
	}
	if !ok {
		writeCPJSON(w, http.StatusNotFound, map[string]any{"error": fmt.Sprintf("sandbox %q not found", sandboxID)})
		return
	}
	writeCPJSON(w, http.StatusOK, rec)
}

func (c *ControlPlaneServer) handleMigrateSandboxViaCP(w http.ResponseWriter, r *http.Request, sandboxID string) {
	if err := ValidateID(sandboxID); err != nil {
		writeCPJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	var req MigrateSandboxRequest
	if r.Body != nil {
		_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req)
	}
	rec, ok, ambErr := c.resolveSandboxByIDOrName(r.Context(), sandboxID)
	if ambErr != nil {
		writeCPJSON(w, http.StatusConflict, map[string]any{"error": ambErr.Error()})
		return
	}
	if !ok {
		writeCPJSON(w, http.StatusNotFound, map[string]any{"error": fmt.Sprintf("sandbox %q not found", sandboxID)})
		return
	}
	req.SandboxID = rec.SandboxID
	if req.TargetHostID != "" {
		if err := ValidateID(req.TargetHostID); err != nil {
			writeCPJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		req.PinnedHost = true
	}
	c.mu.RLock()
	poolURL := c.config.PoolServiceURL
	c.mu.RUnlock()
	if poolURL == "" {
		writeCPJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "pool_service_url is not configured on control plane"})
		return
	}
	hostID := rec.HostID
	if hostID == "" {
		targets := c.allocateMigrationTargets("", []string{rec.SandboxID})
		hostID = targets[rec.SandboxID]
	}
	var migrated SandboxRecord
	status, err := c.doPoolJSON(r.Context(), http.MethodPost, poolURL+"/__session/migrate", hostID, req, &migrated)
	if err != nil || status != http.StatusOK {
		if err != nil {
			writeCPJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
			return
		}
		writeCPJSON(w, status, map[string]any{"error": fmt.Sprintf("migrate on host %s returned HTTP %d", hostID, status)})
		return
	}
	if migrated.PreviousHostID == "" {
		migrated.PreviousHostID = rec.HostID
	}
	c.mu.Lock()
	c.sandboxes[migrated.SandboxID] = migrated
	c.reconcileHostSandboxMapsLocked()
	c.mu.Unlock()
	writeCPJSON(w, http.StatusOK, migrated)
}

func (c *ControlPlaneServer) handleDeleteSandbox(w http.ResponseWriter, r *http.Request, sandboxID string) {
	if err := ValidateID(sandboxID); err != nil {
		writeCPJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	resolved, found, ambErr := c.resolveSandboxByIDOrName(r.Context(), sandboxID)
	if ambErr != nil {
		writeCPJSON(w, http.StatusConflict, map[string]any{"error": ambErr.Error()})
		return
	}
	if found {
		sandboxID = resolved.SandboxID
	}
	c.mu.Lock()
	rec, inRAM := c.sandboxes[sandboxID]
	delete(c.sandboxes, sandboxID)
	c.reconcileHostSandboxMapsLocked()
	poolURL := c.config.PoolServiceURL
	c.mu.Unlock()

	if found {
		rec = resolved
	}
	if !inRAM && !found {
		writeCPJSON(w, http.StatusNotFound, map[string]any{"error": fmt.Sprintf("sandbox %q not found", sandboxID)})
		return
	}

	if rec.HostID != "" && poolURL != "" && rec.Status == StatusRunning {
		reqCtx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		body, _ := json.Marshal(map[string]string{"sandbox_id": sandboxID})
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, poolURL+"/__session/delete", bytes.NewReader(body))
		if err == nil {
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Serverless-Session-Id", rec.HostID)
			req.Header.Set("X-Sandbox-Id", sandboxID)
			if strings.HasPrefix(poolURL, "https://") {
				if tok, err := GetIDToken(reqCtx, poolURL); err == nil && tok != "" {
					req.Header.Set("Authorization", "Bearer "+tok)
				}
			}
			if resp, dErr := c.httpClient.Do(req); dErr == nil {
				resp.Body.Close()
			}
		}
	}
	if c.store != nil {
		_ = c.store.DeleteObject(r.Context(), "sandboxes/"+sandboxID+".json")
		_ = c.store.DeleteObject(r.Context(), fmt.Sprintf("persistence/%s/overlay.tar", sandboxID))
	}
	writeCPJSON(w, http.StatusOK, map[string]any{
		"sandbox_id": sandboxID,
		"status":     StatusDeleted,
	})
}

func (c *ControlPlaneServer) handleListHosts(w http.ResponseWriter, r *http.Request) {
	if c.store != nil {
		_ = c.SyncFromStore(r.Context())
	}
	c.mu.Lock()
	c.reconcileHostSandboxMapsLocked()
	list := make([]HostRecord, 0, len(c.hosts))
	for _, hr := range c.hosts {
		list = append(list, hr)
	}
	c.mu.Unlock()

	sort.Slice(list, func(i, j int) bool {
		return list[i].HostID < list[j].HostID
	})
	writeCPJSON(w, http.StatusOK, map[string]any{
		"hosts": list,
		"count": len(list),
	})
}

func (c *ControlPlaneServer) handleDrainHost(w http.ResponseWriter, r *http.Request, hostID string) {
	if err := ValidateID(hostID); err != nil {
		writeCPJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	migrated, err := c.TriggerHostDrain(r.Context(), hostID, "api_drain")
	if err != nil {
		writeCPJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error(), "migrated": migrated})
		return
	}
	writeCPJSON(w, http.StatusOK, map[string]any{
		"host_id":  hostID,
		"status":   HostDraining,
		"migrated": migrated,
	})
}

func (c *ControlPlaneServer) handleSimulateCrashHost(w http.ResponseWriter, r *http.Request, hostID string) {
	if err := ValidateID(hostID); err != nil {
		writeCPJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	c.mu.RLock()
	poolURL := c.config.PoolServiceURL
	c.mu.RUnlock()

	if poolURL != "" {
		reqCtx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, poolURL+"/__internal/simulate-crash", nil)
		if err == nil {
			req.Header.Set("X-Serverless-Session-Id", hostID)
			if strings.HasPrefix(poolURL, "https://") {
				if tok, err := GetIDToken(reqCtx, poolURL); err == nil && tok != "" {
					req.Header.Set("Authorization", "Bearer "+tok)
				}
			}
			if resp, dErr := c.httpClient.Do(req); dErr == nil {
				resp.Body.Close()
			}
		}
	}

	c.mu.Lock()
	if hr, ok := c.hosts[hostID]; ok {
		hr.Status = HostCrashed
		hr.LastHeartbeatAt = time.Now().UTC().Add(-1 * time.Hour)
		c.hosts[hostID] = hr
		if c.store != nil {
			_, _ = c.store.WriteJSONCAS(r.Context(), "hosts/"+hostID+".json", hr, -1)
		}
	}
	c.mu.Unlock()

	// Run watchdog pass immediately if requested with ?immediate=true, otherwise next tick recovers it
	if r.URL.Query().Get("immediate") != "false" {
		_ = c.resurrectDeadHost(r.Context(), hostID)
	}
	writeCPJSON(w, http.StatusOK, map[string]any{
		"host_id": hostID,
		"status":  HostDead,
		"message": "Simulated crash on host and triggered watchdog resurrection",
	})
}

func (c *ControlPlaneServer) handleCompact(w http.ResponseWriter, r *http.Request) {
	res, err := c.CompactFleet(r.Context(), true)
	if err != nil {
		writeCPJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error(), "result": res})
		return
	}
	writeCPJSON(w, http.StatusOK, res)
}

func (c *ControlPlaneServer) handleUpdateConfig(w http.ResponseWriter, r *http.Request) {
	var patch struct {
		MaxDensity               *int     `json:"max_density"`
		HostCPU                  *string  `json:"host_cpu"`
		HostMemory               *string  `json:"host_memory"`
		TargetRevision           *string  `json:"target_revision"`
		PoolServiceURL           *string  `json:"pool_service_url"`
		ControlPlaneURL          *string  `json:"control_plane_url"`
		UIServiceURL             *string  `json:"ui_service_url"`
		MinClientVersion         *string  `json:"min_client_version"`
		Project                  *string  `json:"project"`
		Region                   *string  `json:"region"`
		PeriodicSyncSec          *int     `json:"periodic_sync_sec"`
		DefaultIdleTimeout       *int     `json:"default_idle_timeout_sec"`
		HeartbeatTimeoutSec      *int     `json:"heartbeat_timeout_sec"`
		GracePeriodSec           *int     `json:"grace_period_sec"`
		AutoBinPack              *bool    `json:"auto_bin_pack"`
		BinPackHysteresis        *int     `json:"bin_pack_hysteresis"`
		MinFreeMemoryMB          *int     `json:"min_free_memory_mb"`
		MinFreeMemoryPct         *float64 `json:"min_free_memory_pct"`
		MaxMemoryOvercommitRatio *float64 `json:"max_memory_overcommit_ratio"`
		HotMemoryThresholdPct    *float64 `json:"hot_memory_threshold_pct"`
		HotCPUThresholdPct       *float64 `json:"hot_cpu_threshold_pct"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&patch); err != nil {
		writeCPJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	var chkCPU, chkMem string
	if patch.HostCPU != nil {
		chkCPU = strings.TrimSpace(*patch.HostCPU)
	}
	if patch.HostMemory != nil {
		chkMem = strings.TrimSpace(*patch.HostMemory)
	}
	if err := ValidateCPUMemory(chkCPU, chkMem); err != nil {
		writeCPJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}

	c.mu.Lock()
	normalizeFleetConfig(&c.config)
	prevCPU := c.config.HostCPU
	prevMem := c.config.HostMemory
	if patch.MaxDensity != nil && *patch.MaxDensity > 0 {
		c.config.MaxDensity = *patch.MaxDensity
	}
	if chkCPU != "" {
		c.config.HostCPU = chkCPU
	}
	if chkMem != "" {
		c.config.HostMemory = chkMem
	}
	if patch.TargetRevision != nil {
		c.config.TargetRevision = strings.TrimSpace(*patch.TargetRevision)
	}
	if patch.PoolServiceURL != nil && *patch.PoolServiceURL != "" {
		c.config.PoolServiceURL = strings.TrimRight(*patch.PoolServiceURL, "/")
	}
	if patch.ControlPlaneURL != nil && *patch.ControlPlaneURL != "" {
		c.config.ControlPlaneURL = strings.TrimRight(*patch.ControlPlaneURL, "/")
	}
	if patch.UIServiceURL != nil && *patch.UIServiceURL != "" {
		c.config.UIServiceURL = strings.TrimRight(*patch.UIServiceURL, "/")
	}
	if patch.MinClientVersion != nil && strings.TrimSpace(*patch.MinClientVersion) != "" {
		c.config.MinClientVersion = strings.TrimSpace(*patch.MinClientVersion)
	}
	if patch.PeriodicSyncSec != nil && *patch.PeriodicSyncSec > 0 {
		c.config.PeriodicSyncSec = *patch.PeriodicSyncSec
	}
	if patch.DefaultIdleTimeout != nil && *patch.DefaultIdleTimeout >= 0 {
		c.config.DefaultIdleTimeout = *patch.DefaultIdleTimeout
	}
	if patch.HeartbeatTimeoutSec != nil && *patch.HeartbeatTimeoutSec > 0 {
		c.config.HeartbeatTimeoutSec = *patch.HeartbeatTimeoutSec
	}
	if patch.GracePeriodSec != nil && *patch.GracePeriodSec > 0 {
		c.config.GracePeriodSec = *patch.GracePeriodSec
	}
	if patch.AutoBinPack != nil {
		c.config.AutoBinPack = *patch.AutoBinPack
	}
	if patch.BinPackHysteresis != nil && *patch.BinPackHysteresis >= 0 {
		c.config.BinPackHysteresis = *patch.BinPackHysteresis
	}
	if patch.MinFreeMemoryMB != nil && *patch.MinFreeMemoryMB >= 0 {
		c.config.MinFreeMemoryMB = *patch.MinFreeMemoryMB
	}
	if patch.MinFreeMemoryPct != nil && *patch.MinFreeMemoryPct >= 0 {
		c.config.MinFreeMemoryPct = *patch.MinFreeMemoryPct
	}
	if patch.MaxMemoryOvercommitRatio != nil && *patch.MaxMemoryOvercommitRatio > 0 {
		c.config.MaxMemoryOvercommitRatio = *patch.MaxMemoryOvercommitRatio
	}
	if patch.HotMemoryThresholdPct != nil && *patch.HotMemoryThresholdPct > 0 {
		c.config.HotMemoryThresholdPct = *patch.HotMemoryThresholdPct
	}
	if patch.HotCPUThresholdPct != nil && *patch.HotCPUThresholdPct > 0 {
		c.config.HotCPUThresholdPct = *patch.HotCPUThresholdPct
	}
	normalizeFleetConfig(&c.config)
	cpuChanged := chkCPU != "" && !strings.EqualFold(c.config.HostCPU, prevCPU)
	memChanged := chkMem != "" && !strings.EqualFold(c.config.HostMemory, prevMem)
	c.config.PoolRolloutTriggered = cpuChanged || memChanged
	c.config.UpdatedAt = time.Now().UTC()
	cfg := c.config
	c.reconcileHostSandboxMapsLocked()
	c.mu.Unlock()

	if c.store != nil {
		_, _ = c.store.WriteJSONCAS(r.Context(), "fleet/config.json", cfg, -1)
	}
	if cpuChanged || memChanged {
		proj := ""
		if patch.Project != nil {
			proj = strings.TrimSpace(*patch.Project)
		}
		reg := ""
		if patch.Region != nil {
			reg = strings.TrimSpace(*patch.Region)
		}
		go func(p, rg, hCPU, hMem string) {
			bgCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			_ = c.reconcilePoolServiceSizing(bgCtx, p, rg, hCPU, hMem)
		}(proj, reg, cfg.HostCPU, cfg.HostMemory)
	}
	if patch.TargetRevision != nil && cfg.TargetRevision != "" {
		go func() {
			bgCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_ = c.runRolloutAndBinPackPass(bgCtx)
		}()
	}
	writeCPJSON(w, http.StatusOK, cfg)
}

func (c *ControlPlaneServer) handleListTemplates(w http.ResponseWriter, r *http.Request) {
	tplMap := BuiltInTemplates()
	if c.store != nil {
		if keys, err := c.store.ListObjects(r.Context(), "templates/"); err == nil {
			for _, k := range keys {
				if !strings.HasSuffix(k, ".json") {
					continue
				}
				var spec TemplateSpec
				if _, ok, rErr := c.store.ReadJSON(r.Context(), k, &spec); rErr == nil && ok && spec.Name != "" {
					tplMap[spec.Name] = spec
				}
			}
		}
	}
	list := make([]TemplateSpec, 0, len(tplMap))
	for _, t := range tplMap {
		list = append(list, t)
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
	})
	writeCPJSON(w, http.StatusOK, map[string]any{"templates": list})
}

func (c *ControlPlaneServer) handleCreateTemplate(w http.ResponseWriter, r *http.Request) {
	var spec TemplateSpec
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&spec); err != nil {
		writeCPJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	force := spec.Force || r.URL.Query().Get("force") == "true"
	if err := ValidateID(spec.Name); err != nil {
		writeCPJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	builtIns := BuiltInTemplates()
	if _, isBuiltIn := builtIns[strings.ToLower(spec.Name)]; isBuiltIn {
		writeCPJSON(w, http.StatusConflict, map[string]any{
			"error": fmt.Sprintf("cannot override built-in template %q", spec.Name),
		})
		return
	}
	if !force && c.store != nil {
		var existing TemplateSpec
		if _, ok, _ := c.store.ReadJSON(r.Context(), "templates/"+spec.Name+".json", &existing); ok && existing.Name != "" {
			writeCPJSON(w, http.StatusConflict, map[string]any{
				"error":    "template_already_exists",
				"message":  fmt.Sprintf("template %q already exists (use --force to overwrite)", spec.Name),
				"template": spec.Name,
			})
			return
		}
	}

	// Server-side orchestration when `from_sandbox` is provided
	if spec.SnapshotID == "" && strings.TrimSpace(spec.FromSandbox) != "" {
		srcIDOrName := strings.TrimSpace(spec.FromSandbox)
		srcRec, found, ambErr := c.resolveSandboxByIDOrName(r.Context(), srcIDOrName)
		if ambErr != nil {
			writeCPJSON(w, http.StatusConflict, map[string]any{"error": ambErr.Error()})
			return
		}
		if !found {
			writeCPJSON(w, http.StatusNotFound, map[string]any{"error": fmt.Sprintf("source sandbox %q not found", srcIDOrName)})
			return
		}
		autoSnapID := "snap-tpl-" + strings.TrimPrefix(spec.Name, "tpl-")
		c.mu.RLock()
		poolURL := c.config.PoolServiceURL
		c.mu.RUnlock()

		var snap SnapshotRecord
		snapCreated := false
		if poolURL != "" && srcRec.HostID != "" && srcRec.Status == StatusRunning {
			status, sErr := c.doPoolJSON(r.Context(), http.MethodPost, poolURL+"/__session/snapshot", srcRec.HostID, map[string]any{
				"sandbox_id":  srcRec.SandboxID,
				"snapshot_id": autoSnapID,
				"force":       true,
			}, &snap)
			if sErr == nil && status == http.StatusOK && snap.SnapshotID != "" {
				snapCreated = true
			}
		}
		if !snapCreated && c.store != nil && srcRec.OverlayPath != "" {
			if rc, ok, rErr := c.store.ReadBlob(r.Context(), srcRec.OverlayPath); rErr == nil && ok {
				tarPath := fmt.Sprintf("snapshots/%s.tar", autoSnapID)
				sz, wErr := c.store.WriteBlob(r.Context(), tarPath, rc)
				_ = rc.Close()
				if wErr == nil {
					snap = SnapshotRecord{
						SnapshotID:    autoSnapID,
						SourceSandbox: srcRec.SandboxID,
						Template:      srcRec.Template,
						TarPath:       tarPath,
						SizeBytes:     sz,
						CreatedAt:     time.Now().UTC(),
					}
					_, _ = c.store.WriteJSONCAS(r.Context(), "snapshots/"+autoSnapID+".json", snap, -1)
					snapCreated = true
				}
			}
		}
		if !snapCreated {
			writeCPJSON(w, http.StatusBadGateway, map[string]any{
				"error": fmt.Sprintf("failed to snapshot source sandbox %s on host %s", srcRec.SandboxID, srcRec.HostID),
			})
			return
		}
		spec.SnapshotID = snap.SnapshotID
		if (spec.BaseTemplate == "" || spec.BaseTemplate == "ubuntu") && srcRec.Template != "" {
			spec.BaseTemplate = srcRec.Template
		}
	}

	if spec.BaseTemplate == "" {
		spec.BaseTemplate = "ubuntu"
	}
	if spec.Image == "" {
		if bSpec, ok := builtIns[strings.ToLower(spec.BaseTemplate)]; ok && bSpec.Image != "" {
			spec.Image = bSpec.Image
		} else {
			spec.Image = "ubuntu:24.04"
		}
	}
	if spec.Type == "" {
		if spec.SnapshotID != "" {
			spec.Type = "snapshot"
		} else {
			spec.Type = "prewarmed"
		}
	}
	if spec.SnapshotID != "" && spec.OverlayPath == "" {
		spec.OverlayPath = fmt.Sprintf("snapshots/%s.tar", spec.SnapshotID)
	}
	spec.FromSandbox = ""
	spec.Force = false
	spec.CreatedAt = time.Now().UTC()
	if c.store != nil {
		_, _ = c.store.WriteJSONCAS(r.Context(), "templates/"+spec.Name+".json", spec, -1)
	}
	writeCPJSON(w, http.StatusOK, spec)
}

func (c *ControlPlaneServer) handleDeleteTemplate(w http.ResponseWriter, r *http.Request, name string) {
	if err := ValidateID(name); err != nil {
		writeCPJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	builtIns := BuiltInTemplates()
	if _, isBuiltIn := builtIns[strings.ToLower(name)]; isBuiltIn {
		writeCPJSON(w, http.StatusConflict, map[string]any{
			"error": fmt.Sprintf("cannot delete built-in template %q", name),
		})
		return
	}
	if c.store != nil {
		var existing TemplateSpec
		if _, ok, _ := c.store.ReadJSON(r.Context(), "templates/"+name+".json", &existing); !ok || existing.Name == "" {
			writeCPJSON(w, http.StatusNotFound, map[string]any{
				"error":    fmt.Sprintf("template %q not found", name),
				"template": name,
			})
			return
		}
		_ = c.store.DeleteObject(r.Context(), "templates/"+name+".json")
		if existing.SnapshotID == "snap-tpl-"+name {
			_ = c.store.DeleteObject(r.Context(), "snapshots/"+existing.SnapshotID+".json")
			_ = c.store.DeleteObject(r.Context(), "snapshots/"+existing.SnapshotID+".tar")
		}
	}
	writeCPJSON(w, http.StatusOK, map[string]any{"deleted": name})
}

func (c *ControlPlaneServer) handleListSnapshots(w http.ResponseWriter, r *http.Request) {
	list := make([]SnapshotRecord, 0)
	if c.store != nil {
		if keys, err := c.store.ListObjects(r.Context(), "snapshots/"); err == nil {
			for _, k := range keys {
				if !strings.HasSuffix(k, ".json") {
					continue
				}
				var snap SnapshotRecord
				if _, ok, rErr := c.store.ReadJSON(r.Context(), k, &snap); rErr == nil && ok && snap.SnapshotID != "" {
					list = append(list, snap)
				}
			}
		}
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].SnapshotID < list[j].SnapshotID
	})
	writeCPJSON(w, http.StatusOK, map[string]any{"snapshots": list})
}

func (c *ControlPlaneServer) handleDeleteSnapshot(w http.ResponseWriter, r *http.Request, snapID string) {
	if err := ValidateID(snapID); err != nil {
		writeCPJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if c.store != nil {
		var existing SnapshotRecord
		if _, ok, _ := c.store.ReadJSON(r.Context(), "snapshots/"+snapID+".json", &existing); !ok || existing.SnapshotID == "" {
			writeCPJSON(w, http.StatusNotFound, map[string]any{
				"error":       fmt.Sprintf("snapshot %q not found", snapID),
				"snapshot_id": snapID,
			})
			return
		}
		_ = c.store.DeleteObject(r.Context(), "snapshots/"+snapID+".json")
		_ = c.store.DeleteObject(r.Context(), "snapshots/"+snapID+".tar")
	}
	writeCPJSON(w, http.StatusOK, map[string]any{"deleted": snapID})
}

func (c *ControlPlaneServer) doPoolJSON(ctx context.Context, method, rawURL, hostID string, reqBody any, respDest any) (int, error) {
	var bodyReader io.Reader
	if reqBody != nil {
		b, err := json.Marshal(reqBody)
		if err != nil {
			return 0, err
		}
		bodyReader = bytes.NewReader(b)
	}
	reqCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, method, rawURL, bodyReader)
	if err != nil {
		return 0, err
	}
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("X-Sbx-Client-Version", DispenserVersion)
	req.Header.Set("X-Sbx-Client-Type", "sbx-controld/go")
	if hostID != "" {
		req.Header.Set("X-Serverless-Session-Id", hostID)
	}
	if strings.HasPrefix(rawURL, "https://") {
		if tok, err := GetIDToken(reqCtx, rawURL); err == nil && tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	rawResp, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if resp.StatusCode == http.StatusOK && respDest != nil && len(rawResp) > 0 {
		if err := json.Unmarshal(rawResp, respDest); err != nil {
			return resp.StatusCode, err
		}
	}
	if resp.StatusCode >= 400 {
		return resp.StatusCode, fmt.Errorf("HTTP %d from %s: %s", resp.StatusCode, rawURL, strings.TrimSpace(string(rawResp)))
	}
	return resp.StatusCode, nil
}

func (c *ControlPlaneServer) reconcilePoolServiceSizing(ctx context.Context, project, region, hostCPU, hostMem string) error {
	c.mu.RLock()
	poolURL := c.config.PoolServiceURL
	c.mu.RUnlock()
	if !strings.HasPrefix(poolURL, "https://") {
		return nil
	}
	if project == "" {
		project = DefaultGCPProject()
	}
	if region == "" {
		region = strings.TrimSpace(os.Getenv("GOOGLE_CLOUD_REGION"))
	}
	if region == "" {
		region = "us-west1"
	}
	// Try Cloud Run v2 REST API first via server-side OAuth access token
	if tok, err := DefaultAccessToken(ctx); err == nil && tok != "" {
		svcURL := fmt.Sprintf("https://run.googleapis.com/v2/projects/%s/locations/%s/services/sandbox-pool", url.PathEscape(project), url.PathEscape(region))
		getReq, gErr := http.NewRequestWithContext(ctx, http.MethodGet, svcURL, nil)
		if gErr == nil {
			getReq.Header.Set("Authorization", "Bearer "+tok)
			if resp, dErr := c.httpClient.Do(getReq); dErr == nil {
				var svc map[string]any
				_ = json.NewDecoder(resp.Body).Decode(&svc)
				resp.Body.Close()
				if tpl, ok := svc["template"].(map[string]any); ok {
					if containers, ok := tpl["containers"].([]any); ok && len(containers) > 0 {
						if c0, ok := containers[0].(map[string]any); ok {
							resMap, _ := c0["resources"].(map[string]any)
							if resMap == nil {
								resMap = map[string]any{}
							}
							limits, _ := resMap["limits"].(map[string]any)
							if limits == nil {
								limits = map[string]any{}
							}
							if hostCPU != "" {
								limits["cpu"] = hostCPU
							}
							if hostMem != "" {
								limits["memory"] = hostMem
							}
							resMap["limits"] = limits
							c0["resources"] = resMap
						}
					}
					b, _ := json.Marshal(svc)
					patchReq, pErr := http.NewRequestWithContext(ctx, http.MethodPatch, svcURL+"?updateMask=template.containers", bytes.NewReader(b))
					if pErr == nil {
						patchReq.Header.Set("Authorization", "Bearer "+tok)
						patchReq.Header.Set("Content-Type", "application/json")
						if pResp, pDoErr := c.httpClient.Do(patchReq); pDoErr == nil {
							pResp.Body.Close()
							if pResp.StatusCode >= 200 && pResp.StatusCode < 300 {
								return nil
							}
						}
					}
				}
			}
		}
	}
	if _, err := exec.LookPath("gcloud"); err == nil {
		args := []string{"run", "services", "update", "sandbox-pool", "--project=" + project, "--region=" + region, "--quiet"}
		if hostCPU != "" {
			args = append(args, "--cpu="+hostCPU)
		}
		if hostMem != "" {
			args = append(args, "--memory="+hostMem)
		}
		args = append(args, fmt.Sprintf("--update-env-vars=HOST_CPU=%s,HOST_MEMORY=%s", hostCPU, hostMem))
		return exec.CommandContext(ctx, "gcloud", args...).Run()
	}
	return nil
}

func (c *ControlPlaneServer) handleDeployFleet(w http.ResponseWriter, r *http.Request) {
	var req DeployFleetRequest
	if r.Body != nil {
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			writeCPJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
	}
	if req.Project == "" {
		req.Project = DefaultGCPProject()
	}
	if req.Region == "" {
		req.Region = strings.TrimSpace(os.Getenv("GOOGLE_CLOUD_REGION"))
	}
	if req.Region == "" {
		req.Region = "us-west1"
	}
	if req.Bucket == "" {
		req.Bucket = strings.TrimSpace(os.Getenv("GCS_BUCKET_NAME"))
	}
	if req.Bucket == "" {
		req.Bucket = req.Project + "-sandbox-dispenser"
	}
	if req.HostCPU == "" {
		req.HostCPU = "4"
	}
	if req.HostMemory == "" {
		req.HostMemory = "8Gi"
	}
	if err := ValidateCPUMemory(req.HostCPU, req.HostMemory); err != nil {
		writeCPJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if req.MaxDensity <= 0 {
		req.MaxDensity = 10
	}
	if req.Image == "" {
		req.Image = fmt.Sprintf("%s-docker.pkg.dev/%s/sandbox-dispenser/dispenser:latest", req.Region, req.Project)
	}
	deployUI := true
	if req.DeployUI != nil {
		deployUI = *req.DeployUI
	}

	c.mu.RLock()
	cpURL := c.config.ControlPlaneURL
	poolURL := c.config.PoolServiceURL
	uiURL := c.config.UIServiceURL
	c.mu.RUnlock()

	if cpURL == "" && r.Host != "" {
		scheme := "http"
		if r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https") {
			scheme = "https"
		}
		cpURL = scheme + "://" + r.Host
	}

	// If running against GCP Cloud Run (https://), reconcile sandbox-pool and sandbox-ui via Cloud Run v2 REST API
	if strings.HasPrefix(cpURL, "https://") {
		if tok, err := DefaultAccessToken(r.Context()); err == nil && tok != "" {
			if u := c.ensureCloudRunV2Service(r.Context(), tok, req.Project, req.Region, "sandbox-pool", map[string]any{
				"DISPENSER_MODE":       "hostd",
				"GOOGLE_CLOUD_PROJECT": req.Project,
				"GCS_BUCKET_NAME":      req.Bucket,
				"GCS_MOUNT_ROOT":       "/mnt/gcs",
				"CONTROL_PLANE_URL":    cpURL,
				"POOL_SERVICE_URL":     poolURL,
				"MAX_DENSITY":          strconv.Itoa(req.MaxDensity),
				"HOST_CPU":             req.HostCPU,
				"HOST_MEMORY":          req.HostMemory,
			}, req.Image, req.HostCPU, req.HostMemory); u != "" {
				poolURL = u
			}
			if deployUI {
				if u := c.ensureCloudRunV2Service(r.Context(), tok, req.Project, req.Region, "sandbox-ui", map[string]any{
					"DISPENSER_MODE":       "ui",
					"GOOGLE_CLOUD_PROJECT": req.Project,
					"GCS_BUCKET_NAME":      req.Bucket,
					"CONTROL_PLANE_URL":    cpURL,
					"POOL_SERVICE_URL":     poolURL,
				}, req.Image, "1", "1Gi"); u != "" {
					uiURL = u
				}
			}
		}
	}
	if deployUI && uiURL == "" && strings.Contains(cpURL, "sandbox-control-plane") {
		uiURL = strings.Replace(cpURL, "sandbox-control-plane", "sandbox-ui", 1)
	}

	callerEmail := ExtractCallerEmail(r, "")
	callerMember := NormalizeIAMMember(callerEmail)

	c.mu.Lock()
	c.config.MaxDensity = req.MaxDensity
	c.config.HostCPU = req.HostCPU
	c.config.HostMemory = req.HostMemory
	if cpURL != "" {
		c.config.ControlPlaneURL = cpURL
	}
	if poolURL != "" {
		c.config.PoolServiceURL = poolURL
	}
	if uiURL != "" {
		c.config.UIServiceURL = uiURL
	}
	normalizeFleetConfig(&c.config)
	c.config.UpdatedAt = time.Now().UTC()
	cfg := c.config

	if len(c.securityPolicy.UIAccessMembers) == 0 && len(c.securityPolicy.BackendAccessMembers) == 0 {
		c.securityPolicy = defaultSecurityPolicy()
	}
	c.securityPolicy.IAPEnabled = true
	if callerEmail != "" {
		c.securityPolicy.DeployerEmail = callerEmail
		c.securityPolicy.UpdatedBy = callerEmail
	}
	if callerMember != "" {
		c.securityPolicy.UIAccessMembers = dedupeMembers(append(c.securityPolicy.UIAccessMembers, callerMember))
		c.securityPolicy.BackendAccessMembers = dedupeMembers(append(c.securityPolicy.BackendAccessMembers, callerMember))
	}
	if runtimeSA, _ := detectRuntimeServiceAccounts(); runtimeSA != "" {
		c.securityPolicy.BackendAccessMembers = dedupeMembers(append(c.securityPolicy.BackendAccessMembers, runtimeSA))
	}
	c.securityPolicy.UpdatedAt = time.Now().UTC()
	secPol := c.securityPolicy
	c.mu.Unlock()

	if c.store != nil {
		_, _ = c.store.WriteJSONCAS(r.Context(), "fleet/config.json", cfg, -1)
		_, _ = c.store.WriteJSONCAS(r.Context(), "fleet/security-policy.json", secPol, -1)
	}

	writeCPJSON(w, http.StatusOK, DeployFleetResponse{
		Status:          "DEPLOYED",
		Project:         req.Project,
		Region:          req.Region,
		Bucket:          req.Bucket,
		Image:           req.Image,
		ControlPlaneURL: cfg.ControlPlaneURL,
		PoolServiceURL:  cfg.PoolServiceURL,
		UIServiceURL:    cfg.UIServiceURL,
		HostCPU:         cfg.HostCPU,
		HostMemory:      cfg.HostMemory,
		MaxDensity:      cfg.MaxDensity,
		ServerVersion:   DispenserVersion,
	})
}

func (c *ControlPlaneServer) ensureCloudRunV2Service(ctx context.Context, tok, project, region, serviceName string, envMap map[string]any, image, cpu, memory string) string {
	svcURL := fmt.Sprintf("https://run.googleapis.com/v2/projects/%s/locations/%s/services/%s",
		url.PathEscape(project), url.PathEscape(region), url.PathEscape(serviceName))
	getReq, err := http.NewRequestWithContext(ctx, http.MethodGet, svcURL, nil)
	if err != nil {
		return ""
	}
	getReq.Header.Set("Authorization", "Bearer "+tok)
	resp, err := c.httpClient.Do(getReq)
	if err != nil {
		return ""
	}
	var existing map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&existing)
	resp.Body.Close()
	if uri, ok := existing["uri"].(string); ok && uri != "" {
		// Also update sizing if cpu/memory changed
		if serviceName == "sandbox-pool" {
			_ = c.reconcilePoolServiceSizing(ctx, project, region, cpu, memory)
		}
		if serviceName == "sandbox-ui" {
			existing["iapEnabled"] = true
			ann, _ := existing["annotations"].(map[string]any)
			if ann == nil {
				ann = map[string]any{}
			}
			ann["run.googleapis.com/iap-enabled"] = "true"
			existing["annotations"] = ann
		}
		return strings.TrimRight(uri, "/")
	}
	_ = envMap
	_ = image
	return ""
}

func (c *ControlPlaneServer) handleGetMetrics(w http.ResponseWriter, r *http.Request) {
	if c.store != nil {
		_ = c.SyncFromStore(r.Context())
	}
	hostFilter := strings.TrimSpace(r.URL.Query().Get("host"))
	sbxFilter := strings.TrimSpace(r.URL.Query().Get("sandbox"))
	includeHistory := r.URL.Query().Get("history") == "true" || r.URL.Query().Get("history") == "1"

	summary := c.BuildFleetSummary()
	hostsOut := make(map[string]HostRecord)
	for hid, hr := range summary.Hosts {
		if hostFilter != "" && hid != hostFilter {
			continue
		}
		hostsOut[hid] = hr
	}

	sbxOut := make(map[string]SandboxRecord)
	for sid, sr := range summary.Sandboxes {
		if hostFilter != "" && sr.HostID != hostFilter {
			continue
		}
		if sbxFilter != "" && sid != sbxFilter && sr.Name != sbxFilter {
			continue
		}
		sbxOut[sid] = sr
	}

	historyOut := make(map[string][]HostResourceSample)
	if includeHistory {
		for hid, hr := range hostsOut {
			var hist HostMetricsHistory
			if c.store != nil {
				_, _, _ = c.store.ReadJSON(r.Context(), "metrics/hosts/"+hid+".json", &hist)
			}
			if len(hist.Samples) == 0 {
				hist.Samples = []HostResourceSample{{
					Timestamp:              time.Now().UTC(),
					ActiveSandboxes:        hr.ActiveSandboxes,
					UsedMemoryBytes:        hr.UsedMemoryBytes,
					AvailableMemoryBytes:   hr.AvailableMemoryBytes,
					TotalMemoryBytes:       hr.TotalMemoryBytes,
					UsedMemoryPct:          hr.UsedMemoryPct,
					AllocatedMemoryBytes:   hr.AllocatedMemoryBytes,
					MemoryOvercommitRatio:  hr.MemoryOvercommitRatio,
					AllocatedCPUMillicores: hr.AllocatedCPUMillicores,
					CPUOvercommitRatio:     hr.CPUOvercommitRatio,
					CPUUsagePct:            hr.CPUUsagePct,
					OverloadState:          hr.OverloadState,
				}}
			}
			historyOut[hid] = hist.Samples
		}
	}

	var recs []string
	maxMemRatio := 0.0
	maxUsedMemPct := 0.0
	for _, hr := range hostsOut {
		if hr.MemoryOvercommitRatio > maxMemRatio {
			maxMemRatio = hr.MemoryOvercommitRatio
		}
		if hr.UsedMemoryPct > maxUsedMemPct {
			maxUsedMemPct = hr.UsedMemoryPct
		}
		if hr.OverloadState == "ADMISSION_BLOCKED" || hr.OverloadState == "HOT_SHEDDING" {
			recs = append(recs, fmt.Sprintf("Host %s is in %s state (used_mem=%.1f%%, overcommit=%.2fx): consider increasing --host-memory or lowering --max-density", hr.HostID, hr.OverloadState, hr.UsedMemoryPct, hr.MemoryOvercommitRatio))
		}
	}
	if maxMemRatio > 1.5 && maxUsedMemPct < 50.0 {
		recs = append(recs, fmt.Sprintf("High nominal memory overcommit (%.2fx) with low actual RSS usage (%.1f%%): current overcommit ratio is safe for sparse/idle sandboxes", maxMemRatio, maxUsedMemPct))
	} else if maxMemRatio > 1.0 {
		recs = append(recs, fmt.Sprintf("Fleet memory overcommit is %.2fx (peak host RSS %.1f%%): monitor hot-host shedding thresholds (--hot-memory-threshold-pct=%.1f)", summary.FleetMemoryOvercommitRatio, maxUsedMemPct, summary.Config.HotMemoryThresholdPct))
	} else {
		recs = append(recs, fmt.Sprintf("Fleet resource utilization is healthy (memory overcommit=%.2fx, CPU overcommit=%.2fx, max_density=%d)", summary.FleetMemoryOvercommitRatio, summary.FleetCPUOvercommitRatio, summary.Config.MaxDensity))
	}

	writeCPJSON(w, http.StatusOK, map[string]any{
		"fleet_memory_overcommit_ratio": summary.FleetMemoryOvercommitRatio,
		"fleet_cpu_overcommit_ratio":    summary.FleetCPUOvercommitRatio,
		"total_hot_migrations":          summary.TotalHotMigrations,
		"total_memory_rejections":       summary.TotalMemoryRejections,
		"hosts":                         hostsOut,
		"sandboxes":                     sbxOut,
		"history":                       historyOut,
		"recommendations":               recs,
		"updated_at":                    summary.UpdatedAt,
	})
}

func isProtectedSystemMember(member string) bool {
	m := strings.ToLower(strings.TrimSpace(member))
	runtimeSA, iapSA := detectRuntimeServiceAccounts()
	if m == strings.ToLower(runtimeSA) || m == strings.ToLower(iapSA) {
		return true
	}
	return strings.Contains(m, "-compute@developer.gserviceaccount.com") ||
		strings.Contains(m, "@gcp-sa-iap.iam.gserviceaccount.com")
}

func dedupeMembers(list []string) []string {
	seen := make(map[string]bool, len(list))
	out := make([]string, 0, len(list))
	for _, raw := range list {
		norm := NormalizeIAMMember(raw)
		if norm == "" || seen[norm] {
			continue
		}
		seen[norm] = true
		out = append(out, norm)
	}
	return out
}

func containsMember(list []string, target string) bool {
	normTarget := NormalizeIAMMember(target)
	for _, m := range list {
		if NormalizeIAMMember(m) == normTarget {
			return true
		}
	}
	return false
}

func (c *ControlPlaneServer) snapshotSecurityPolicyForCaller(r *http.Request) SecurityPolicy {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.securityPolicy.UIAccessMembers) == 0 && len(c.securityPolicy.BackendAccessMembers) == 0 {
		c.securityPolicy = defaultSecurityPolicy()
	}
	runtimeSA, iapSA := detectRuntimeServiceAccounts()
	callerEmail := ExtractCallerEmail(r, c.securityPolicy.DeployerEmail)
	callerMember := NormalizeIAMMember(callerEmail)
	if !containsMember(c.securityPolicy.UIAccessMembers, callerMember) && len(c.securityPolicy.UIAccessMembers) == 0 {
		c.securityPolicy.UIAccessMembers = append(c.securityPolicy.UIAccessMembers, callerMember)
	}
	if !containsMember(c.securityPolicy.BackendAccessMembers, callerMember) && len(c.securityPolicy.BackendAccessMembers) == 0 {
		c.securityPolicy.BackendAccessMembers = append(c.securityPolicy.BackendAccessMembers, callerMember)
	}
	if runtimeSA != "" && !containsMember(c.securityPolicy.BackendAccessMembers, runtimeSA) {
		c.securityPolicy.BackendAccessMembers = append(c.securityPolicy.BackendAccessMembers, runtimeSA)
	}
	pol := c.securityPolicy
	pol.UIAccessMembers = dedupeMembers(pol.UIAccessMembers)
	pol.BackendAccessMembers = dedupeMembers(pol.BackendAccessMembers)
	pol.CurrentUser = callerEmail
	pol.CurrentMember = callerMember
	protected := []string{callerMember, runtimeSA, iapSA}
	for _, m := range pol.BackendAccessMembers {
		if isProtectedSystemMember(m) {
			protected = append(protected, m)
		}
	}
	pol.ProtectedMembers = dedupeMembers(protected)
	return pol
}

func (c *ControlPlaneServer) handleGetSecurityPolicy(w http.ResponseWriter, r *http.Request) {
	if c.store != nil {
		var stored SecurityPolicy
		if _, ok, err := c.store.ReadJSON(r.Context(), "fleet/security-policy.json", &stored); err == nil && ok && len(stored.UIAccessMembers) > 0 {
			c.mu.Lock()
			c.securityPolicy = stored
			c.mu.Unlock()
		}
	}
	pol := c.snapshotSecurityPolicyForCaller(r)
	writeCPJSON(w, http.StatusOK, pol)
}

func (c *ControlPlaneServer) handleUpdateSecurityPolicy(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Action               string   `json:"action"`
		Member               string   `json:"member"`
		IAPEnabled           *bool    `json:"iap_enabled"`
		UIAccessMembers      []string `json:"ui_access_members"`
		BackendAccessMembers []string `json:"backend_access_members"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeCPJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid JSON body: " + err.Error()})
		return
	}

	curPol := c.snapshotSecurityPolicyForCaller(r)
	callerEmail := curPol.CurrentUser
	callerMember := curPol.CurrentMember

	uiMembers := append([]string{}, curPol.UIAccessMembers...)
	backendMembers := append([]string{}, curPol.BackendAccessMembers...)
	iapEnabled := curPol.IAPEnabled
	if req.IAPEnabled != nil {
		iapEnabled = *req.IAPEnabled
	}

	action := strings.ToLower(strings.TrimSpace(req.Action))
	normMember := NormalizeIAMMember(req.Member)

	var iamOp string
	switch action {
	case "add_ui_member":
		if normMember == "" {
			writeCPJSON(w, http.StatusBadRequest, map[string]any{"error": "member is required"})
			return
		}
		uiMembers = dedupeMembers(append(uiMembers, normMember))
		iamOp = "add_ui"
	case "remove_ui_member":
		if normMember == "" {
			writeCPJSON(w, http.StatusBadRequest, map[string]any{"error": "member is required"})
			return
		}
		if normMember == callerMember {
			writeCPJSON(w, http.StatusBadRequest, map[string]any{
				"error":   "self_revocation_forbidden",
				"caller":  callerMember,
				"message": fmt.Sprintf("Safeguard blocked: you cannot revoke your own UI access (%s).", callerMember),
			})
			return
		}
		var filtered []string
		for _, m := range uiMembers {
			if NormalizeIAMMember(m) != normMember {
				filtered = append(filtered, m)
			}
		}
		uiMembers = dedupeMembers(filtered)
		iamOp = "remove_ui"
	case "add_backend_member":
		if normMember == "" {
			writeCPJSON(w, http.StatusBadRequest, map[string]any{"error": "member is required"})
			return
		}
		backendMembers = dedupeMembers(append(backendMembers, normMember))
		iamOp = "add_backend"
	case "remove_backend_member":
		if normMember == "" {
			writeCPJSON(w, http.StatusBadRequest, map[string]any{"error": "member is required"})
			return
		}
		if normMember == callerMember || isProtectedSystemMember(normMember) {
			writeCPJSON(w, http.StatusBadRequest, map[string]any{
				"error":   "self_revocation_forbidden",
				"caller":  callerMember,
				"message": fmt.Sprintf("Safeguard blocked: you cannot revoke your own backend access (%s) or a protected system service account.", normMember),
			})
			return
		}
		var filtered []string
		for _, m := range backendMembers {
			if NormalizeIAMMember(m) != normMember {
				filtered = append(filtered, m)
			}
		}
		backendMembers = dedupeMembers(filtered)
		iamOp = "remove_backend"
	case "set_iap_enabled":
		// handled via req.IAPEnabled
	case "":
		if req.UIAccessMembers != nil {
			candUI := dedupeMembers(req.UIAccessMembers)
			if !containsMember(candUI, callerMember) {
				writeCPJSON(w, http.StatusBadRequest, map[string]any{
					"error":   "self_revocation_forbidden",
					"caller":  callerMember,
					"message": fmt.Sprintf("Safeguard blocked: ui_access_members must retain your own principal (%s).", callerMember),
				})
				return
			}
			uiMembers = candUI
		}
		if req.BackendAccessMembers != nil {
			candBackend := dedupeMembers(req.BackendAccessMembers)
			if !containsMember(candBackend, callerMember) {
				writeCPJSON(w, http.StatusBadRequest, map[string]any{
					"error":   "self_revocation_forbidden",
					"caller":  callerMember,
					"message": fmt.Sprintf("Safeguard blocked: backend_access_members must retain your own principal (%s).", callerMember),
				})
				return
			}
			for _, oldM := range backendMembers {
				if isProtectedSystemMember(oldM) && !containsMember(candBackend, oldM) {
					writeCPJSON(w, http.StatusBadRequest, map[string]any{
						"error":   "self_revocation_forbidden",
						"caller":  callerMember,
						"message": fmt.Sprintf("Safeguard blocked: cannot remove protected system service account (%s).", oldM),
					})
					return
				}
			}
			backendMembers = candBackend
		}
	default:
		writeCPJSON(w, http.StatusBadRequest, map[string]any{"error": "unsupported security policy action " + action})
		return
	}

	c.mu.Lock()
	c.securityPolicy.IAPEnabled = iapEnabled
	c.securityPolicy.UIAccessMembers = uiMembers
	c.securityPolicy.BackendAccessMembers = backendMembers
	if c.securityPolicy.DeployerEmail == "" {
		c.securityPolicy.DeployerEmail = callerEmail
	}
	c.securityPolicy.UpdatedAt = time.Now().UTC()
	c.securityPolicy.UpdatedBy = callerEmail
	c.recordEventLocked(ActivityEvent{
		Type:    "SECURITY_POLICY",
		Message: fmt.Sprintf("Updated security policy by %s (action=%s, member=%s, iap=%v)", callerEmail, action, normMember, iapEnabled),
	})
	savedPol := c.securityPolicy
	cpURL := c.config.ControlPlaneURL
	c.mu.Unlock()

	if c.store != nil {
		_, _ = c.store.WriteJSONCAS(r.Context(), "fleet/security-policy.json", savedPol, -1)
	}
	if strings.HasPrefix(cpURL, "https://") && iamOp != "" && normMember != "" {
		go func(op, member string) {
			bgCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			c.applyCloudIAMBindingChange(bgCtx, op, member)
		}(iamOp, normMember)
	}

	outPol := c.snapshotSecurityPolicyForCaller(r)
	writeCPJSON(w, http.StatusOK, outPol)
}

func (c *ControlPlaneServer) updateRESTIAMPolicyBinding(ctx context.Context, tok, resourceURL, role, member string, add bool) bool {
	getReq, err := http.NewRequestWithContext(ctx, http.MethodPost, resourceURL+":getIamPolicy", strings.NewReader("{}"))
	if err != nil {
		return false
	}
	getReq.Header.Set("Authorization", "Bearer "+tok)
	getReq.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(getReq)
	if err != nil {
		return false
	}
	var pol struct {
		Version  int    `json:"version,omitempty"`
		Etag     string `json:"etag,omitempty"`
		Bindings []struct {
			Role    string   `json:"role"`
			Members []string `json:"members"`
		} `json:"bindings"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&pol)
	resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false
	}

	foundRole := false
	for i := range pol.Bindings {
		if pol.Bindings[i].Role == role {
			foundRole = true
			if add {
				if !containsMember(pol.Bindings[i].Members, member) {
					pol.Bindings[i].Members = append(pol.Bindings[i].Members, member)
				}
			} else {
				var kept []string
				for _, m := range pol.Bindings[i].Members {
					if NormalizeIAMMember(m) != NormalizeIAMMember(member) {
						kept = append(kept, m)
					}
				}
				pol.Bindings[i].Members = kept
			}
		}
	}
	if add && !foundRole {
		pol.Bindings = append(pol.Bindings, struct {
			Role    string   `json:"role"`
			Members []string `json:"members"`
		}{
			Role:    role,
			Members: []string{member},
		})
	}
	b, err := json.Marshal(map[string]any{"policy": pol})
	if err != nil {
		return false
	}
	setReq, err := http.NewRequestWithContext(ctx, http.MethodPost, resourceURL+":setIamPolicy", bytes.NewReader(b))
	if err != nil {
		return false
	}
	setReq.Header.Set("Authorization", "Bearer "+tok)
	setReq.Header.Set("Content-Type", "application/json")
	setResp, err := c.httpClient.Do(setReq)
	if err != nil {
		return false
	}
	setResp.Body.Close()
	return setResp.StatusCode >= 200 && setResp.StatusCode < 300
}

func (c *ControlPlaneServer) applyCloudIAMBindingChange(ctx context.Context, op, member string) {
	project := DefaultGCPProject()
	projNum := strings.TrimSpace(os.Getenv("GOOGLE_CLOUD_PROJECT_NUMBER"))
	if projNum == "" {
		projNum = "000000000000"
	}
	region := strings.TrimSpace(os.Getenv("GOOGLE_CLOUD_REGION"))
	if region == "" {
		region = "us-west1"
	}

	if tok, err := DefaultAccessToken(ctx); err == nil && tok != "" {
		runBase := fmt.Sprintf("https://run.googleapis.com/v2/projects/%s/locations/%s/services/", url.PathEscape(project), url.PathEscape(region))
		iapRes := fmt.Sprintf("https://iap.googleapis.com/v1/projects/%s/iap_web/cloud_run-%s/services/sandbox-ui", url.PathEscape(projNum), url.PathEscape(region))
		switch op {
		case "add_ui":
			_ = c.updateRESTIAMPolicyBinding(ctx, tok, iapRes, "roles/iap.httpsResourceAccessor", member, true)
			_ = c.updateRESTIAMPolicyBinding(ctx, tok, runBase+"sandbox-ui", "roles/run.invoker", member, true)
			return
		case "remove_ui":
			_ = c.updateRESTIAMPolicyBinding(ctx, tok, iapRes, "roles/iap.httpsResourceAccessor", member, false)
			return
		case "add_backend":
			for _, svc := range []string{"sandbox-control-plane", "sandbox-pool"} {
				_ = c.updateRESTIAMPolicyBinding(ctx, tok, runBase+svc, "roles/run.invoker", member, true)
			}
			return
		case "remove_backend":
			for _, svc := range []string{"sandbox-control-plane", "sandbox-pool"} {
				_ = c.updateRESTIAMPolicyBinding(ctx, tok, runBase+svc, "roles/run.invoker", member, false)
			}
			return
		}
	}

	if _, err := exec.LookPath("gcloud"); err != nil {
		return
	}
	switch op {
	case "add_ui":
		_ = exec.CommandContext(ctx, "gcloud", "iap", "web", "add-iam-policy-binding",
			"--resource-type=cloud-run", "--service=sandbox-ui", "--region="+region, "--project="+project,
			"--member="+member, "--role=roles/iap.httpsResourceAccessor", "--quiet").Run()
		_ = exec.CommandContext(ctx, "gcloud", "run", "services", "add-iam-policy-binding", "sandbox-ui",
			"--region="+region, "--project="+project, "--member="+member, "--role=roles/run.invoker", "--quiet").Run()
	case "remove_ui":
		_ = exec.CommandContext(ctx, "gcloud", "iap", "web", "remove-iam-policy-binding",
			"--resource-type=cloud-run", "--service=sandbox-ui", "--region="+region, "--project="+project,
			"--member="+member, "--role=roles/iap.httpsResourceAccessor", "--quiet").Run()
	case "add_backend":
		for _, svc := range []string{"sandbox-control-plane", "sandbox-pool"} {
			_ = exec.CommandContext(ctx, "gcloud", "run", "services", "add-iam-policy-binding", svc,
				"--region="+region, "--project="+project, "--member="+member, "--role=roles/run.invoker", "--quiet").Run()
		}
	case "remove_backend":
		for _, svc := range []string{"sandbox-control-plane", "sandbox-pool"} {
			_ = exec.CommandContext(ctx, "gcloud", "run", "services", "remove-iam-policy-binding", svc,
				"--region="+region, "--project="+project, "--member="+member, "--role=roles/run.invoker", "--quiet").Run()
		}
	}
}
