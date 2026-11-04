package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// HostServer is the session-isolated Sandbox Pool Host Supervisor (`hostd`).
// Each Cloud Run session clone runs one HostServer managing up to MaxDensity
// Cloud Run Sandbox Runtime container sandboxes.
type HostServer struct {
	mu                     sync.RWMutex
	hostID                 string
	instanceID             string
	revision               string
	status                 HostStatus
	maxDensity             int
	cfgHostCPU             string
	cfgHostMemory          string
	minFreeMemoryMB        int
	minFreeMemoryPct       float64
	maxMemoryOvercommit    float64
	hotMemoryThresholdPct  float64
	hotCPUThresholdPct     float64
	peakMemoryPct          float64
	peakCPUUsagePct        float64
	rejectedCreationsMem   int
	hotMigrationsTriggered int
	metricsHistory         []HostResourceSample
	lastShedSandboxes      map[string]string
	shedMu                 sync.Mutex
	startedAt              time.Time
	lastHeartbeatAt        time.Time
	hadSandboxes           bool
	emptySince             time.Time
	retired                bool
	defaultIdleTimeoutSec  int
	activeConns            map[string]int
	sandboxes              map[string]SandboxRecord
	migratedTo             map[string]string
	dirtySandboxes         map[string]bool
	lastExportAt           map[string]time.Time
	sbxLocksMu             sync.Mutex
	sbxLocks               map[string]*sync.Mutex
	recentEvents           []ActivityEvent
	store                  Store
	engine                 SandboxEngine
	controlPlaneURL        string
	poolServiceURL         string
	minClientVersion       string
	httpClient             *http.Client
	stopCh                 chan struct{}
	stopOnce               sync.Once
}

// NewHostServer initializes a HostServer for the given host session ID.
// If hostID and HOST_SESSION_ID are empty, h.hostID remains empty until the first request
// binds it from X-Serverless-Session-Id (preventing phantom boot-time host records).
func NewHostServer(hostID string, store Store, engine SandboxEngine, maxDensity int, controlPlaneURL, poolServiceURL string) *HostServer {
	if hostID == "" {
		hostID = strings.TrimSpace(os.Getenv("HOST_SESSION_ID"))
	}
	if maxDensity <= 0 {
		if envMax, err := strconv.Atoi(os.Getenv("MAX_DENSITY")); err == nil && envMax > 0 {
			maxDensity = envMax
		} else {
			maxDensity = 10
		}
	}
	rev := os.Getenv("K_REVISION")
	if rev == "" {
		rev = "sandbox-pool-00001-local"
	}
	inst := detectHostInstanceID()
	h := &HostServer{
		hostID:                hostID,
		instanceID:            inst,
		revision:              rev,
		status:                HostActive,
		maxDensity:            maxDensity,
		minFreeMemoryMB:       512,
		minFreeMemoryPct:      15.0,
		maxMemoryOvercommit:   2.0,
		hotMemoryThresholdPct: 85.0,
		hotCPUThresholdPct:    90.0,
		lastShedSandboxes:     make(map[string]string),
		startedAt:             time.Now().UTC(),
		lastHeartbeatAt:       time.Now().UTC(),
		activeConns:           make(map[string]int),
		sandboxes:             make(map[string]SandboxRecord),
		migratedTo:            make(map[string]string),
		dirtySandboxes:        make(map[string]bool),
		lastExportAt:          make(map[string]time.Time),
		sbxLocks:              make(map[string]*sync.Mutex),
		store:                 store,
		engine:                engine,
		controlPlaneURL:       strings.TrimRight(controlPlaneURL, "/"),
		poolServiceURL:        strings.TrimRight(poolServiceURL, "/"),
		minClientVersion:      DefaultMinClientVersion,
		httpClient:            &http.Client{Timeout: 30 * time.Second},
		stopCh:                make(chan struct{}),
	}
	SetInstanceBusyState(false)
	return h
}

func (h *HostServer) getSandboxLock(sandboxID string) *sync.Mutex {
	h.sbxLocksMu.Lock()
	defer h.sbxLocksMu.Unlock()
	m, ok := h.sbxLocks[sandboxID]
	if !ok {
		m = &sync.Mutex{}
		h.sbxLocks[sandboxID] = m
	}
	return m
}

func (h *HostServer) markSandboxDirty(sandboxID string) {
	h.mu.Lock()
	h.dirtySandboxes[sandboxID] = true
	h.mu.Unlock()
}

func detectHostInstanceID() string {
	req, err := http.NewRequest(http.MethodGet, "http://metadata.google.internal/computeMetadata/v1/instance/id", nil)
	if err == nil {
		req.Header.Set("Metadata-Flavor", "Google")
		client := &http.Client{Timeout: 500 * time.Millisecond}
		if resp, err := client.Do(req); err == nil {
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 128))
			resp.Body.Close()
			if s := strings.TrimSpace(string(b)); s != "" {
				if len(s) > 16 {
					return s[len(s)-16:]
				}
				return s
			}
		}
	}
	if hn, err := os.Hostname(); err == nil && hn != "" {
		return hn
	}
	return NewRandomID("inst")
}

// StartBackgroundLoops starts the periodic heartbeat, persistence sync, in-guest state watcher,
// and Cloud Run busy-session keepalive loops.
func (h *HostServer) StartBackgroundLoops(ctx context.Context) {
	// Initial self-registration flush
	_ = h.SyncStateAndHeartbeat(ctx)

	go func() {
		hbTicker := time.NewTicker(4 * time.Second)
		syncTicker := time.NewTicker(1500 * time.Millisecond)
		guestTicker := time.NewTicker(400 * time.Millisecond)
		defer hbTicker.Stop()
		defer syncTicker.Stop()
		defer guestTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-h.stopCh:
				return
			case <-hbTicker.C:
				if h.IsCrashed() {
					continue
				}
				_ = h.SyncStateAndHeartbeat(ctx)
				_, _ = h.checkResourceOverloadAndShed(ctx)
			case <-syncTicker.C:
				if h.IsCrashed() {
					continue
				}
				h.syncPersistentSandboxes(ctx)
			case <-guestTicker.C:
				if h.IsCrashed() {
					continue
				}
				h.checkInGuestAndIdleTimeouts(ctx)
			}
		}
	}()

	// Cloud Run busy-instance keepalive: while ActiveSandboxes > 0 on Cloud Run, maintain a single
	// active request to /__internal/hold-busy so Cloud Run's autoscaler never sends premature idle SIGTERM!
	// The instant ActiveSandboxes drops to 0, /__internal/hold-busy returns and the clone scales to 0 cleanly.
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-h.stopCh:
				return
			case <-ticker.C:
				state := h.HostState()
				if state.HostID == "" || state.ActiveSandboxes <= 0 || state.Status != HostActive || h.IsCrashed() {
					continue
				}
				if !strings.HasPrefix(h.poolServiceURL, "https://") {
					continue
				}
				h.holdBusySelfPass(ctx, state.HostID)
			}
		}
	}()
}

func (h *HostServer) holdBusySelfPass(ctx context.Context, hostID string) {
	reqCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, h.poolServiceURL+"/__internal/hold-busy", nil)
	if err != nil {
		return
	}
	req.Header.Set("X-Serverless-Session-Id", hostID)
	if tok, err := GetIDToken(reqCtx, h.poolServiceURL); err == nil && tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	if resp, err := h.httpClient.Do(req); err == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

// Close stops background loops on the HostServer.
func (h *HostServer) Close() {
	h.stopOnce.Do(func() {
		close(h.stopCh)
	})
}

// IsCrashed returns true if the host has entered simulated hard-crash state.
func (h *HostServer) IsCrashed() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.status == HostCrashed || h.status == HostDead
}

func (h *HostServer) recordEventLocked(evtType, sandboxID, targetID, duration, msg string) {
	evt := ActivityEvent{
		EventID:   NewRandomID("evt"),
		Timestamp: time.Now().UTC(),
		Type:      evtType,
		SandboxID: sandboxID,
		HostID:    h.hostID,
		TargetID:  targetID,
		Duration:  duration,
		Message:   msg,
	}
	h.recentEvents = append(h.recentEvents, evt)
	if len(h.recentEvents) > 50 {
		h.recentEvents = h.recentEvents[len(h.recentEvents)-50:]
	}
}

func sumDirSizeBounded(root string, maxEntries int) int64 {
	var total int64
	count := 0
	_ = filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		count++
		if maxEntries > 0 && count > maxEntries {
			return filepath.SkipDir
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total
}

func measureProcTreeAndEnvForSandbox(sandboxID string, includeContainerDirs bool) int64 {
	if sandboxID == "" {
		return 0
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0
	}
	needle := "SANDBOX_ID=" + sandboxID
	pageSize := int64(os.Getpagesize())
	if pageSize <= 0 {
		pageSize = 4096
	}
	var totalRSS int64
	var firstPID string
	checked := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if len(name) == 0 || name[0] < '0' || name[0] > '9' {
			continue
		}
		checked++
		if checked > 256 {
			break
		}
		envBytes, err := os.ReadFile(filepath.Join("/proc", name, "environ"))
		if err != nil || len(envBytes) == 0 {
			continue
		}
		matched := false
		for _, kv := range bytes.Split(envBytes, []byte{0}) {
			if string(kv) == needle {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		if firstPID == "" {
			firstPID = name
		}
		if statmBytes, sErr := os.ReadFile(filepath.Join("/proc", name, "statm")); sErr == nil {
			fields := strings.Fields(string(statmBytes))
			if len(fields) >= 2 {
				if rssPages, pErr := strconv.ParseInt(fields[1], 10, 64); pErr == nil && rssPages > 0 {
					totalRSS += rssPages * pageSize
				}
			}
		}
	}
	var containerDirBytes int64
	if includeContainerDirs && firstPID != "" {
		for _, sub := range []string{"workspace", "persistent", "tmp"} {
			containerDirBytes += sumDirSizeBounded(filepath.Join("/proc", firstPID, "root", sub), 200)
		}
	}
	return totalRSS + containerDirBytes
}

func readCgroupMemoryCurrent(expectedTotalBytes int64) int64 {
	if os.Getenv("K_SERVICE") == "" {
		return 0
	}
	curB, err1 := os.ReadFile("/sys/fs/cgroup/memory.current")
	maxB, err2 := os.ReadFile("/sys/fs/cgroup/memory.max")
	if err1 != nil || err2 != nil {
		return 0
	}
	cur, e1 := strconv.ParseInt(strings.TrimSpace(string(curB)), 10, 64)
	lim, e2 := strconv.ParseInt(strings.TrimSpace(string(maxB)), 10, 64)
	if e1 != nil || e2 != nil || cur <= 0 || lim <= 0 {
		return 0
	}
	if expectedTotalBytes > 0 && (lim > expectedTotalBytes*2 || lim < expectedTotalBytes/2) {
		return 0
	}
	return cur
}

func (h *HostServer) measureSandboxUsage(rec SandboxRecord) (int64, float64) {
	var simMemBytes int64
	for _, rawVal := range []string{
		rec.Metadata["simulated_memory_mb"],
		rec.Env["SBX_SIMULATE_MEMORY_MB"],
	} {
		if s := strings.TrimSpace(rawVal); s != "" {
			if mb, err := strconv.ParseFloat(s, 64); err == nil && mb > 0 {
				simMemBytes = int64(mb * float64(1<<20))
				break
			}
		}
	}
	var measuredBytes int64
	if lp, ok := h.engine.(*LocalProcessEngine); ok {
		if sbxRoot, err := lp.sandboxRoot(rec.SandboxID); err == nil {
			measuredBytes += sumDirSizeBounded(sbxRoot, 250)
		}
		measuredBytes += measureProcTreeAndEnvForSandbox(rec.SandboxID, false)
	} else if h.engine != nil {
		measuredBytes += measureProcTreeAndEnvForSandbox(rec.SandboxID, true)
	}
	if measuredBytes < (16 << 20) {
		measuredBytes = 16 << 20 // 16 MiB baseline container footprint
	}
	usedMem := measuredBytes
	if simMemBytes > usedMem {
		usedMem = simMemBytes
	}

	cpuPct := 2.0
	if len(rec.BackgroundCommands) > 0 {
		cpuPct = 8.5
	}
	for _, rawCPU := range []string{
		rec.Metadata["simulated_cpu_pct"],
		rec.Env["SBX_SIMULATE_CPU_PCT"],
	} {
		if s := strings.TrimSpace(rawCPU); s != "" {
			if pct, err := strconv.ParseFloat(s, 64); err == nil && pct >= 0 {
				cpuPct = pct
				break
			}
		}
	}
	return usedMem, cpuPct
}

func (h *HostServer) applyFleetConfigLocked(cfg FleetConfig) {
	if cfg.MinClientVersion != "" {
		h.minClientVersion = cfg.MinClientVersion
	}
	if cfg.MaxDensity > 0 {
		h.maxDensity = cfg.MaxDensity
	}
	if cfg.HostCPU != "" {
		h.cfgHostCPU = cfg.HostCPU
	}
	if cfg.HostMemory != "" {
		h.cfgHostMemory = cfg.HostMemory
	}
	if cfg.MinFreeMemoryMB > 0 {
		h.minFreeMemoryMB = cfg.MinFreeMemoryMB
	}
	if cfg.MinFreeMemoryPct > 0 {
		h.minFreeMemoryPct = cfg.MinFreeMemoryPct
	}
	if cfg.MaxMemoryOvercommitRatio > 0 {
		h.maxMemoryOvercommit = cfg.MaxMemoryOvercommitRatio
	}
	if cfg.HotMemoryThresholdPct > 0 {
		h.hotMemoryThresholdPct = cfg.HotMemoryThresholdPct
	}
	if cfg.HotCPUThresholdPct > 0 {
		h.hotCPUThresholdPct = cfg.HotCPUThresholdPct
	}
	if cfg.DefaultIdleTimeout >= 0 {
		h.defaultIdleTimeoutSec = cfg.DefaultIdleTimeout
	}
}

func (h *HostServer) beginSandboxActivity(sandboxID string) {
	now := time.Now().UTC()
	h.mu.Lock()
	h.activeConns[sandboxID]++
	if cur, ok := h.sandboxes[sandboxID]; ok {
		cur.LastActiveAt = now
		h.sandboxes[sandboxID] = cur
	}
	h.mu.Unlock()
}

func (h *HostServer) touchSandboxActivity(sandboxID string) {
	now := time.Now().UTC()
	h.mu.Lock()
	if cur, ok := h.sandboxes[sandboxID]; ok {
		cur.LastActiveAt = now
		h.sandboxes[sandboxID] = cur
	}
	h.mu.Unlock()
}

func (h *HostServer) endSandboxActivity(sandboxID string) {
	now := time.Now().UTC()
	h.mu.Lock()
	if h.activeConns[sandboxID] > 1 {
		h.activeConns[sandboxID]--
	} else {
		delete(h.activeConns, sandboxID)
	}
	if cur, ok := h.sandboxes[sandboxID]; ok {
		cur.LastActiveAt = now
		h.sandboxes[sandboxID] = cur
	}
	h.mu.Unlock()
}

func (h *HostServer) syncConfigFromStore(ctx context.Context) {
	if h.store == nil {
		return
	}
	var cfg FleetConfig
	if _, ok, err := h.store.ReadJSON(ctx, "fleet/config.json", &cfg); err == nil && ok {
		h.mu.Lock()
		h.applyFleetConfigLocked(cfg)
		h.mu.Unlock()
	}
}

func (h *HostServer) appendMetricsSampleLocked(rec HostRecord) {
	if rec.HostID == "" {
		return
	}
	sample := HostResourceSample{
		Timestamp:             time.Now().UTC(),
		ActiveSandboxes:       rec.ActiveSandboxes,
		UsedMemoryBytes:       rec.UsedMemoryBytes,
		AvailableMemoryBytes:  rec.AvailableMemoryBytes,
		UsedMemoryPct:         rec.UsedMemoryPct,
		AllocatedMemoryBytes:  rec.AllocatedMemoryBytes,
		MemoryOvercommitRatio: rec.MemoryOvercommitRatio,
		CPUUsagePct:           rec.CPUUsagePct,
		CPUOvercommitRatio:    rec.CPUOvercommitRatio,
		OverloadState:         rec.OverloadState,
	}
	h.metricsHistory = append(h.metricsHistory, sample)
	if len(h.metricsHistory) > 120 {
		h.metricsHistory = h.metricsHistory[len(h.metricsHistory)-120:]
	}
}

func (h *HostServer) snapshotHostRecordLocked() HostRecord {
	hostCPU := strings.TrimSpace(os.Getenv("HOST_CPU"))
	if hostCPU == "" {
		hostCPU = h.cfgHostCPU
	}
	if hostCPU == "" {
		hostCPU = "4"
	}
	hostMem := strings.TrimSpace(os.Getenv("HOST_MEMORY"))
	if hostMem == "" {
		hostMem = h.cfgHostMemory
	}
	if hostMem == "" {
		hostMem = "8Gi"
	}
	totalMemBytes := ParseMemoryBytes(hostMem)
	totalCPUMilli := ParseCPUMillicores(hostCPU)

	sbxCopy := make(map[string]SandboxRecord, len(h.sandboxes))
	active := 0
	var allocMemBytes int64
	var allocCPUMilli int
	var sumUsedMemBytes int64
	var sumCPUPct float64
	hasSimulatedMem := false

	for k, v := range h.sandboxes {
		v.AllocatedMemoryBytes = ParseMemoryBytes(v.Memory)
		v.AllocatedCPUMillicores = ParseCPUMillicores(v.CPU)
		if v.Status == StatusRunning || v.Status == StatusCreating || v.Status == StatusMigrating {
			active++
			allocMemBytes += v.AllocatedMemoryBytes
			allocCPUMilli += v.AllocatedCPUMillicores
			if v.Metadata["simulated_memory_mb"] != "" || v.Env["SBX_SIMULATE_MEMORY_MB"] != "" {
				hasSimulatedMem = true
			}
			usedB, cpuP := h.measureSandboxUsage(v)
			v.UsedMemoryBytes = usedB
			if v.AllocatedMemoryBytes > 0 {
				v.UsedMemoryPct = math.Round((float64(usedB)/float64(v.AllocatedMemoryBytes))*1000.0) / 10.0
			}
			if usedB > v.PeakMemoryBytes {
				v.PeakMemoryBytes = usedB
			}
			v.CPUUsagePct = math.Round(cpuP*10.0) / 10.0
			if v.CPUUsagePct > v.PeakCPUUsagePct {
				v.PeakCPUUsagePct = v.CPUUsagePct
			}
			sumUsedMemBytes += usedB
			sumCPUPct += v.CPUUsagePct
			h.sandboxes[k] = v
		}
		sbxCopy[k] = v
	}

	usedMemBytes := sumUsedMemBytes
	if !hasSimulatedMem {
		if cgMem := readCgroupMemoryCurrent(totalMemBytes); cgMem > usedMemBytes {
			usedMemBytes = cgMem
		}
	}
	if usedMemBytes > totalMemBytes {
		usedMemBytes = totalMemBytes
	}
	availMemBytes := totalMemBytes - usedMemBytes
	if availMemBytes < 0 {
		availMemBytes = 0
	}
	usedMemPct := 0.0
	memOvercommit := 0.0
	if totalMemBytes > 0 {
		usedMemPct = math.Round((float64(usedMemBytes)/float64(totalMemBytes))*1000.0) / 10.0
		memOvercommit = math.Round((float64(allocMemBytes)/float64(totalMemBytes))*100.0) / 100.0
	}
	if usedMemPct > h.peakMemoryPct {
		h.peakMemoryPct = usedMemPct
	}
	cpuOvercommit := 0.0
	if totalCPUMilli > 0 {
		cpuOvercommit = math.Round((float64(allocCPUMilli)/float64(totalCPUMilli))*100.0) / 100.0
	}
	hostCPUPct := math.Min(100.0, math.Round(sumCPUPct*10.0)/10.0)
	if hostCPUPct > h.peakCPUUsagePct {
		h.peakCPUUsagePct = hostCPUPct
	}

	minFreeBytes := int64(h.minFreeMemoryMB) * (1 << 20)
	if pctBytes := int64(float64(totalMemBytes) * (h.minFreeMemoryPct / 100.0)); pctBytes > minFreeBytes {
		minFreeBytes = pctBytes
	}

	overloadState := "NORMAL"
	if (h.hotMemoryThresholdPct > 0 && usedMemPct >= h.hotMemoryThresholdPct) || (h.hotCPUThresholdPct > 0 && hostCPUPct >= h.hotCPUThresholdPct) {
		overloadState = "HOT_SHEDDING"
	} else if availMemBytes < minFreeBytes || (h.minFreeMemoryPct > 0 && usedMemPct >= (100.0-h.minFreeMemoryPct)) {
		overloadState = "ADMISSION_BLOCKED"
	} else if memOvercommit > 1.0 || usedMemPct >= 65.0 {
		overloadState = "ELEVATED"
	}

	if active > 0 {
		h.hadSandboxes = true
		h.retired = false
		h.emptySince = time.Time{}
	} else if h.emptySince.IsZero() {
		h.emptySince = time.Now().UTC()
	}
	free := h.maxDensity - active
	if free < 0 || h.status != HostActive || overloadState == "ADMISSION_BLOCKED" || overloadState == "HOT_SHEDDING" {
		free = 0
	}
	instState := SetInstanceBusyState(active > 0)
	evts := append([]ActivityEvent(nil), h.recentEvents...)
	engType := "local"
	if h.engine != nil {
		engType = h.engine.EngineType()
	}

	return HostRecord{
		HostID:                  h.hostID,
		InstanceID:              h.instanceID,
		Revision:                h.revision,
		EngineType:              engType,
		HostCPU:                 hostCPU,
		HostMemory:              hostMem,
		Status:                  h.status,
		MaxDensity:              h.maxDensity,
		ActiveSandboxes:         active,
		FreeSlots:               free,
		TotalMemoryBytes:        totalMemBytes,
		UsedMemoryBytes:         usedMemBytes,
		AvailableMemoryBytes:    availMemBytes,
		UsedMemoryPct:           usedMemPct,
		PeakMemoryPct:           h.peakMemoryPct,
		AllocatedMemoryBytes:    allocMemBytes,
		MemoryOvercommitRatio:   memOvercommit,
		TotalCPUMillicores:      totalCPUMilli,
		AllocatedCPUMillicores:  allocCPUMilli,
		CPUOvercommitRatio:      cpuOvercommit,
		CPUUsagePct:             hostCPUPct,
		PeakCPUUsagePct:         h.peakCPUUsagePct,
		OverloadState:           overloadState,
		RejectedCreationsMemory: h.rejectedCreationsMem,
		HotMigrationsTriggered:  h.hotMigrationsTriggered,
		Sandboxes:               sbxCopy,
		InstanceState:           instState,
		StartedAt:               h.startedAt,
		LastHeartbeatAt:         time.Now().UTC(),
		RecentEvents:            evts,
	}
}

// HostState returns the current HostRecord snapshot.
func (h *HostServer) HostState() HostRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.snapshotHostRecordLocked()
}

// retireEmptyHost deregisters an empty/drained host session from the store and control plane,
// marks the instance idle, and suppresses subsequent background heartbeats while the host remains empty.
func (h *HostServer) retireEmptyHost(ctx context.Context, reason string) {
	h.mu.Lock()
	hid := h.hostID
	if hid == "" {
		h.mu.Unlock()
		return
	}
	for _, s := range h.sandboxes {
		if s.Status == StatusRunning || s.Status == StatusCreating || s.Status == StatusMigrating {
			h.mu.Unlock()
			return
		}
	}
	h.retired = true
	h.hadSandboxes = false
	h.emptySince = time.Now().UTC()
	if h.status == HostDraining {
		h.status = HostActive
	}
	SetInstanceBusyState(false)
	deadRec := h.snapshotHostRecordLocked()
	deadRec.Status = HostDead
	deadRec.ActiveSandboxes = 0
	deadRec.FreeSlots = 0
	h.mu.Unlock()

	if h.store != nil {
		_ = h.store.DeleteObject(ctx, "hosts/"+hid+".json")
	}
	if h.controlPlaneURL != "" {
		if b, err := json.Marshal(deadRec); err == nil {
			reqCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
			defer cancel()
			if req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, h.controlPlaneURL+"/v1/heartbeat", bytes.NewReader(b)); err == nil {
				req.Header.Set("Content-Type", "application/json")
				if strings.HasPrefix(h.controlPlaneURL, "https://") {
					if tok, err := GetIDToken(reqCtx, h.controlPlaneURL); err == nil && tok != "" {
						req.Header.Set("Authorization", "Bearer "+tok)
					}
				}
				if resp, err := h.httpClient.Do(req); err == nil {
					_, _ = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
			}
		}
	}
}

// SyncStateAndHeartbeat writes `hosts/<host_id>.json` (single-writer per host), appends to
// `metrics/hosts/<host_id>.json`, and sends an HTTP heartbeat to `sandbox-control-plane` if configured.
// If h.hostID is not yet bound to a session ID, it is a no-op so idle unbound clones never pollute the fleet.
func (h *HostServer) SyncStateAndHeartbeat(ctx context.Context) error {
	h.syncConfigFromStore(ctx)
	h.mu.Lock()
	if h.hostID == "" {
		h.mu.Unlock()
		return nil
	}
	if h.status == HostCrashed {
		h.mu.Unlock()
		return ErrHostCrashed
	}
	if h.retired && len(h.sandboxes) == 0 {
		h.mu.Unlock()
		return nil
	}
	h.lastHeartbeatAt = time.Now().UTC()
	rec := h.snapshotHostRecordLocked()
	shouldRetireEmpty := rec.ActiveSandboxes == 0 && (h.hadSandboxes || (!h.emptySince.IsZero() && time.Since(h.emptySince) > 15*time.Second))
	if shouldRetireEmpty {
		h.mu.Unlock()
		h.retireEmptyHost(ctx, "empty_auto_scale_down")
		return nil
	}
	h.appendMetricsSampleLocked(rec)
	histCopy := HostMetricsHistory{
		HostID:    h.hostID,
		Samples:   append([]HostResourceSample(nil), h.metricsHistory...),
		UpdatedAt: time.Now().UTC(),
	}
	h.mu.Unlock()

	if h.store != nil {
		_, _ = h.store.WriteJSONCAS(ctx, "hosts/"+h.hostID+".json", rec, -1)
		_, _ = h.store.WriteJSONCAS(ctx, "metrics/hosts/"+h.hostID+".json", histCopy, -1)
	}
	if h.controlPlaneURL != "" {
		b, err := json.Marshal(rec)
		if err == nil {
			reqCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, h.controlPlaneURL+"/v1/heartbeat", bytes.NewReader(b))
			if err == nil {
				req.Header.Set("Content-Type", "application/json")
				if strings.HasPrefix(h.controlPlaneURL, "https://") {
					if tok, err := GetIDToken(reqCtx, h.controlPlaneURL); err == nil && tok != "" {
						req.Header.Set("Authorization", "Bearer "+tok)
					}
				}
				if resp, err := h.httpClient.Do(req); err == nil {
					var hbResp struct {
						FleetConfig
						FencedSandboxes []string `json:"fenced_sandboxes"`
						Retired         bool     `json:"retired"`
					}
					if json.NewDecoder(resp.Body).Decode(&hbResp) == nil {
						h.mu.Lock()
						h.applyFleetConfigLocked(hbResp.FleetConfig)
						for _, fencedID := range hbResp.FencedSandboxes {
							delete(h.sandboxes, fencedID)
						}
						rem := len(h.sandboxes)
						h.mu.Unlock()
						for _, fencedID := range hbResp.FencedSandboxes {
							_ = h.engine.Stop(ctx, fencedID)
						}
						if hbResp.Retired || (len(hbResp.FencedSandboxes) > 0 && rem == 0) {
							h.retireEmptyHost(ctx, "control_plane_fenced")
						}
					}
					_, _ = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
			}
		}
	}
	return nil
}

func (h *HostServer) syncPersistentSandboxes(ctx context.Context) {
	now := time.Now()
	h.mu.Lock()
	var list []SandboxRecord
	for id, r := range h.sandboxes {
		if r.Status != StatusRunning || !r.Persistent {
			continue
		}
		lastExp := h.lastExportAt[id]
		dirty := h.dirtySandboxes[id]
		// Export dirty sandboxes at most once every 3s, and all persistent sandboxes at least every 10s
		if (dirty && now.Sub(lastExp) >= 3*time.Second) || now.Sub(lastExp) >= 10*time.Second {
			delete(h.dirtySandboxes, id)
			h.lastExportAt[id] = now
			list = append(list, r)
		}
	}
	h.mu.Unlock()

	for _, rec := range list {
		_ = h.exportSandboxOverlayToStore(ctx, &rec, "periodic")
	}
}

func (h *HostServer) checkInGuestAndIdleTimeouts(ctx context.Context) {
	h.mu.RLock()
	var list []SandboxRecord
	for _, r := range h.sandboxes {
		if r.Status == StatusRunning && h.activeConns[r.SandboxID] <= 0 {
			list = append(list, r)
		}
	}
	h.mu.RUnlock()

	for _, rec := range list {
		h.mu.RLock()
		activeNow := h.activeConns[rec.SandboxID]
		curRec, stillRunning := h.sandboxes[rec.SandboxID]
		h.mu.RUnlock()
		if !stillRunning || curRec.Status != StatusRunning || activeNow > 0 {
			continue
		}
		guestState := h.engine.CheckInGuestState(rec.SandboxID)
		if guestState == "IDLE" || guestState == "STOP" || guestState == "SUSPEND" || guestState == "STOPPED" {
			_, _ = h.stopOrSuspendSandbox(ctx, rec.SandboxID, StatusSuspended, "in_guest_"+strings.ToLower(guestState))
			continue
		}
		if curRec.IdleTimeoutSec > 0 && time.Since(curRec.LastActiveAt) > time.Duration(curRec.IdleTimeoutSec)*time.Second {
			_, _ = h.stopOrSuspendSandbox(ctx, rec.SandboxID, StatusSuspended, "idle_timeout")
		}
	}
}

func (h *HostServer) exportSandboxOverlayToStore(ctx context.Context, rec *SandboxRecord, reason string) error {
	sbxMu := h.getSandboxLock(rec.SandboxID)
	sbxMu.Lock()
	defer sbxMu.Unlock()
	return h.exportSandboxOverlayToStoreLocked(ctx, rec, reason)
}

func (h *HostServer) exportSandboxOverlayToStoreLocked(ctx context.Context, rec *SandboxRecord, reason string) error {
	if h.store == nil {
		return nil
	}
	if h.IsCrashed() {
		return ErrHostCrashed
	}
	isBackgroundSync := reason == "periodic" || reason == "post_exec" || reason == "post_cp"
	if isBackgroundSync {
		h.mu.RLock()
		cur, ok := h.sandboxes[rec.SandboxID]
		h.mu.RUnlock()
		if !ok || cur.Status != StatusRunning {
			return fmt.Errorf("sandbox %s is no longer running on %s", rec.SandboxID, h.hostID)
		}
		*rec = cur
	}

	// Ownership fencing: for all exports after initial creation, verify we still own this sandbox and epoch in Store!
	var storeGen int64 = -1
	if reason != "initial_create" && reason != "fork_init" {
		var storeRec SandboxRecord
		if gen, exists, err := h.store.ReadJSON(ctx, "sandboxes/"+rec.SandboxID+".json", &storeRec); err == nil {
			storeGen = gen
			if !exists || storeRec.Status == StatusDeleted || (storeRec.HostID != "" && storeRec.HostID != h.hostID) || storeRec.Epoch > rec.Epoch {
				_ = h.engine.Stop(ctx, rec.SandboxID)
				h.mu.Lock()
				delete(h.sandboxes, rec.SandboxID)
				h.mu.Unlock()
				return fmt.Errorf("fenced stale sandbox %s on %s", rec.SandboxID, h.hostID)
			}
		}
	}

	prevOverlayPath := rec.OverlayPath
	isLifecycleTransition := reason == "api_request" || reason == "idle_timeout" ||
		strings.HasPrefix(reason, "in_guest_") || strings.HasPrefix(reason, "drain_") ||
		strings.HasPrefix(reason, "cli_migrate")
	if isLifecycleTransition {
		// Cleanly stop any internal database (PostgreSQL) before creating the final lifecycle tarball!
		_ = h.engine.Quiesce(ctx, rec.SandboxID)
		rec.OverlayPath = fmt.Sprintf("persistence/%s/overlay-e%d.tar", rec.SandboxID, rec.Epoch)
	}
	overlayPath := rec.OverlayPath
	if overlayPath == "" {
		overlayPath = fmt.Sprintf("persistence/%s/overlay.tar", rec.SandboxID)
		rec.OverlayPath = overlayPath
	}

	localTar, commit, err := h.store.LocalBlobPath(ctx, overlayPath, true)
	if err != nil {
		return err
	}
	if err := h.engine.ExportTar(ctx, *rec, localTar); err != nil {
		return err
	}
	if err := commit(); err != nil {
		return err
	}
	now := time.Now().UTC()
	rec.LastSnapshotAt = now
	rec.UpdatedAt = now
	h.mu.Lock()
	if cur, ok := h.sandboxes[rec.SandboxID]; ok {
		cur.OverlayPath = overlayPath
		cur.LastSnapshotAt = now
		cur.UpdatedAt = now
		h.sandboxes[rec.SandboxID] = cur
	}
	h.lastExportAt[rec.SandboxID] = now
	h.mu.Unlock()
	if reason == "periodic" || (isBackgroundSync && prevOverlayPath != overlayPath) {
		_, _ = h.store.WriteJSONCAS(ctx, "sandboxes/"+rec.SandboxID+".json", rec, storeGen)
	}
	return nil
}

// recoverReplacedSessionSandboxes checks if this newly bound clone replaced a crashed Cloud Run
// clone for the same session ID `boundHostID`, and immediately hydrates its RUNNING sandboxes.
func (h *HostServer) recoverReplacedSessionSandboxes(ctx context.Context, boundHostID string) {
	if h.store == nil || boundHostID == "" {
		return
	}
	var prevHost HostRecord
	_, exists, err := h.store.ReadJSON(ctx, "hosts/"+boundHostID+".json", &prevHost)
	if err != nil || !exists {
		return
	}
	if prevHost.InstanceID == "" || prevHost.InstanceID == h.instanceID {
		return
	}
	for sbxID := range prevHost.Sandboxes {
		var storeRec SandboxRecord
		gen, ok, rErr := h.store.ReadJSON(ctx, "sandboxes/"+sbxID+".json", &storeRec)
		if rErr == nil && ok && storeRec.HostID == boundHostID && storeRec.Status == StatusRunning {
			_, _ = h.hydrateSandboxInternal(ctx, storeRec, storeRec.OverlayPath, "crash_recovery", gen)
		}
	}
}

// ServeHTTP routes all `/__session/*` and `/__internal/*` requests on the HostServer.
func (h *HostServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Security headers
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")

	// Method allow-list (allow all standard HTTP verbs for /proxy/ and /__session/proxy)
	isProxyReq := strings.HasPrefix(r.URL.Path, "/proxy/") || r.URL.Path == "/__session/proxy"
	if !isProxyReq && r.Method != http.MethodGet && r.Method != http.MethodPost && r.Method != http.MethodDelete && r.Method != http.MethodHead {
		writeHostJSON(w, h, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}

	// Bind Host Session ID once and only once from the first incoming request's X-Serverless-Session-Id header!
	reqHostID := strings.TrimSpace(r.Header.Get("X-Serverless-Session-Id"))
	justBound := false
	var boundID string
	h.mu.Lock()
	if h.hostID == "" {
		if reqHostID != "" && ValidateID(reqHostID) == nil {
			h.hostID = reqHostID
			boundID = reqHostID
			justBound = true
		} else if r.URL.Path == "/__session/create" || r.URL.Path == "/__internal/hydrate" || r.URL.Path == "/__session/exec" || r.URL.Path == "/__session/exec-stream" || r.URL.Path == "/__session/port-forward" || isProxyReq {
			h.hostID = NewRandomID("host")
			boundID = h.hostID
			justBound = true
		}
	}
	curHostID := h.hostID
	h.mu.Unlock()

	if justBound && boundID != "" {
		h.recoverReplacedSessionSandboxes(r.Context(), boundID)
	}

	w.Header().Set("X-Host-Session-Id", curHostID)
	w.Header().Set("X-Cloud-Run-Instance-Id", h.instanceID)
	h.mu.RLock()
	minVer := h.minClientVersion
	h.mu.RUnlock()
	if minVer == "" {
		minVer = DefaultMinClientVersion
	}
	w.Header().Set("X-Sbx-Server-Version", DispenserVersion)
	w.Header().Set("X-Sbx-Min-Client-Version", minVer)

	if strings.HasPrefix(r.URL.Path, "/__session/") && !isProxyReq {
		if err := CheckClientVersion(r, minVer); err != nil {
			WriteUpgradeRequired(w, r, minVer)
			return
		}
	}

	if h.IsCrashed() && r.URL.Path != "/__internal/recover-host" {
		writeHostJSON(w, h, http.StatusServiceUnavailable, map[string]any{
			"error":   "host_crashed",
			"host_id": h.hostID,
		})
		return
	}

	switch r.URL.Path {
	case "/healthz", "/__internal/ping":
		state := h.HostState()
		writeHostJSON(w, h, http.StatusOK, map[string]any{
			"ok":               true,
			"host_id":          h.hostID,
			"status":           state.Status,
			"active_sandboxes": state.ActiveSandboxes,
			"free_slots":       state.FreeSlots,
			"revision":         h.revision,
		})
	case "/__session/state":
		writeHostJSON(w, h, http.StatusOK, h.HostState())
	case "/__session/create":
		h.handleCreate(w, r)
	case "/__session/exec":
		if r.URL.Query().Get("stream") == "true" || strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
			h.handleExecStream(w, r)
		} else {
			h.handleExec(w, r)
		}
	case "/__session/exec-stream":
		h.handleExecStream(w, r)
	case "/__session/proxy":
		h.handleProxy(w, r)
	case "/__session/port-forward":
		h.handlePortForward(w, r)
	case "/__session/cp":
		h.handleCopyFile(w, r)
	case "/__session/logs":
		h.handleLogs(w, r)
	case "/__session/inspect", "/__session/status":
		h.handleInspect(w, r)
	case "/__session/update":
		h.handleUpdate(w, r)
	case "/__session/snapshot", "/__session/checkpoint":
		h.handleSnapshot(w, r)
	case "/__session/copy", "/__session/fork":
		h.handleCopySandbox(w, r)
	case "/__session/migrate":
		h.handleMigrate(w, r)
	case "/__session/stop":
		h.handleStopOrSuspend(w, r, StatusStopped)
	case "/__session/suspend":
		h.handleStopOrSuspend(w, r, StatusSuspended)
	case "/__session/start", "/__session/resume":
		h.handleStartOrResume(w, r)
	case "/__session/delete":
		h.handleDelete(w, r)
	case "/__internal/hydrate":
		h.handleInternalHydrate(w, r)
	case "/__internal/drain":
		h.handleInternalDrain(w, r)
	case "/__internal/check-overload":
		h.handleInternalCheckOverload(w, r)
	case "/__internal/simulate-crash":
		h.handleSimulateCrash(w, r)
	case "/__internal/hold-busy":
		h.handleHoldBusy(w, r)
	default:
		if strings.HasPrefix(r.URL.Path, "/proxy/") {
			h.handleProxy(w, r)
			return
		}
		writeHostJSON(w, h, http.StatusNotFound, map[string]any{"error": "unknown endpoint " + r.URL.Path})
	}
}

func (h *HostServer) handleHoldBusy(w http.ResponseWriter, r *http.Request) {
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		state := h.HostState()
		if state.ActiveSandboxes == 0 || h.IsCrashed() || state.Status != HostActive {
			break
		}
		select {
		case <-r.Context().Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
	writeHostJSON(w, h, http.StatusOK, map[string]any{"ok": true, "host_id": h.hostID})
}

func writeHostJSON(w http.ResponseWriter, h *HostServer, status int, payload any) {
	state := h.HostState()
	w.Header().Set("X-Host-Session-Id", state.HostID)
	w.Header().Set("X-Serverless-Session-Id", state.HostID)
	w.Header().Set("X-Host-Free-Slots", strconv.Itoa(state.FreeSlots))
	w.Header().Set("X-Host-Revision", state.Revision)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func extractSandboxID(r *http.Request, bodyID string) string {
	for _, v := range []string{
		strings.TrimSpace(bodyID),
		strings.TrimSpace(r.Header.Get("X-Sandbox-Id")),
		strings.TrimSpace(r.URL.Query().Get("sandbox_id")),
		strings.TrimSpace(r.URL.Query().Get("session_id")),
	} {
		if v != "" {
			return v
		}
	}
	return ""
}

// resolveSandboxIdentifier maps either a host-generated `sandbox_id` (e.g. `sbx-<host>-<hex>`)
// or a user-provided `name` to its canonical `sandbox_id`.
// If `idOrName` is a name that matches multiple active sandboxes, it returns an ambiguity error.
func (h *HostServer) resolveSandboxIdentifier(ctx context.Context, idOrName string) (string, error) {
	h.mu.RLock()
	if _, ok := h.sandboxes[idOrName]; ok {
		h.mu.RUnlock()
		return idOrName, nil
	}
	if _, ok := h.migratedTo[idOrName]; ok {
		h.mu.RUnlock()
		return idOrName, nil
	}
	var localMatches []SandboxRecord
	for _, s := range h.sandboxes {
		if s.Name == idOrName && s.Status != StatusDeleted {
			localMatches = append(localMatches, s)
		}
	}
	h.mu.RUnlock()

	if h.store != nil {
		var exact SandboxRecord
		if _, exists, err := h.store.ReadJSON(ctx, "sandboxes/"+idOrName+".json", &exact); err == nil && exists && exact.Status != StatusDeleted {
			return idOrName, nil
		}
		if ExtractInitialHostFromSandboxID(idOrName) == "" {
			if paths, err := h.store.ListObjects(ctx, "sandboxes/"); err == nil {
				var matches []SandboxRecord
				for _, p := range paths {
					var s SandboxRecord
					if _, ok, rErr := h.store.ReadJSON(ctx, p, &s); rErr == nil && ok && s.Name == idOrName && s.Status != StatusDeleted {
						matches = append(matches, s)
					}
				}
				if len(matches) > 1 {
					sort.Slice(matches, func(i, j int) bool { return matches[i].SandboxID < matches[j].SandboxID })
					ids := make([]string, 0, len(matches))
					for _, m := range matches {
						ids = append(ids, m.SandboxID)
					}
					return "", fmt.Errorf("ambiguous sandbox name %q matches %d sandboxes (%s); please specify the exact sandbox_id", idOrName, len(matches), strings.Join(ids, ", "))
				}
				if len(matches) == 1 {
					return matches[0].SandboxID, nil
				}
			}
		}
	}
	if len(localMatches) > 1 {
		sort.Slice(localMatches, func(i, j int) bool { return localMatches[i].SandboxID < localMatches[j].SandboxID })
		ids := make([]string, 0, len(localMatches))
		for _, m := range localMatches {
			ids = append(ids, m.SandboxID)
		}
		return "", fmt.Errorf("ambiguous sandbox name %q matches %d sandboxes (%s); please specify the exact sandbox_id", idOrName, len(localMatches), strings.Join(ids, ", "))
	}
	if len(localMatches) == 1 {
		return localMatches[0].SandboxID, nil
	}
	return idOrName, nil
}

// resolveOrAutoHydrateSandbox checks if sandboxID is hosted on this HostServer.
// If not, it inspects `sandboxes/<sandbox_id>.json` in the Store:
//   - If RUNNING on another active host -> writes HTTP 409 Conflict with `X-Sandbox-Host-Session: <owner_host_id>`.
//   - If SUSPENDED/STOPPED (or owner host is DEAD/CRASHED) and allowAutoResume is true -> hydrates it onto this host!
func (h *HostServer) resolveOrAutoHydrateSandbox(ctx context.Context, w http.ResponseWriter, sandboxID string, allowAutoResume bool) (SandboxRecord, bool) {
	if err := ValidateID(sandboxID); err != nil {
		writeHostJSON(w, h, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return SandboxRecord{}, false
	}
	resolvedID, ambErr := h.resolveSandboxIdentifier(ctx, sandboxID)
	if ambErr != nil {
		writeHostJSON(w, h, http.StatusConflict, map[string]any{"error": ambErr.Error()})
		return SandboxRecord{}, false
	}
	sandboxID = resolvedID

	h.mu.RLock()
	localRec, ok := h.sandboxes[sandboxID]
	migTarget := h.migratedTo[sandboxID]
	h.mu.RUnlock()
	if ok && (localRec.Status == StatusMigrating || localRec.Status == StatusCreating) {
		// Wait for in-flight migration/drain/hydration of this sandbox to complete before deciding!
		sbxMu := h.getSandboxLock(sandboxID)
		sbxMu.Lock()
		h.mu.RLock()
		localRec, ok = h.sandboxes[sandboxID]
		migTarget = h.migratedTo[sandboxID]
		h.mu.RUnlock()
		sbxMu.Unlock()
	}
	if ok && localRec.Status == StatusRunning {
		return localRec, true
	}
	if !ok && migTarget != "" && migTarget != h.hostID {
		w.Header().Set("X-Sandbox-Host-Session", migTarget)
		writeHostJSON(w, h, http.StatusConflict, MisdirectedSandboxError{
			ErrorCode:     "misdirected_sandbox",
			SandboxID:     sandboxID,
			HostSessionID: migTarget,
			Status:        StatusRunning,
		})
		return SandboxRecord{}, false
	}

	var storeRec SandboxRecord
	var gen int64
	exists := false
	if h.store != nil {
		var err error
		gen, exists, err = h.store.ReadJSON(ctx, "sandboxes/"+sandboxID+".json", &storeRec)
		if err != nil {
			writeHostJSON(w, h, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return SandboxRecord{}, false
		}
	} else if ok {
		storeRec = localRec
		exists = true
	}

	if !exists || storeRec.Status == StatusDeleted {
		writeHostJSON(w, h, http.StatusNotFound, map[string]any{
			"error":      fmt.Sprintf("sandbox %q not found", sandboxID),
			"sandbox_id": sandboxID,
		})
		return SandboxRecord{}, false
	}

	// Check if running on another host
	hydrateReason := "auto_resume"
	if storeRec.Status == StatusRunning && storeRec.HostID != "" && storeRec.HostID != h.hostID {
		// Check if that other host is explicitly marked DEAD or CRASHED or timed out
		otherDead := false
		if h.store != nil {
			var otherHost HostRecord
			if _, hostExists, _ := h.store.ReadJSON(ctx, "hosts/"+storeRec.HostID+".json", &otherHost); hostExists {
				if otherHost.Status == HostDead || otherHost.Status == HostCrashed || time.Since(otherHost.LastHeartbeatAt) > 20*time.Second {
					otherDead = true
				}
			} else {
				otherDead = true
			}
		}
		if !otherDead {
			w.Header().Set("X-Sandbox-Host-Session", storeRec.HostID)
			w.Header().Set("X-Sandbox-Current-Host", storeRec.HostID)
			writeHostJSON(w, h, http.StatusConflict, MisdirectedSandboxError{
				ErrorCode:     "misdirected_sandbox",
				SandboxID:     sandboxID,
				HostSessionID: storeRec.HostID,
				Epoch:         storeRec.Epoch,
				Status:        storeRec.Status,
			})
			return SandboxRecord{}, false
		}
		hydrateReason = "crash_recovery"
	} else if storeRec.Status == StatusRunning && storeRec.HostID == h.hostID {
		// Previous instance of this same session crashed while sandbox was RUNNING
		hydrateReason = "crash_recovery"
	}

	if !allowAutoResume {
		return storeRec, true
	}

	// Auto-hydrate / auto-resume onto this host!
	hydratedRec, err := h.hydrateSandboxInternal(ctx, storeRec, storeRec.OverlayPath, hydrateReason, gen)
	if err != nil {
		if hydratedRec.HostID != "" && hydratedRec.HostID != h.hostID {
			w.Header().Set("X-Sandbox-Host-Session", hydratedRec.HostID)
			w.Header().Set("X-Sandbox-Current-Host", hydratedRec.HostID)
			writeHostJSON(w, h, http.StatusConflict, MisdirectedSandboxError{
				ErrorCode:     "misdirected_sandbox",
				SandboxID:     sandboxID,
				HostSessionID: hydratedRec.HostID,
				Epoch:         hydratedRec.Epoch,
				Status:        hydratedRec.Status,
			})
			return SandboxRecord{}, false
		}
		status := http.StatusInternalServerError
		if errors.Is(err, ErrHostFull) {
			status = http.StatusTooManyRequests
		} else if errors.Is(err, ErrHostDraining) {
			status = http.StatusServiceUnavailable
		}
		writeHostJSON(w, h, status, map[string]any{"error": err.Error(), "sandbox_id": sandboxID})
		return SandboxRecord{}, false
	}
	return hydratedRec, true
}

func (h *HostServer) handleCreate(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var req CreateSandboxRequest
	if r.Body != nil {
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			writeHostJSON(w, h, http.StatusBadRequest, map[string]any{"error": "invalid JSON payload: " + err.Error()})
			return
		}
	}
	sbxName := strings.TrimSpace(req.Name)
	if sbxName == "" {
		sbxName = strings.TrimSpace(req.SandboxID)
	}
	if sbxName == "" {
		sbxName = extractSandboxID(r, "")
	}
	if sbxName != "" {
		if err := ValidateID(sbxName); err != nil {
			writeHostJSON(w, h, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
	}
	// Host generates a globally unique distributed SandboxID embedding the initial host session ID!
	req.SandboxID = GenerateDistributedSandboxID(h.hostID)
	if sbxName == "" {
		sbxName = req.SandboxID
	}
	if req.Template == "" {
		req.Template = "ubuntu"
	}
	if err := ValidateID(req.Template); err != nil {
		writeHostJSON(w, h, http.StatusBadRequest, map[string]any{"error": "invalid template: " + err.Error()})
		return
	}
	if req.CPU == "" {
		req.CPU = "1"
	}
	if req.Memory == "" {
		req.Memory = "1Gi"
	}
	if err := ValidateCPUMemory(req.CPU, req.Memory); err != nil {
		writeHostJSON(w, h, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}

	// Validate template (must be built-in or registered in templates/<name>.json)
	builtIns := BuiltInTemplates()
	_, isBuiltIn := builtIns[strings.ToLower(req.Template)]
	var importBlobPath string
	if !isBuiltIn {
		foundCustom := false
		if h.store != nil {
			var customTpl TemplateSpec
			if _, ok, _ := h.store.ReadJSON(r.Context(), "templates/"+req.Template+".json", &customTpl); ok && customTpl.Name != "" {
				foundCustom = true
				if customTpl.OverlayPath != "" {
					importBlobPath = customTpl.OverlayPath
				}
				if customTpl.DefaultEnv != nil {
					if req.Env == nil {
						req.Env = make(map[string]string)
					}
					for k, v := range customTpl.DefaultEnv {
						if _, exists := req.Env[k]; !exists {
							req.Env[k] = v
						}
					}
				}
			}
		}
		if !foundCustom {
			writeHostJSON(w, h, http.StatusBadRequest, map[string]any{
				"error":    fmt.Sprintf("unknown template %q (not found in built-in or custom templates)", req.Template),
				"template": req.Template,
			})
			return
		}
	}
	if req.FromSnapshotID != "" {
		if err := ValidateID(req.FromSnapshotID); err != nil {
			writeHostJSON(w, h, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		if h.store != nil {
			var snap SnapshotRecord
			if _, ok, _ := h.store.ReadJSON(r.Context(), "snapshots/"+req.FromSnapshotID+".json", &snap); ok && snap.TarPath != "" {
				importBlobPath = snap.TarPath
			} else {
				writeHostJSON(w, h, http.StatusNotFound, map[string]any{
					"error":       fmt.Sprintf("snapshot %q not found", req.FromSnapshotID),
					"snapshot_id": req.FromSnapshotID,
				})
				return
			}
		}
	}

	// Serialize per-sandbox operations on this host
	sbxMu := h.getSandboxLock(req.SandboxID)
	sbxMu.Lock()
	defer sbxMu.Unlock()

	h.syncConfigFromStore(r.Context())

	// Enforce draining, max_density, and memory-aware admission control under lock
	now := time.Now().UTC()
	h.mu.Lock()
	if h.status == HostDraining {
		h.mu.Unlock()
		writeHostJSON(w, h, http.StatusServiceUnavailable, map[string]any{
			"error":   "host_draining",
			"message": ErrHostDraining.Error(),
			"host_id": h.hostID,
		})
		return
	}
	activeCount := 0
	for _, sbx := range h.sandboxes {
		if sbx.Status == StatusRunning || sbx.Status == StatusCreating || sbx.Status == StatusMigrating {
			activeCount++
		}
	}
	if activeCount >= h.maxDensity {
		h.mu.Unlock()
		writeHostJSON(w, h, http.StatusTooManyRequests, map[string]any{
			"error":       "host_full",
			"message":     ErrHostFull.Error(),
			"host_id":     h.hostID,
			"max_density": h.maxDensity,
			"free_slots":  0,
		})
		return
	}

	recSnap := h.snapshotHostRecordLocked()
	minFreeBytes := int64(h.minFreeMemoryMB) * (1 << 20)
	if pctBytes := int64(float64(recSnap.TotalMemoryBytes) * (h.minFreeMemoryPct / 100.0)); pctBytes > minFreeBytes {
		minFreeBytes = pctBytes
	}
	newReqMemBytes := ParseMemoryBytes(req.Memory)
	if !req.Force && activeCount > 0 && (recSnap.AvailableMemoryBytes < minFreeBytes ||
		(h.minFreeMemoryPct > 0 && recSnap.UsedMemoryPct >= (100.0-h.minFreeMemoryPct)) ||
		(h.maxMemoryOvercommit > 0 && recSnap.TotalMemoryBytes > 0 && float64(recSnap.AllocatedMemoryBytes+newReqMemBytes)/float64(recSnap.TotalMemoryBytes) > h.maxMemoryOvercommit)) {
		h.rejectedCreationsMem++
		availMB := recSnap.AvailableMemoryBytes / (1 << 20)
		reqMinMB := minFreeBytes / (1 << 20)
		msg := fmt.Sprintf("host %s rejected sandbox creation due to host_memory_pressure: used=%.1f%%, available=%dMiB < min_free=%dMiB (overcommit=%.2fx, max=%.2fx)",
			h.hostID, recSnap.UsedMemoryPct, availMB, reqMinMB, recSnap.MemoryOvercommitRatio, h.maxMemoryOvercommit)
		h.recordEventLocked("ADMISSION_REJECTED", req.SandboxID, h.hostID, "", msg)
		updatedHostRec := h.snapshotHostRecordLocked()
		h.appendMetricsSampleLocked(updatedHostRec)
		h.mu.Unlock()
		if h.store != nil {
			_, _ = h.store.WriteJSONCAS(r.Context(), "hosts/"+h.hostID+".json", updatedHostRec, -1)
		}
		w.Header().Set("X-Host-Memory-Pressure", "true")
		writeHostJSON(w, h, http.StatusTooManyRequests, map[string]any{
			"error":               "host_memory_pressure",
			"message":             msg,
			"host_id":             h.hostID,
			"used_memory_pct":     recSnap.UsedMemoryPct,
			"available_memory_mb": availMB,
			"min_free_memory_mb":  reqMinMB,
			"free_slots":          0,
		})
		go func() {
			bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = h.SyncStateAndHeartbeat(bgCtx)
		}()
		return
	}

	engType := "local"
	if h.engine != nil {
		engType = h.engine.EngineType()
	}
	effectiveIdle := req.IdleTimeoutSec
	if effectiveIdle == 0 && !req.DisableAutoSuspend && h.defaultIdleTimeoutSec > 0 {
		effectiveIdle = h.defaultIdleTimeoutSec
	}
	overlayPath := fmt.Sprintf("persistence/%s/overlay.tar", req.SandboxID)
	rec := SandboxRecord{
		SandboxID:          req.SandboxID,
		Name:               sbxName,
		HostID:             h.hostID,
		HostRevision:       h.revision,
		EngineType:         engType,
		PinnedHost:         req.PinnedHost,
		Epoch:              1,
		Status:             StatusCreating,
		Template:           req.Template,
		Persistent:         req.Persistent || effectiveIdle > 0 || strings.Contains(strings.ToLower(req.Template), "postgres"),
		CPU:                req.CPU,
		Memory:             req.Memory,
		AllowEgress:        req.AllowEgress,
		Env:                req.Env,
		Metadata:           req.Metadata,
		IdleTimeoutSec:     effectiveIdle,
		DisableAutoSuspend: req.DisableAutoSuspend,
		OverlayPath:        overlayPath,
		BaseSnapshotID:     req.FromSnapshotID,
		CreatedAt:          now,
		UpdatedAt:          now,
		LastActiveAt:       now,
	}
	if strings.Contains(strings.ToLower(req.Template), "postgres") {
		rec.PostgresPort = 5432
	}
	// Reserve slot locally immediately before starting container
	h.sandboxes[req.SandboxID] = rec
	delete(h.migratedTo, req.SandboxID)
	SetInstanceBusyState(true)
	h.mu.Unlock()

	var localImportTar string
	var cleanupImport func() error
	if importBlobPath != "" && h.store != nil {
		if p, cleanFn, err := h.store.LocalBlobPath(r.Context(), importBlobPath, false); err == nil {
			localImportTar = p
			cleanupImport = cleanFn
		}
	}
	if cleanupImport != nil {
		defer func() { _ = cleanupImport() }()
	}

	if err := h.engine.Start(r.Context(), rec, localImportTar); err != nil {
		h.mu.Lock()
		delete(h.sandboxes, req.SandboxID)
		h.snapshotHostRecordLocked()
		h.mu.Unlock()
		writeHostJSON(w, h, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}

	rec.Status = StatusRunning
	rec.UpdatedAt = time.Now().UTC()
	if lp, ok := h.engine.(*LocalProcessEngine); ok && strings.Contains(strings.ToLower(rec.Template), "postgres") {
		rec.PostgresPort = lp.postgresPortFor(rec.SandboxID)
	}
	if rec.Persistent {
		_ = h.exportSandboxOverlayToStoreLocked(r.Context(), &rec, "initial_create")
	}

	dur := fmt.Sprintf("%dms", time.Since(start).Milliseconds())
	h.mu.Lock()
	h.sandboxes[rec.SandboxID] = rec
	h.recordEventLocked("CREATE", rec.SandboxID, h.hostID, dur,
		fmt.Sprintf("Created sandbox %s (name=%s, template=%s, persistent=%v, allow_egress=%v) on %s in %s", rec.SandboxID, rec.Name, rec.Template, rec.Persistent, rec.AllowEgress, h.hostID, dur))
	hostRecAfter := h.snapshotHostRecordLocked()
	h.appendMetricsSampleLocked(hostRecAfter)
	if updatedSbx, ok := h.sandboxes[rec.SandboxID]; ok {
		rec = updatedSbx
	}
	histCopy := HostMetricsHistory{
		HostID:    h.hostID,
		Samples:   append([]HostResourceSample(nil), h.metricsHistory...),
		UpdatedAt: time.Now().UTC(),
	}
	h.mu.Unlock()

	if h.store != nil {
		_, _ = h.store.WriteJSONCAS(r.Context(), "sandboxes/"+rec.SandboxID+".json", rec, -1)
		_, _ = h.store.WriteJSONCAS(r.Context(), "hosts/"+h.hostID+".json", hostRecAfter, -1)
		_, _ = h.store.WriteJSONCAS(r.Context(), "metrics/hosts/"+h.hostID+".json", histCopy, -1)
	}

	// Async flush host state & heartbeat
	go func() {
		bgCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = h.SyncStateAndHeartbeat(bgCtx)
	}()

	writeHostJSON(w, h, http.StatusOK, rec)
}

func (h *HostServer) handleExec(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("stream") == "true" || strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
		h.handleExecStream(w, r)
		return
	}
	start := time.Now()
	var req ExecRequest
	if r.Body != nil {
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			writeHostJSON(w, h, http.StatusBadRequest, map[string]any{"error": "invalid JSON: " + err.Error()})
			return
		}
	}
	if req.Stream {
		h.execStreamWithRequest(w, r, req, start)
		return
	}
	if req.SandboxID == "" {
		req.SandboxID = extractSandboxID(r, "")
	}
	if len(req.Command) == 0 {
		writeHostJSON(w, h, http.StatusBadRequest, map[string]any{"error": "command is required"})
		return
	}

	rec, ok := h.resolveOrAutoHydrateSandbox(r.Context(), w, req.SandboxID, true)
	if !ok {
		return
	}
	h.beginSandboxActivity(rec.SandboxID)
	defer h.endSandboxActivity(rec.SandboxID)

	effectiveAllowEgress := rec.AllowEgress
	if req.AllowEgress != nil {
		effectiveAllowEgress = *req.AllowEgress
	}
	mergedEnv := make(map[string]string)
	for k, v := range rec.Env {
		mergedEnv[k] = v
	}
	for k, v := range req.Env {
		mergedEnv[k] = v
	}
	mergedEnv["SBX_ALLOW_EGRESS"] = strconv.FormatBool(effectiveAllowEgress)
	timeout := time.Duration(req.TimeoutSec) * time.Second
	stdout, stderr, exitCode, pid, err := h.engine.Exec(r.Context(), rec.SandboxID, req.Command, req.Workdir, mergedEnv, req.Background, timeout)
	if err != nil {
		// Check if the sandbox was concurrently migrated or drained away while Exec was starting
		if strings.Contains(err.Error(), "is not running") {
			h.mu.RLock()
			migTarget := h.migratedTo[rec.SandboxID]
			h.mu.RUnlock()
			if migTarget == "" && h.store != nil {
				var storeRec SandboxRecord
				if _, exists, _ := h.store.ReadJSON(r.Context(), "sandboxes/"+rec.SandboxID+".json", &storeRec); exists && storeRec.HostID != "" && storeRec.HostID != h.hostID {
					migTarget = storeRec.HostID
				}
			}
			if migTarget != "" && migTarget != h.hostID {
				w.Header().Set("X-Sandbox-Host-Session", migTarget)
				writeHostJSON(w, h, http.StatusConflict, MisdirectedSandboxError{
					ErrorCode:     "misdirected_sandbox",
					SandboxID:     rec.SandboxID,
					HostSessionID: migTarget,
					Status:        StatusRunning,
				})
				return
			}
		}
		if exitCode == -1 {
			writeHostJSON(w, h, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
	}

	now := time.Now().UTC()
	bgCmdStr := strings.Join(req.Command, " ")
	if len(req.Command) >= 3 && (req.Command[0] == "sh" || req.Command[0] == "/bin/sh" || req.Command[0] == "bash") && req.Command[1] == "-c" {
		bgCmdStr = strings.Join(req.Command[2:], " ")
	}
	h.mu.Lock()
	if cur, exists := h.sandboxes[rec.SandboxID]; exists {
		cur.LastActiveAt = now
		cur.UpdatedAt = now
		if req.Background && bgCmdStr != "" {
			already := false
			for _, existingCmd := range cur.BackgroundCommands {
				if existingCmd == bgCmdStr {
					already = true
					break
				}
			}
			if !already {
				cur.BackgroundCommands = append(cur.BackgroundCommands, bgCmdStr)
			}
		}
		h.sandboxes[rec.SandboxID] = cur
		rec = cur
	}
	h.recordEventLocked("EXEC", rec.SandboxID, h.hostID, fmt.Sprintf("%dms", time.Since(start).Milliseconds()),
		fmt.Sprintf("Executed %q in %s (exit=%d)", strings.Join(req.Command, " "), rec.SandboxID, exitCode))
	h.mu.Unlock()

	if req.Background && h.store != nil {
		_, _ = h.store.WriteJSONCAS(r.Context(), "sandboxes/"+rec.SandboxID+".json", rec, -1)
	}
	if rec.Persistent && !req.Background {
		_ = h.exportSandboxOverlayToStore(r.Context(), &rec, "post_exec")
	} else if rec.Persistent {
		h.markSandboxDirty(rec.SandboxID)
	}

	writeHostJSON(w, h, http.StatusOK, ExecResponse{
		SandboxID:  rec.SandboxID,
		HostID:     h.hostID,
		Epoch:      rec.Epoch,
		ExitCode:   exitCode,
		Stdout:     stdout,
		Stderr:     stderr,
		PID:        pid,
		DurationMs: time.Since(start).Milliseconds(),
	})
}

func (h *HostServer) handleExecStream(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var req ExecRequest
	if r.Body != nil {
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			writeHostJSON(w, h, http.StatusBadRequest, map[string]any{"error": "invalid JSON: " + err.Error()})
			return
		}
	}
	h.execStreamWithRequest(w, r, req, start)
}

func (h *HostServer) execStreamWithRequest(w http.ResponseWriter, r *http.Request, req ExecRequest, start time.Time) {
	if req.SandboxID == "" {
		req.SandboxID = extractSandboxID(r, "")
	}
	if len(req.Command) == 0 {
		writeHostJSON(w, h, http.StatusBadRequest, map[string]any{"error": "command is required"})
		return
	}

	rec, ok := h.resolveOrAutoHydrateSandbox(r.Context(), w, req.SandboxID, true)
	if !ok {
		return
	}
	h.beginSandboxActivity(rec.SandboxID)
	defer h.endSandboxActivity(rec.SandboxID)

	effectiveAllowEgress := rec.AllowEgress
	if req.AllowEgress != nil {
		effectiveAllowEgress = *req.AllowEgress
	}
	mergedEnv := make(map[string]string)
	for k, v := range rec.Env {
		mergedEnv[k] = v
	}
	for k, v := range req.Env {
		mergedEnv[k] = v
	}
	mergedEnv["SBX_ALLOW_EGRESS"] = strconv.FormatBool(effectiveAllowEgress)
	timeout := time.Duration(req.TimeoutSec) * time.Second

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("X-Sandbox-Host-Session", h.hostID)
	w.WriteHeader(http.StatusOK)

	flusher, _ := w.(http.Flusher)
	var sseMu sync.Mutex
	sendEvent := func(evt ExecStreamEvent) {
		sseMu.Lock()
		defer sseMu.Unlock()
		b, err := json.Marshal(evt)
		if err != nil {
			return
		}
		_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", evt.Type, string(b))
		if flusher != nil {
			flusher.Flush()
		}
	}

	sendEvent(ExecStreamEvent{
		Type:      "start",
		SandboxID: rec.SandboxID,
		HostID:    h.hostID,
		Epoch:     rec.Epoch,
	})

	_, _, exitCode, err := h.engine.ExecStream(r.Context(), rec.SandboxID, req.Command, req.Workdir, mergedEnv, timeout, func(stream, data string) {
		h.touchSandboxActivity(rec.SandboxID)
		sendEvent(ExecStreamEvent{
			Type:      stream,
			Data:      data,
			SandboxID: rec.SandboxID,
			HostID:    h.hostID,
		})
	})

	durMs := time.Since(start).Milliseconds()
	if err != nil && exitCode == -1 {
		sendEvent(ExecStreamEvent{
			Type:      "error",
			SandboxID: rec.SandboxID,
			HostID:    h.hostID,
			ExitCode:  exitCode,
			Error:     err.Error(),
		})
	}

	h.mu.Lock()
	if cur, exists := h.sandboxes[rec.SandboxID]; exists {
		cur.LastActiveAt = time.Now().UTC()
		cur.UpdatedAt = cur.LastActiveAt
		h.sandboxes[rec.SandboxID] = cur
		rec = cur
	}
	h.recordEventLocked("EXEC_STREAM", rec.SandboxID, h.hostID, fmt.Sprintf("%dms", durMs),
		fmt.Sprintf("Streamed %q in %s (exit=%d)", strings.Join(req.Command, " "), rec.SandboxID, exitCode))
	h.mu.Unlock()

	if rec.Persistent {
		_ = h.exportSandboxOverlayToStore(r.Context(), &rec, "post_exec_stream")
	}

	sendEvent(ExecStreamEvent{
		Type:       "exit",
		SandboxID:  rec.SandboxID,
		HostID:     h.hostID,
		Epoch:      rec.Epoch,
		ExitCode:   exitCode,
		DurationMs: durMs,
	})
}

func parseProxyPathAndPort(r *http.Request) (sandboxID string, port int, subPath string, err error) {
	path := r.URL.Path
	if strings.HasPrefix(path, "/proxy/") {
		rest := strings.TrimPrefix(path, "/proxy/")
		parts := strings.SplitN(rest, "/", 3)
		if len(parts) < 2 || strings.TrimSpace(parts[0]) == "" {
			return "", 0, "", fmt.Errorf("invalid proxy URL format; expected /proxy/<sandbox_id>/<port>[/<path>]")
		}
		sandboxID = strings.TrimSpace(parts[0])
		p, pErr := strconv.Atoi(strings.TrimSpace(parts[1]))
		if pErr != nil || p <= 0 || p > 65535 {
			return "", 0, "", fmt.Errorf("invalid port %q in proxy URL", parts[1])
		}
		port = p
		if len(parts) == 3 {
			subPath = "/" + parts[2]
			if strings.HasSuffix(path, "/") && !strings.HasSuffix(subPath, "/") {
				subPath += "/"
			}
		} else {
			subPath = "/"
		}
		return sandboxID, port, subPath, nil
	}

	sandboxID = extractSandboxID(r, "")
	portStr := strings.TrimSpace(r.Header.Get("X-Sandbox-Forward-Port"))
	if portStr == "" {
		portStr = strings.TrimSpace(r.URL.Query().Get("port"))
	}
	if sandboxID == "" || portStr == "" {
		return "", 0, "", fmt.Errorf("sandbox_id and port are required")
	}
	p, pErr := strconv.Atoi(portStr)
	if pErr != nil || p <= 0 || p > 65535 {
		return "", 0, "", fmt.Errorf("invalid port %q", portStr)
	}
	subPath = r.Header.Get("X-Sandbox-Forward-Path")
	if subPath == "" {
		subPath = r.URL.Query().Get("path")
	}
	if subPath == "" {
		subPath = "/"
	}
	if !strings.HasPrefix(subPath, "/") {
		subPath = "/" + subPath
	}
	return sandboxID, p, subPath, nil
}

func (h *HostServer) handleProxy(w http.ResponseWriter, r *http.Request) {
	sandboxID, port, subPath, err := parseProxyPathAndPort(r)
	if err != nil {
		writeHostJSON(w, h, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}

	rec, ok := h.resolveOrAutoHydrateSandbox(r.Context(), w, sandboxID, true)
	if !ok {
		return
	}
	h.beginSandboxActivity(rec.SandboxID)
	defer h.endSandboxActivity(rec.SandboxID)

	w.Header().Set("X-Sandbox-Host-Session", h.hostID)
	w.Header().Set("X-Sandbox-Id", rec.SandboxID)

	proxy := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme = "http"
			pr.Out.URL.Host = fmt.Sprintf("127.0.0.1:%d", port)
			pr.Out.URL.Path = subPath
			pr.Out.URL.RawPath = ""
			pr.Out.Host = fmt.Sprintf("127.0.0.1:%d", port)
		},
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return h.engine.DialSandboxPort(ctx, rec, port)
			},
			DisableKeepAlives: true,
		},
		FlushInterval: -1,
		ErrorHandler: func(rw http.ResponseWriter, req *http.Request, proxyErr error) {
			writeHostJSON(rw, h, http.StatusBadGateway, map[string]any{
				"error":      fmt.Sprintf("failed to proxy to sandbox %s port %d: %v", rec.SandboxID, port, proxyErr),
				"sandbox_id": rec.SandboxID,
				"port":       port,
			})
		},
	}
	proxy.ServeHTTP(w, r)
}

func (h *HostServer) handlePortForward(w http.ResponseWriter, r *http.Request) {
	sandboxID := extractSandboxID(r, "")
	portStr := strings.TrimSpace(r.Header.Get("X-Sandbox-Forward-Port"))
	if portStr == "" {
		portStr = strings.TrimSpace(r.URL.Query().Get("port"))
	}
	port, err := strconv.Atoi(portStr)
	if sandboxID == "" || err != nil || port <= 0 || port > 65535 {
		writeHostJSON(w, h, http.StatusBadRequest, map[string]any{"error": "valid sandbox_id and port are required"})
		return
	}

	rec, ok := h.resolveOrAutoHydrateSandbox(r.Context(), w, sandboxID, true)
	if !ok {
		return
	}
	h.beginSandboxActivity(rec.SandboxID)
	defer h.endSandboxActivity(rec.SandboxID)

	upstreamConn, err := h.engine.DialSandboxPort(r.Context(), rec, port)
	if err != nil {
		writeHostJSON(w, h, http.StatusBadGateway, map[string]any{
			"error":      fmt.Sprintf("failed to connect to sandbox %s port %d: %v", rec.SandboxID, port, err),
			"sandbox_id": rec.SandboxID,
			"port":       port,
		})
		return
	}
	defer upstreamConn.Close()

	if strings.EqualFold(r.Header.Get("Connection"), "Upgrade") || r.Header.Get("Upgrade") != "" {
		if hj, ok := w.(http.Hijacker); ok {
			clientConn, brw, hErr := hj.Hijack()
			if hErr != nil {
				return
			}
			defer clientConn.Close()
			upgradeProto := strings.TrimSpace(r.Header.Get("Upgrade"))
			if upgradeProto == "" {
				upgradeProto = "websocket"
			}
			wsAcceptHdr := ""
			if wsKey := strings.TrimSpace(r.Header.Get("Sec-WebSocket-Key")); wsKey != "" {
				sum := sha1.Sum([]byte(wsKey + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
				wsAcceptHdr = "Sec-WebSocket-Accept: " + base64.StdEncoding.EncodeToString(sum[:]) + "\r\n"
			}
			_, _ = fmt.Fprintf(clientConn, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: %s\r\nConnection: Upgrade\r\n%sX-Sandbox-Host-Session: %s\r\n\r\n", upgradeProto, wsAcceptHdr, h.hostID)
			if brw != nil && brw.Reader.Buffered() > 0 {
				buffered := make([]byte, brw.Reader.Buffered())
				if n, _ := brw.Read(buffered); n > 0 {
					_, _ = upstreamConn.Write(buffered[:n])
				}
			}
			done := make(chan struct{}, 2)
			go func() {
				_, _ = io.Copy(upstreamConn, clientConn)
				done <- struct{}{}
			}()
			go func() {
				_, _ = io.Copy(clientConn, upstreamConn)
				done <- struct{}{}
			}()
			<-done
			return
		}
	}

	// Fallback HTTP streaming tunnel (e.g. HTTP/2 or non-Upgrade POST)
	w.Header().Set("X-Sandbox-Host-Session", h.hostID)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	if r.Body != nil {
		go func() {
			defer r.Body.Close()
			_, _ = io.Copy(upstreamConn, r.Body)
		}()
	}
	buf := make([]byte, 32*1024)
	for {
		n, rErr := upstreamConn.Read(buf)
		if n > 0 {
			h.touchSandboxActivity(rec.SandboxID)
			if _, wErr := w.Write(buf[:n]); wErr != nil {
				break
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if rErr != nil {
			break
		}
	}
}

func (h *HostServer) handleCopyFile(w http.ResponseWriter, r *http.Request) {
	var req FileCopyRequest
	if r.Method == http.MethodGet {
		req.SandboxID = extractSandboxID(r, "")
		req.Direction = "download"
		req.Path = r.URL.Query().Get("path")
	} else {
		if err := json.NewDecoder(io.LimitReader(r.Body, 32<<20)).Decode(&req); err != nil {
			writeHostJSON(w, h, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		if req.SandboxID == "" {
			req.SandboxID = extractSandboxID(r, "")
		}
	}
	if req.Path == "" {
		writeHostJSON(w, h, http.StatusBadRequest, map[string]any{"error": "path is required"})
		return
	}
	if strings.Contains(req.Path, "..") {
		writeHostJSON(w, h, http.StatusBadRequest, map[string]any{"error": ErrPathTraversal.Error()})
		return
	}

	rec, ok := h.resolveOrAutoHydrateSandbox(r.Context(), w, req.SandboxID, true)
	if !ok {
		return
	}
	h.beginSandboxActivity(rec.SandboxID)
	defer h.endSandboxActivity(rec.SandboxID)

	resp, err := h.engine.CopyFile(r.Context(), rec.SandboxID, req)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, ErrPathTraversal) {
			status = http.StatusBadRequest
		} else if os.IsNotExist(err) {
			status = http.StatusNotFound
		}
		writeHostJSON(w, h, status, map[string]any{"error": err.Error()})
		return
	}
	if rec.Persistent && !strings.EqualFold(req.Direction, "download") {
		_ = h.exportSandboxOverlayToStore(r.Context(), &rec, "post_cp")
	}
	writeHostJSON(w, h, http.StatusOK, resp)
}

func (h *HostServer) handleLogs(w http.ResponseWriter, r *http.Request) {
	sandboxID := extractSandboxID(r, "")
	rec, ok := h.resolveOrAutoHydrateSandbox(r.Context(), w, sandboxID, false)
	if !ok {
		return
	}
	logs, err := h.engine.ReadLogs(r.Context(), rec.SandboxID)
	if err != nil {
		writeHostJSON(w, h, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if tailStr := strings.TrimSpace(r.URL.Query().Get("tail")); tailStr != "" {
		if tailN, tErr := strconv.Atoi(tailNStr(tailStr)); tErr == nil && tailN > 0 {
			logs = tailLogLines(logs, tailN)
		}
	}
	writeHostJSON(w, h, http.StatusOK, map[string]any{
		"sandbox_id": rec.SandboxID,
		"host_id":    h.hostID,
		"logs":       logs,
	})
}

func tailNStr(s string) string { return strings.TrimSpace(s) }

func tailLogLines(raw string, n int) string {
	if n <= 0 || raw == "" {
		return raw
	}
	trimmed := strings.TrimRight(raw, "\n")
	if trimmed == "" {
		return ""
	}
	lines := strings.Split(trimmed, "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

func (h *HostServer) handleInspect(w http.ResponseWriter, r *http.Request) {
	sandboxID := extractSandboxID(r, "")
	rec, ok := h.resolveOrAutoHydrateSandbox(r.Context(), w, sandboxID, false)
	if !ok {
		return
	}
	writeHostJSON(w, h, http.StatusOK, rec)
}

func (h *HostServer) handleUpdate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SandboxID          string            `json:"sandbox_id"`
		CPU                string            `json:"cpu"`
		Memory             string            `json:"memory"`
		AllowEgress        *bool             `json:"allow_egress"`
		IdleTimeoutSec     *int              `json:"idle_timeout_sec"`
		DisableAutoSuspend *bool             `json:"disable_auto_suspend"`
		Env                map[string]string `json:"env"`
		Metadata           map[string]string `json:"metadata"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeHostJSON(w, h, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if req.SandboxID == "" {
		req.SandboxID = extractSandboxID(r, "")
	}
	if err := ValidateCPUMemory(req.CPU, req.Memory); err != nil {
		writeHostJSON(w, h, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	rec, ok := h.resolveOrAutoHydrateSandbox(r.Context(), w, req.SandboxID, false)
	if !ok {
		return
	}

	h.mu.Lock()
	_, isLocalRunning := h.sandboxes[rec.SandboxID]
	isRunningOnThisHost := isLocalRunning && rec.Status == StatusRunning
	prevEgress := rec.AllowEgress
	if req.CPU != "" {
		rec.CPU = req.CPU
	}
	if req.Memory != "" {
		rec.Memory = req.Memory
	}
	if req.AllowEgress != nil {
		rec.AllowEgress = *req.AllowEgress
	}
	if req.IdleTimeoutSec != nil {
		rec.IdleTimeoutSec = *req.IdleTimeoutSec
		if *req.IdleTimeoutSec > 0 {
			rec.Persistent = true
			rec.DisableAutoSuspend = false
		} else {
			rec.DisableAutoSuspend = true
		}
	}
	if req.DisableAutoSuspend != nil {
		rec.DisableAutoSuspend = *req.DisableAutoSuspend
	}
	if req.Env != nil {
		if rec.Env == nil {
			rec.Env = make(map[string]string)
		}
		for k, v := range req.Env {
			rec.Env[k] = v
		}
	}
	if req.Metadata != nil {
		if rec.Metadata == nil {
			rec.Metadata = make(map[string]string)
		}
		for k, v := range req.Metadata {
			rec.Metadata[k] = v
		}
	}
	rec.UpdatedAt = time.Now().UTC()
	if isRunningOnThisHost {
		h.sandboxes[rec.SandboxID] = rec
		hostRecAfter := h.snapshotHostRecordLocked()
		h.appendMetricsSampleLocked(hostRecAfter)
		if updatedSbx, exists := h.sandboxes[rec.SandboxID]; exists {
			rec = updatedSbx
		}
	}
	h.mu.Unlock()

	if isRunningOnThisHost && req.AllowEgress != nil && rec.AllowEgress != prevEgress {
		if lp, ok := h.engine.(*LocalProcessEngine); ok {
			lp.SetAllowEgress(rec.SandboxID, rec.AllowEgress)
		} else if sg, ok := h.engine.(*CloudRunSandboxEngine); ok && rec.Status == StatusRunning {
			// Recreate Cloud Run Sandbox Runtime container with updated --allow-egress while preserving overlay state
			tmpTar := filepath.Join(os.TempDir(), "egress-update-"+NewRandomID("")+".tar")
			if sg.ExportTar(r.Context(), rec, tmpTar) == nil {
				_ = sg.Stop(r.Context(), rec.SandboxID)
				_ = sg.Start(r.Context(), rec, tmpTar)
			}
			_ = os.Remove(tmpTar)
		}
	}

	if h.store != nil {
		_, _ = h.store.WriteJSONCAS(r.Context(), "sandboxes/"+rec.SandboxID+".json", rec, -1)
	}
	if isRunningOnThisHost {
		_ = h.SyncStateAndHeartbeat(r.Context())
	}
	writeHostJSON(w, h, http.StatusOK, rec)
}

func (h *HostServer) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SandboxID  string `json:"sandbox_id"`
		SnapshotID string `json:"snapshot_id"`
		Force      bool   `json:"force"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req)
	}
	if r.URL.Query().Get("force") == "true" {
		req.Force = true
	}
	if req.SandboxID == "" {
		req.SandboxID = extractSandboxID(r, "")
	}
	if req.SnapshotID == "" {
		req.SnapshotID = NewRandomID("snap")
	}
	if err := ValidateID(req.SnapshotID); err != nil {
		writeHostJSON(w, h, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if !req.Force && h.store != nil {
		var existingSnap SnapshotRecord
		if _, okSnap, _ := h.store.ReadJSON(r.Context(), "snapshots/"+req.SnapshotID+".json", &existingSnap); okSnap && existingSnap.SnapshotID != "" {
			writeHostJSON(w, h, http.StatusConflict, map[string]any{
				"error":       "snapshot_already_exists",
				"message":     fmt.Sprintf("snapshot %q already exists", req.SnapshotID),
				"snapshot_id": req.SnapshotID,
			})
			return
		}
	}

	rec, ok := h.resolveOrAutoHydrateSandbox(r.Context(), w, req.SandboxID, true)
	if !ok {
		return
	}

	sbxMu := h.getSandboxLock(rec.SandboxID)
	sbxMu.Lock()
	defer sbxMu.Unlock()

	tarBlobPath := fmt.Sprintf("snapshots/%s.tar", req.SnapshotID)
	localTar, commit, err := h.store.LocalBlobPath(r.Context(), tarBlobPath, true)
	if err != nil {
		writeHostJSON(w, h, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if err := h.engine.ExportTar(r.Context(), rec, localTar); err != nil {
		writeHostJSON(w, h, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	var sizeBytes int64
	if st, statErr := os.Stat(localTar); statErr == nil {
		sizeBytes = st.Size()
	}
	if err := commit(); err != nil {
		writeHostJSON(w, h, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}

	snap := SnapshotRecord{
		SnapshotID:    req.SnapshotID,
		SourceSandbox: rec.SandboxID,
		Template:      rec.Template,
		TarPath:       tarBlobPath,
		SizeBytes:     sizeBytes,
		CreatedAt:     time.Now().UTC(),
	}
	if h.store != nil {
		_, _ = h.store.WriteJSONCAS(r.Context(), "snapshots/"+req.SnapshotID+".json", snap, -1)
	}
	h.mu.Lock()
	h.recordEventLocked("SNAPSHOT", rec.SandboxID, req.SnapshotID, "",
		fmt.Sprintf("Created snapshot %s (%d bytes) from %s", req.SnapshotID, sizeBytes, rec.SandboxID))
	h.mu.Unlock()
	_ = h.SyncStateAndHeartbeat(r.Context())

	writeHostJSON(w, h, http.StatusOK, snap)
}

func (h *HostServer) handleCopySandbox(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var req CopySandboxRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		writeHostJSON(w, h, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if req.SourceSandboxID == "" {
		req.SourceSandboxID = extractSandboxID(r, "")
	}
	targetName := strings.TrimSpace(req.Name)
	if targetName == "" {
		targetName = strings.TrimSpace(req.TargetSandboxID)
	}
	if targetName != "" {
		if err := ValidateID(targetName); err != nil {
			writeHostJSON(w, h, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
	}
	if req.SourceSandboxID != "" && req.TargetSandboxID != "" && req.SourceSandboxID == req.TargetSandboxID {
		writeHostJSON(w, h, http.StatusConflict, map[string]any{"error": "source and target sandbox IDs must be different"})
		return
	}

	srcRec, ok := h.resolveOrAutoHydrateSandbox(r.Context(), w, req.SourceSandboxID, true)
	if !ok {
		return
	}
	if targetName != "" && targetName == srcRec.SandboxID {
		writeHostJSON(w, h, http.StatusConflict, map[string]any{"error": "source and target sandbox IDs must be different"})
		return
	}

	// Check if same-host CoW fork is possible
	h.mu.Lock()
	activeCount := 0
	for _, s := range h.sandboxes {
		if s.Status == StatusRunning || s.Status == StatusCreating || s.Status == StatusMigrating {
			activeCount++
		}
	}
	canForkLocally := (req.TargetHostID == "" || req.TargetHostID == h.hostID) && h.status == HostActive && activeCount < h.maxDensity
	now := time.Now().UTC()
	engType := "local"
	if h.engine != nil {
		engType = h.engine.EngineType()
	}
	req.TargetSandboxID = GenerateDistributedSandboxID(h.hostID)
	if targetName == "" {
		targetName = req.TargetSandboxID
	}
	targetRec := SandboxRecord{
		SandboxID:          req.TargetSandboxID,
		Name:               targetName,
		HostID:             h.hostID,
		HostRevision:       h.revision,
		EngineType:         engType,
		PinnedHost:         req.TargetHostID != "",
		Epoch:              1,
		Status:             StatusRunning,
		Template:           srcRec.Template,
		Persistent:         req.Persistent || srcRec.Persistent,
		CPU:                srcRec.CPU,
		Memory:             srcRec.Memory,
		AllowEgress:        srcRec.AllowEgress,
		Env:                srcRec.Env,
		Metadata:           map[string]string{"forked_from": srcRec.SandboxID},
		BackgroundCommands: append([]string(nil), srcRec.BackgroundCommands...),
		OverlayPath:        fmt.Sprintf("persistence/%s/overlay.tar", req.TargetSandboxID),
		CreatedAt:          now,
		UpdatedAt:          now,
		LastActiveAt:       now,
	}
	if canForkLocally {
		h.sandboxes[targetRec.SandboxID] = targetRec
	}
	h.mu.Unlock()

	if canForkLocally {
		forkMethod, err := h.engine.Fork(r.Context(), srcRec.SandboxID, targetRec)
		if err != nil {
			h.mu.Lock()
			delete(h.sandboxes, targetRec.SandboxID)
			h.mu.Unlock()
			writeHostJSON(w, h, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		targetRec.Metadata["fork_method"] = forkMethod
		if targetRec.Persistent {
			_ = h.exportSandboxOverlayToStore(r.Context(), &targetRec, "fork_init")
		}
		if h.store != nil {
			_, _ = h.store.WriteJSONCAS(r.Context(), "sandboxes/"+targetRec.SandboxID+".json", targetRec, -1)
		}
		h.mu.Lock()
		h.sandboxes[targetRec.SandboxID] = targetRec
		h.recordEventLocked("COPY", targetRec.SandboxID, h.hostID, fmt.Sprintf("%dms", time.Since(start).Milliseconds()),
			fmt.Sprintf("Cloned %s -> %s (name=%s) via %s on %s", srcRec.SandboxID, targetRec.SandboxID, targetRec.Name, forkMethod, h.hostID))
		h.mu.Unlock()
		_ = h.SyncStateAndHeartbeat(r.Context())
		writeHostJSON(w, h, http.StatusOK, targetRec)
		return
	}

	// Cross-host clone: export overlay tar of srcRec, select/mint target host, and hydrate!
	if err := h.exportSandboxOverlayToStore(r.Context(), &srcRec, "cross_host_copy"); err != nil {
		writeHostJSON(w, h, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	targetHost := req.TargetHostID
	if targetHost == "" || targetHost == h.hostID {
		targets := h.requestMigrationTargets(r.Context(), []string{targetRec.SandboxID}, "cross_host_copy")
		targetHost = targets[targetRec.SandboxID]
	}
	if targetHost == "" || targetHost == h.hostID {
		targetHost = NewRandomID("host")
	}
	targetRec.SandboxID = GenerateDistributedSandboxID(targetHost)
	targetRec.OverlayPath = fmt.Sprintf("persistence/%s/overlay.tar", targetRec.SandboxID)
	// Copy the overlay blob to targetRec.OverlayPath
	if h.store != nil {
		if rc, exists, err := h.store.ReadBlob(r.Context(), srcRec.OverlayPath); err == nil && exists {
			_, _ = h.store.WriteBlob(r.Context(), targetRec.OverlayPath, rc)
			_ = rc.Close()
		}
	}
	targetRec.HostID = targetHost
	targetRec.Metadata["fork_method"] = "cross_host_snapshot_clone"
	if h.store != nil {
		_, _ = h.store.WriteJSONCAS(r.Context(), "sandboxes/"+targetRec.SandboxID+".json", targetRec, -1)
	}
	hydrated, err := h.sendHydrateToHost(r.Context(), targetHost, HydrateRequest{
		Record:     targetRec,
		OverlayTar: targetRec.OverlayPath,
		Reason:     "cross_host_copy",
	})
	if err != nil {
		writeHostJSON(w, h, http.StatusInternalServerError, map[string]any{"error": "cross-host copy hydrate failed: " + err.Error()})
		return
	}
	writeHostJSON(w, h, http.StatusOK, hydrated)
}

func (h *HostServer) handleMigrate(w http.ResponseWriter, r *http.Request) {
	var req MigrateSandboxRequest
	if r.Body != nil {
		_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req)
	}
	if req.SandboxID == "" {
		req.SandboxID = extractSandboxID(r, "")
	}
	rec, ok := h.resolveOrAutoHydrateSandbox(r.Context(), w, req.SandboxID, true)
	if !ok {
		return
	}
	if req.TargetHostID == h.hostID {
		writeHostJSON(w, h, http.StatusOK, rec)
		return
	}

	targetHost := req.TargetHostID
	if targetHost == "" {
		targets := h.requestMigrationTargets(r.Context(), []string{rec.SandboxID}, "cli_migrate")
		targetHost = targets[rec.SandboxID]
	}
	if targetHost == "" || targetHost == h.hostID {
		targetHost = NewRandomID("host")
	}

	sbxMu := h.getSandboxLock(rec.SandboxID)
	sbxMu.Lock()

	h.mu.Lock()
	cur, exists := h.sandboxes[rec.SandboxID]
	if !exists {
		migTarget := h.migratedTo[rec.SandboxID]
		h.mu.Unlock()
		sbxMu.Unlock()
		if migTarget != "" && migTarget != h.hostID {
			w.Header().Set("X-Sandbox-Host-Session", migTarget)
			writeHostJSON(w, h, http.StatusConflict, MisdirectedSandboxError{
				ErrorCode:     "misdirected_sandbox",
				SandboxID:     rec.SandboxID,
				HostSessionID: migTarget,
				Status:        StatusRunning,
			})
			return
		}
		writeHostJSON(w, h, http.StatusNotFound, map[string]any{"error": "sandbox no longer on host"})
		return
	}
	cur.Status = StatusMigrating
	if req.PinnedHost {
		cur.PinnedHost = true
	}
	h.sandboxes[rec.SandboxID] = cur
	rec = cur
	h.mu.Unlock()

	if err := h.exportSandboxOverlayToStoreLocked(r.Context(), &rec, "cli_migrate"); err != nil {
		h.mu.Lock()
		rec.Status = StatusRunning
		h.sandboxes[rec.SandboxID] = rec
		h.mu.Unlock()
		_ = h.engine.Start(r.Context(), rec, "")
		sbxMu.Unlock()
		writeHostJSON(w, h, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}

	reason := "cli_migrate"
	if req.PinnedHost || req.TargetHostID != "" {
		reason = "cli_migrate_pinned"
	}
	hydrated, err := h.sendHydrateToHost(r.Context(), targetHost, HydrateRequest{
		Record:     rec,
		OverlayTar: rec.OverlayPath,
		Reason:     reason,
	})
	if err != nil && req.TargetHostID == "" {
		fallbackHost := NewRandomID("host")
		if h2, retryErr := h.sendHydrateToHost(r.Context(), fallbackHost, HydrateRequest{
			Record:     rec,
			OverlayTar: rec.OverlayPath,
			Reason:     reason,
		}); retryErr == nil {
			hydrated = h2
			err = nil
		}
	}
	if err != nil {
		h.mu.Lock()
		rec.Status = StatusRunning
		h.sandboxes[rec.SandboxID] = rec
		h.mu.Unlock()
		_ = h.engine.Start(r.Context(), rec, "")
		sbxMu.Unlock()
		writeHostJSON(w, h, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}

	h.mu.Lock()
	h.migratedTo[rec.SandboxID] = hydrated.HostID
	delete(h.sandboxes, rec.SandboxID)
	h.snapshotHostRecordLocked()
	h.mu.Unlock()
	_ = h.engine.Stop(r.Context(), rec.SandboxID)
	sbxMu.Unlock()

	_ = h.SyncStateAndHeartbeat(r.Context())
	writeHostJSON(w, h, http.StatusOK, hydrated)
}

func (h *HostServer) stopOrSuspendSandbox(ctx context.Context, sandboxID string, targetStatus SandboxStatus, reason string) (SandboxRecord, error) {
	sbxMu := h.getSandboxLock(sandboxID)
	sbxMu.Lock()
	defer sbxMu.Unlock()

	h.mu.RLock()
	rec, ok := h.sandboxes[sandboxID]
	h.mu.RUnlock()
	if !ok {
		return SandboxRecord{}, ErrNotFound
	}
	if err := h.exportSandboxOverlayToStoreLocked(ctx, &rec, reason); err != nil {
		return SandboxRecord{}, err
	}
	_ = h.engine.Stop(ctx, sandboxID)
	rec.Status = targetStatus
	rec.UpdatedAt = time.Now().UTC()

	h.mu.Lock()
	delete(h.sandboxes, sandboxID)
	h.recordEventLocked(string(targetStatus), sandboxID, h.hostID, "",
		fmt.Sprintf("Sandbox %s transitioned to %s (%s); slot released on %s", sandboxID, targetStatus, reason, h.hostID))
	h.snapshotHostRecordLocked()
	h.mu.Unlock()

	if h.store != nil {
		_, _ = h.store.WriteJSONCAS(ctx, "sandboxes/"+sandboxID+".json", rec, -1)
	}
	_ = h.SyncStateAndHeartbeat(ctx)
	return rec, nil
}

func (h *HostServer) handleStopOrSuspend(w http.ResponseWriter, r *http.Request, targetStatus SandboxStatus) {
	var req struct {
		SandboxID string `json:"sandbox_id"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req)
	}
	sandboxID := extractSandboxID(r, req.SandboxID)
	rec, ok := h.resolveOrAutoHydrateSandbox(r.Context(), w, sandboxID, false)
	if !ok {
		return
	}
	sandboxID = rec.SandboxID
	if rec.Status == StatusSuspended || rec.Status == StatusStopped {
		rec.Status = targetStatus
		if h.store != nil {
			_, _ = h.store.WriteJSONCAS(r.Context(), "sandboxes/"+sandboxID+".json", rec, -1)
		}
		writeHostJSON(w, h, http.StatusOK, rec)
		return
	}
	updated, err := h.stopOrSuspendSandbox(r.Context(), sandboxID, targetStatus, "api_request")
	if err != nil {
		writeHostJSON(w, h, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeHostJSON(w, h, http.StatusOK, updated)
}

func (h *HostServer) handleStartOrResume(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SandboxID string `json:"sandbox_id"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req)
	}
	sandboxID := extractSandboxID(r, req.SandboxID)
	rec, ok := h.resolveOrAutoHydrateSandbox(r.Context(), w, sandboxID, true)
	if !ok {
		return
	}
	writeHostJSON(w, h, http.StatusOK, rec)
}

func (h *HostServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SandboxID string `json:"sandbox_id"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req)
	}
	sandboxID := extractSandboxID(r, req.SandboxID)
	if err := ValidateID(sandboxID); err != nil {
		writeHostJSON(w, h, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	resolvedID, ambErr := h.resolveSandboxIdentifier(r.Context(), sandboxID)
	if ambErr != nil {
		writeHostJSON(w, h, http.StatusConflict, map[string]any{
			"error":      ambErr.Error(),
			"error_code": "ambiguous_sandbox_name",
		})
		return
	}
	sandboxID = resolvedID

	h.mu.RLock()
	_, isLocal := h.sandboxes[sandboxID]
	h.mu.RUnlock()

	if !isLocal && h.store != nil {
		var storeRec SandboxRecord
		_, exists, _ := h.store.ReadJSON(r.Context(), "sandboxes/"+sandboxID+".json", &storeRec)
		if !exists || storeRec.Status == StatusDeleted {
			writeHostJSON(w, h, http.StatusNotFound, map[string]any{
				"error":      fmt.Sprintf("sandbox %q not found", sandboxID),
				"sandbox_id": sandboxID,
			})
			return
		}
		// If the sandbox is RUNNING on another active host, redirect with HTTP 409 so the CLI deletes it on the true owner!
		if storeRec.Status == StatusRunning && storeRec.HostID != "" && storeRec.HostID != h.hostID {
			otherDead := false
			var otherHost HostRecord
			if _, hExists, _ := h.store.ReadJSON(r.Context(), "hosts/"+storeRec.HostID+".json", &otherHost); hExists {
				if otherHost.Status == HostDead || otherHost.Status == HostCrashed || time.Since(otherHost.LastHeartbeatAt) > 20*time.Second {
					otherDead = true
				}
			} else {
				otherDead = true
			}
			if !otherDead {
				w.Header().Set("X-Sandbox-Host-Session", storeRec.HostID)
				writeHostJSON(w, h, http.StatusConflict, MisdirectedSandboxError{
					ErrorCode:     "misdirected_sandbox",
					SandboxID:     sandboxID,
					HostSessionID: storeRec.HostID,
					Epoch:         storeRec.Epoch,
					Status:        storeRec.Status,
				})
				return
			}
		}
	}

	_ = h.engine.Stop(r.Context(), sandboxID)
	h.mu.Lock()
	delete(h.sandboxes, sandboxID)
	h.recordEventLocked("DELETE", sandboxID, h.hostID, "", fmt.Sprintf("Deleted sandbox %s on %s", sandboxID, h.hostID))
	h.snapshotHostRecordLocked()
	h.mu.Unlock()

	if h.store != nil {
		_ = h.store.DeleteObject(r.Context(), "sandboxes/"+sandboxID+".json")
		_ = h.store.DeleteObject(r.Context(), fmt.Sprintf("persistence/%s/overlay.tar", sandboxID))
	}
	_ = h.SyncStateAndHeartbeat(r.Context())
	writeHostJSON(w, h, http.StatusOK, map[string]any{
		"sandbox_id": sandboxID,
		"status":     StatusDeleted,
		"host_id":    h.hostID,
	})
}

func (h *HostServer) hydrateSandboxInternal(ctx context.Context, rec SandboxRecord, overlayTarPath string, reason string, expectedGen int64) (SandboxRecord, error) {
	start := time.Now()
	if err := ValidateID(rec.SandboxID); err != nil {
		return SandboxRecord{}, err
	}
	sbxMu := h.getSandboxLock(rec.SandboxID)
	sbxMu.Lock()
	defer sbxMu.Unlock()

	h.mu.Lock()
	if h.status == HostDraining {
		h.mu.Unlock()
		return SandboxRecord{}, ErrHostDraining
	}
	if existing, ok := h.sandboxes[rec.SandboxID]; ok && existing.Status == StatusRunning {
		h.mu.Unlock()
		return existing, nil
	}
	activeCount := 0
	for _, s := range h.sandboxes {
		if s.Status == StatusRunning || s.Status == StatusCreating || s.Status == StatusMigrating {
			activeCount++
		}
	}
	if activeCount >= h.maxDensity {
		h.mu.Unlock()
		return SandboxRecord{}, ErrHostFull
	}
	if activeCount > 0 && reason != "cli_migrate_pinned" && reason != "crash_recovery" {
		recSnap := h.snapshotHostRecordLocked()
		if recSnap.OverloadState == "ADMISSION_BLOCKED" || recSnap.OverloadState == "HOT_SHEDDING" {
			h.mu.Unlock()
			return SandboxRecord{}, ErrHostFull
		}
	}

	if overlayTarPath == "" {
		overlayTarPath = rec.OverlayPath
	}
	if overlayTarPath == "" {
		overlayTarPath = fmt.Sprintf("persistence/%s/overlay.tar", rec.SandboxID)
	}
	prevHost := rec.HostID
	engType := "local"
	if h.engine != nil {
		engType = h.engine.EngineType()
	}
	rec.PreviousHostID = prevHost
	rec.HostID = h.hostID
	rec.HostRevision = h.revision
	rec.EngineType = engType
	rec.Epoch++
	// Mark as CREATING until engine.Start completes so concurrent exec/hydrate waits on sbxMu!
	rec.Status = StatusCreating
	rec.OverlayPath = overlayTarPath
	now := time.Now().UTC()
	rec.UpdatedAt = now
	rec.LastActiveAt = now
	if reason == "crash_recovery" {
		rec.CrashRecoveries++
	} else if reason != "auto_resume" {
		rec.MigrationCount++
	}
	if reason == "cli_migrate_pinned" {
		rec.PinnedHost = true
	}
	h.sandboxes[rec.SandboxID] = rec
	delete(h.migratedTo, rec.SandboxID)
	SetInstanceBusyState(true)
	h.mu.Unlock()

	var localTar string
	var cleanup func() error
	if h.store != nil && overlayTarPath != "" {
		if p, cleanFn, err := h.store.LocalBlobPath(ctx, overlayTarPath, false); err == nil {
			localTar = p
			cleanup = cleanFn
		}
	}
	if cleanup != nil {
		defer func() { _ = cleanup() }()
	}

	if err := h.engine.Start(ctx, rec, localTar); err != nil {
		h.mu.Lock()
		delete(h.sandboxes, rec.SandboxID)
		h.snapshotHostRecordLocked()
		h.mu.Unlock()
		return SandboxRecord{}, err
	}
	rec.Status = StatusRunning
	if lp, ok := h.engine.(*LocalProcessEngine); ok && strings.Contains(strings.ToLower(rec.Template), "postgres") {
		rec.PostgresPort = lp.postgresPortFor(rec.SandboxID)
	}

	// Re-launch any persistent background commands (and /workspace/.sbx_init.sh if present)
	for _, bgCmd := range rec.BackgroundCommands {
		if bgCmd != "" {
			_, _, _, _, _ = h.engine.Exec(ctx, rec.SandboxID, []string{"sh", "-c", bgCmd}, "/workspace", rec.Env, true, 30*time.Second)
		}
	}
	_, _, _, _, _ = h.engine.Exec(ctx, rec.SandboxID, []string{"sh", "-c", "if [ -f /workspace/.sbx_init.sh ]; then sh /workspace/.sbx_init.sh >/tmp/sbx_init.log 2>&1 </dev/null & fi"}, "/workspace", rec.Env, false, 10*time.Second)

	downtimeMs := time.Since(start).Milliseconds()
	rec.TotalDowntimeMs += downtimeMs

	if h.store != nil {
		if expectedGen > 0 {
			if _, err := h.store.WriteJSONCAS(ctx, "sandboxes/"+rec.SandboxID+".json", rec, expectedGen); errors.Is(err, ErrPreconditionFailed) {
				var latest SandboxRecord
				if g2, ok, _ := h.store.ReadJSON(ctx, "sandboxes/"+rec.SandboxID+".json", &latest); ok {
					if latest.Status == StatusRunning && latest.HostID != "" && latest.HostID != h.hostID {
						_ = h.engine.Stop(ctx, rec.SandboxID)
						h.mu.Lock()
						delete(h.sandboxes, rec.SandboxID)
						h.migratedTo[rec.SandboxID] = latest.HostID
						h.snapshotHostRecordLocked()
						h.mu.Unlock()
						return latest, fmt.Errorf("conflict: sandbox concurrently hydrated on host %s", latest.HostID)
					}
					rec.Epoch = max(rec.Epoch, latest.Epoch+1)
					_, _ = h.store.WriteJSONCAS(ctx, "sandboxes/"+rec.SandboxID+".json", rec, g2)
				}
			}
		} else {
			_, _ = h.store.WriteJSONCAS(ctx, "sandboxes/"+rec.SandboxID+".json", rec, -1)
		}
	}

	evtType := "MIGRATE"
	if reason == "crash_recovery" {
		evtType = "CRASH_RECOVERED"
	} else if reason == "auto_resume" {
		evtType = "RESUME"
	} else if reason == "hot_host_shedding" {
		evtType = "HOT_MIGRATE"
	}
	h.mu.Lock()
	h.sandboxes[rec.SandboxID] = rec
	h.recordEventLocked(evtType, rec.SandboxID, h.hostID, fmt.Sprintf("%dms", downtimeMs),
		fmt.Sprintf("Hydrated sandbox %s on %s (from %s, reason=%s, epoch=%d) in %dms", rec.SandboxID, h.hostID, prevHost, reason, rec.Epoch, downtimeMs))
	hostRecAfter := h.snapshotHostRecordLocked()
	h.appendMetricsSampleLocked(hostRecAfter)
	if updatedSbx, exists := h.sandboxes[rec.SandboxID]; exists {
		rec = updatedSbx
	}
	h.mu.Unlock()

	_ = h.SyncStateAndHeartbeat(ctx)
	return rec, nil
}

func (h *HostServer) handleInternalHydrate(w http.ResponseWriter, r *http.Request) {
	var req HydrateRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 2<<20)).Decode(&req); err != nil {
		writeHostJSON(w, h, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	var gen int64 = -1
	if h.store != nil {
		var cur SandboxRecord
		if g, ok, _ := h.store.ReadJSON(r.Context(), "sandboxes/"+req.Record.SandboxID+".json", &cur); ok {
			gen = g
			if cur.Epoch > req.Record.Epoch {
				req.Record.Epoch = cur.Epoch
			}
		}
	}
	hydrated, err := h.hydrateSandboxInternal(r.Context(), req.Record, req.OverlayTar, req.Reason, gen)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, ErrHostFull) {
			status = http.StatusTooManyRequests
		} else if errors.Is(err, ErrHostDraining) {
			status = http.StatusServiceUnavailable
		}
		writeHostJSON(w, h, status, map[string]any{"error": err.Error()})
		return
	}
	writeHostJSON(w, h, http.StatusOK, hydrated)
}

func (h *HostServer) handleInternalDrain(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Reason  string            `json:"reason"`
		Targets map[string]string `json:"targets,omitempty"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req)
	}
	if req.Reason == "" {
		req.Reason = "manual_drain"
	}
	migrated, err := h.DrainAndEvacuate(r.Context(), req.Reason, req.Targets)
	if err != nil {
		writeHostJSON(w, h, http.StatusInternalServerError, map[string]any{
			"error":    err.Error(),
			"migrated": migrated,
		})
		return
	}
	writeHostJSON(w, h, http.StatusOK, map[string]any{
		"host_id":  h.hostID,
		"status":   h.HostState().Status,
		"migrated": migrated,
	})
}

func (h *HostServer) handleInternalCheckOverload(w http.ResponseWriter, r *http.Request) {
	shed, err := h.checkResourceOverloadAndShed(r.Context())
	if len(shed) == 0 {
		h.mu.RLock()
		for k, v := range h.lastShedSandboxes {
			shed[k] = v
		}
		h.mu.RUnlock()
	}
	var firstShedID string
	for k := range shed {
		firstShedID = k
		break
	}
	state := h.HostState()
	if err != nil {
		writeHostJSON(w, h, http.StatusInternalServerError, map[string]any{
			"error":                    err.Error(),
			"host_id":                  state.HostID,
			"overload_state":           state.OverloadState,
			"used_memory_pct":          state.UsedMemoryPct,
			"cpu_usage_pct":            state.CPUUsagePct,
			"shed":                     len(shed) > 0,
			"sandbox_id":               firstShedID,
			"shed_sandboxes":           shed,
			"hot_migrations_triggered": state.HotMigrationsTriggered,
		})
		return
	}
	writeHostJSON(w, h, http.StatusOK, map[string]any{
		"host_id":                  state.HostID,
		"overload_state":           state.OverloadState,
		"used_memory_pct":          state.UsedMemoryPct,
		"cpu_usage_pct":            state.CPUUsagePct,
		"shed":                     len(shed) > 0,
		"sandbox_id":               firstShedID,
		"shed_sandboxes":           shed,
		"hot_migrations_triggered": state.HotMigrationsTriggered,
	})
}

func (h *HostServer) checkResourceOverloadAndShed(ctx context.Context) (map[string]string, error) {
	h.shedMu.Lock()
	defer h.shedMu.Unlock()

	if h.IsCrashed() {
		return map[string]string{}, nil
	}
	h.syncConfigFromStore(ctx)

	h.mu.Lock()
	if h.hostID == "" || h.status != HostActive || strings.HasPrefix(h.hostID, "host-endurance-") {
		h.mu.Unlock()
		return map[string]string{}, nil
	}
	state := h.snapshotHostRecordLocked()
	isHotMem := h.hotMemoryThresholdPct > 0 && state.UsedMemoryPct >= h.hotMemoryThresholdPct
	isHotCPU := h.hotCPUThresholdPct > 0 && state.CPUUsagePct >= h.hotCPUThresholdPct
	if !isHotMem && !isHotCPU {
		h.mu.Unlock()
		return map[string]string{}, nil
	}
	var running []SandboxRecord
	for _, s := range h.sandboxes {
		if s.Status == StatusRunning {
			running = append(running, s)
		}
	}
	if len(running) < 2 {
		h.mu.Unlock()
		return map[string]string{}, nil
	}
	// Select the running sandbox with the largest resource footprint to shed off the hot host!
	sort.Slice(running, func(i, j int) bool {
		if isHotMem && running[i].UsedMemoryBytes != running[j].UsedMemoryBytes {
			return running[i].UsedMemoryBytes > running[j].UsedMemoryBytes
		}
		if running[i].CPUUsagePct != running[j].CPUUsagePct {
			return running[i].CPUUsagePct > running[j].CPUUsagePct
		}
		return running[i].SandboxID < running[j].SandboxID
	})
	victim := running[0]
	h.mu.Unlock()

	sbxMu := h.getSandboxLock(victim.SandboxID)
	sbxMu.Lock()

	h.mu.Lock()
	cur, ok := h.sandboxes[victim.SandboxID]
	if !ok || cur.Status != StatusRunning {
		h.mu.Unlock()
		sbxMu.Unlock()
		return map[string]string{}, nil
	}
	cur.Status = StatusMigrating
	h.sandboxes[victim.SandboxID] = cur
	victim = cur
	h.mu.Unlock()

	targets := h.requestMigrationTargets(ctx, []string{victim.SandboxID}, "hot_host_shedding")
	targetHost := targets[victim.SandboxID]
	if targetHost == "" || targetHost == h.hostID {
		targetHost = NewRandomID("host")
	}

	if err := h.exportSandboxOverlayToStoreLocked(ctx, &victim, "drain_hot_host_shedding"); err != nil {
		h.mu.Lock()
		victim.Status = StatusRunning
		h.sandboxes[victim.SandboxID] = victim
		h.mu.Unlock()
		_ = h.engine.Start(ctx, victim, "")
		sbxMu.Unlock()
		return map[string]string{}, err
	}

	hydrated, err := h.sendHydrateToHost(ctx, targetHost, HydrateRequest{
		Record:     victim,
		OverlayTar: victim.OverlayPath,
		Reason:     "hot_host_shedding",
	})
	if err != nil {
		fallbackHost := NewRandomID("host")
		if h2, retryErr := h.sendHydrateToHost(ctx, fallbackHost, HydrateRequest{
			Record:     victim,
			OverlayTar: victim.OverlayPath,
			Reason:     "hot_host_shedding",
		}); retryErr == nil {
			hydrated = h2
			targetHost = h2.HostID
			err = nil
		}
	} else if hydrated.HostID != "" {
		targetHost = hydrated.HostID
	}
	if err != nil {
		h.mu.Lock()
		victim.Status = StatusRunning
		h.sandboxes[victim.SandboxID] = victim
		h.mu.Unlock()
		_ = h.engine.Start(ctx, victim, "")
		sbxMu.Unlock()
		return map[string]string{}, err
	}

	h.mu.Lock()
	h.migratedTo[victim.SandboxID] = targetHost
	delete(h.sandboxes, victim.SandboxID)
	h.hotMigrationsTriggered++
	h.lastShedSandboxes[victim.SandboxID] = targetHost
	h.recordEventLocked("HOT_MIGRATE", victim.SandboxID, targetHost, "",
		fmt.Sprintf("Proactively live-migrated %s (%dMiB) from overloaded host %s (mem=%.1f%%, cpu=%.1f%%) -> %s",
			victim.SandboxID, victim.UsedMemoryBytes/(1<<20), h.hostID, state.UsedMemoryPct, state.CPUUsagePct, targetHost))
	afterRec := h.snapshotHostRecordLocked()
	h.appendMetricsSampleLocked(afterRec)
	h.mu.Unlock()

	_ = h.engine.Stop(ctx, victim.SandboxID)
	sbxMu.Unlock()

	_ = h.SyncStateAndHeartbeat(ctx)
	return map[string]string{victim.SandboxID: targetHost}, nil
}

func (h *HostServer) handleSimulateCrash(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	h.status = HostCrashed
	// Snapshot the host record BEFORE clearing local sandboxes so hosts/<id>.json retains the
	// running sandbox list for the Control Plane Watchdog to resurrect!
	rec := h.snapshotHostRecordLocked()
	rec.Status = HostCrashed
	// Backdate LastHeartbeatAt so the Control Plane Watchdog immediately detects the crash
	rec.LastHeartbeatAt = time.Now().UTC().Add(-1 * time.Hour)
	var crashedIDs []string
	for sbxID := range h.sandboxes {
		crashedIDs = append(crashedIDs, sbxID)
	}
	// Clear local sandbox map so any subsequent SIGTERM on this crashed container cannot
	// re-export or evacuate sandboxes that the Watchdog has already resurrected elsewhere!
	h.sandboxes = make(map[string]SandboxRecord)
	h.dirtySandboxes = make(map[string]bool)
	h.mu.Unlock()

	for _, sbxID := range crashedIDs {
		_ = h.engine.Stop(r.Context(), sbxID)
	}
	SetInstanceBusyState(false)

	if h.store != nil {
		_, _ = h.store.WriteJSONCAS(r.Context(), "hosts/"+h.hostID+".json", rec, -1)
	}
	writeHostJSON(w, h, http.StatusOK, map[string]any{
		"host_id": h.hostID,
		"status":  HostCrashed,
		"message": "Host marked CRASHED without SIGTERM flush; watchdog will resurrect sandboxes",
	})
}

// DrainAndEvacuate transitions the host to DRAINING, requests target hosts from the control plane
// (if not pre-supplied in presetTargets), exports every running sandbox's overlay tar, and hydrates
// each sandbox onto its destination host.
func (h *HostServer) DrainAndEvacuate(ctx context.Context, reason string, presetTargets map[string]string) (map[string]string, error) {
	if h.IsCrashed() {
		return map[string]string{}, nil
	}
	h.mu.Lock()
	h.status = HostDraining
	var toEvacuate []SandboxRecord
	var ids []string
	for k, s := range h.sandboxes {
		if s.Status == StatusRunning {
			s.Status = StatusMigrating
			h.sandboxes[k] = s
			toEvacuate = append(toEvacuate, s)
			ids = append(ids, s.SandboxID)
		}
	}
	h.recordEventLocked("DRAIN", "", h.hostID, "", fmt.Sprintf("Draining %d sandboxes from %s (reason=%s)", len(toEvacuate), h.hostID, reason))
	h.mu.Unlock()
	_ = h.SyncStateAndHeartbeat(ctx)

	if len(toEvacuate) == 0 {
		h.retireEmptyHost(ctx, "drain_empty_"+reason)
		return map[string]string{}, nil
	}

	targets := make(map[string]string)
	for k, v := range presetTargets {
		targets[k] = v
	}
	var missing []string
	for _, id := range ids {
		if targets[id] == "" || targets[id] == h.hostID {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		brokered := h.requestMigrationTargets(ctx, missing, reason)
		for k, v := range brokered {
			targets[k] = v
		}
	}

	migrated := make(map[string]string)
	var firstErr error
	for _, rec := range toEvacuate {
		sbxMu := h.getSandboxLock(rec.SandboxID)
		sbxMu.Lock()

		targetHost := targets[rec.SandboxID]
		if targetHost == "" || targetHost == h.hostID {
			targetHost = NewRandomID("host")
		}
		if err := h.exportSandboxOverlayToStoreLocked(ctx, &rec, "drain_"+reason); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			h.mu.Lock()
			rec.Status = StatusRunning
			h.sandboxes[rec.SandboxID] = rec
			h.mu.Unlock()
			_ = h.engine.Start(ctx, rec, "")
			sbxMu.Unlock()
			continue
		}
		hydrateReason := reason
		if rec.PinnedHost {
			hydrateReason = "cli_migrate_pinned"
		}
		hydrated, err := h.sendHydrateToHost(ctx, targetHost, HydrateRequest{
			Record:     rec,
			OverlayTar: rec.OverlayPath,
			Reason:     hydrateReason,
		})
		if err != nil {
			// Fallback: mint a fresh host session and retry once
			fallbackHost := NewRandomID("host")
			if h2, retryErr := h.sendHydrateToHost(ctx, fallbackHost, HydrateRequest{
				Record:     rec,
				OverlayTar: rec.OverlayPath,
				Reason:     hydrateReason,
			}); retryErr == nil {
				targetHost = h2.HostID
				err = nil
			}
		} else if hydrated.HostID != "" {
			targetHost = hydrated.HostID
		}
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			h.mu.Lock()
			rec.Status = StatusRunning
			h.sandboxes[rec.SandboxID] = rec
			h.mu.Unlock()
			_ = h.engine.Start(ctx, rec, "")
			sbxMu.Unlock()
			continue
		}
		// Remove from local map and record redirect BEFORE stopping engine so concurrent requests get HTTP 409 immediately
		h.mu.Lock()
		h.migratedTo[rec.SandboxID] = targetHost
		delete(h.sandboxes, rec.SandboxID)
		h.mu.Unlock()
		_ = h.engine.Stop(ctx, rec.SandboxID)
		sbxMu.Unlock()
		migrated[rec.SandboxID] = targetHost
	}

	h.mu.Lock()
	remaining := len(h.sandboxes)
	if reason != "sigterm_shutdown" && remaining == 0 {
		h.status = HostActive
	}
	h.snapshotHostRecordLocked()
	h.mu.Unlock()
	if remaining == 0 {
		h.retireEmptyHost(ctx, "drained_"+reason)
	} else {
		_ = h.SyncStateAndHeartbeat(ctx)
	}
	return migrated, firstErr
}

func (h *HostServer) requestMigrationTargets(ctx context.Context, sandboxIDs []string, reason string) map[string]string {
	out := make(map[string]string)
	if h.controlPlaneURL != "" {
		reqBody, _ := json.Marshal(MigrationTargetsRequest{
			SourceHostID: h.hostID,
			SandboxIDs:   sandboxIDs,
			Reason:       reason,
		})
		reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, h.controlPlaneURL+"/v1/migration-targets", bytes.NewReader(reqBody))
		if err == nil {
			req.Header.Set("Content-Type", "application/json")
			if strings.HasPrefix(h.controlPlaneURL, "https://") {
				if tok, err := GetIDToken(reqCtx, h.controlPlaneURL); err == nil && tok != "" {
					req.Header.Set("Authorization", "Bearer "+tok)
				}
			}
			if resp, err := h.httpClient.Do(req); err == nil {
				var mtResp MigrationTargetsResponse
				if resp.StatusCode == http.StatusOK && json.NewDecoder(resp.Body).Decode(&mtResp) == nil {
					for k, v := range mtResp.Targets {
						out[k] = v
					}
					if h.poolServiceURL == "" && mtResp.PoolServiceURL != "" {
						h.poolServiceURL = strings.TrimRight(mtResp.PoolServiceURL, "/")
					}
				}
				resp.Body.Close()
			}
		}
	}
	// Autonomous fallback: mint a new host session ID if control plane is unreachable
	var fallbackHost string
	for _, id := range sandboxIDs {
		if out[id] == "" || out[id] == h.hostID {
			if fallbackHost == "" {
				fallbackHost = NewRandomID("host")
			}
			out[id] = fallbackHost
		}
	}
	return out
}

func (h *HostServer) sendHydrateToHost(ctx context.Context, targetHostID string, reqPayload HydrateRequest) (SandboxRecord, error) {
	poolURL := h.poolServiceURL
	if poolURL == "" && h.store != nil {
		var cfg FleetConfig
		if _, ok, _ := h.store.ReadJSON(ctx, "fleet/config.json", &cfg); ok && cfg.PoolServiceURL != "" {
			poolURL = strings.TrimRight(cfg.PoolServiceURL, "/")
		}
	}
	if poolURL == "" {
		return SandboxRecord{}, errors.New("pool_service_url is not configured for cross-host hydration")
	}
	b, err := json.Marshal(reqPayload)
	if err != nil {
		return SandboxRecord{}, err
	}
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
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return SandboxRecord{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return SandboxRecord{}, fmt.Errorf("hydrate on %s returned %d: %s", targetHostID, resp.StatusCode, string(errBody))
	}
	var hydrated SandboxRecord
	if err := json.NewDecoder(resp.Body).Decode(&hydrated); err != nil {
		return SandboxRecord{}, err
	}
	return hydrated, nil
}
