package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"
)

//go:embed go.mod main.go dispenser.go runtime.go hostd.go controld.go cli.go ui.go Dockerfile deploy.sh service-control-plane.yaml service-pool.yaml service-ui.yaml
var embeddedDeployAssets embed.FS

// resolveCLIProject returns the active GCP project ID from an explicit flag,
// environment variables (`GOOGLE_CLOUD_PROJECT`, `GCLOUD_PROJECT`, `DEVSHELL_PROJECT_ID`, `CLOUDSDK_CORE_PROJECT`),
// or `gcloud config get-value project`.
func resolveCLIProject(ctx context.Context, explicit string) string {
	v := strings.TrimSpace(explicit)
	if v != "" && v != "my-gcp-project" {
		return v
	}
	if def := DefaultGCPProject(); def != "" && def != "my-gcp-project" {
		return def
	}
	if out, err := exec.CommandContext(ctx, "gcloud", "config", "get-value", "project").Output(); err == nil {
		p := strings.TrimSpace(string(out))
		if p != "" && p != "(unset)" {
			return p
		}
	}
	return ""
}

// extractEmbeddedDeployAssets materializes the embedded source files, Dockerfile,
// service YAML manifests, and deploy.sh into a temporary staging directory so that
// `sbx deploy` works from any directory when installed via `go install`.
func extractEmbeddedDeployAssets() (string, error) {
	entries, err := embeddedDeployAssets.ReadDir(".")
	if err != nil {
		return "", err
	}
	tmpDir, err := os.MkdirTemp("", "sbx-deploy-*")
	if err != nil {
		return "", err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		data, err := embeddedDeployAssets.ReadFile(entry.Name())
		if err != nil {
			_ = os.RemoveAll(tmpDir)
			return "", err
		}
		mode := os.FileMode(0644)
		if entry.Name() == "deploy.sh" {
			mode = 0755
		}
		if err := os.WriteFile(filepath.Join(tmpDir, entry.Name()), data, mode); err != nil {
			_ = os.RemoveAll(tmpDir)
			return "", err
		}
	}
	return tmpDir, nil
}

// CLIClient implements the decentralized `sbx` (`gcloud run sandbox`) client.
// It caches:
//  1. `candidates.json` (open host sessions from `/v1/candidates`, refreshed adaptively)
//  2. `routes.json` (`<sandbox_id> -> <host_session_id>`, self-healed on `HTTP 409`)
type CLIClient struct {
	ControlPlaneURL string
	PoolServiceURL  string
	CacheDir        string
	ClientVersion   string
	ClientType      string
	HTTPClient      *http.Client
	mu              sync.Mutex
}

func (c *CLIClient) clientVersion() string {
	if strings.TrimSpace(c.ClientVersion) != "" {
		return strings.TrimSpace(c.ClientVersion)
	}
	return DispenserVersion
}

func (c *CLIClient) clientType() string {
	if strings.TrimSpace(c.ClientType) != "" {
		return strings.TrimSpace(c.ClientType)
	}
	return "sbx-cli/go"
}

type cachedCandidatesFile struct {
	Response      CandidatesResponse `json:"response"`
	FetchedAt     time.Time          `json:"fetched_at"`
	CreateCounter int                `json:"create_counter"`
}

type cachedRoutesFile struct {
	Routes    map[string]string `json:"routes"`          // sandbox_id -> host_session_id
	Names     map[string]string `json:"names,omitempty"` // name -> sandbox_id
	UpdatedAt time.Time         `json:"updated_at"`
}

// NewCLIClient constructs a CLIClient with configurable URLs and cache directory.
func NewCLIClient(controlPlaneURL, poolServiceURL, cacheDir string) *CLIClient {
	if controlPlaneURL == "" {
		controlPlaneURL = os.Getenv("SBX_CONTROL_PLANE_URL")
	}
	if poolServiceURL == "" {
		poolServiceURL = os.Getenv("SBX_POOL_URL")
	}
	if cacheDir == "" {
		cacheDir = os.Getenv("SBX_CACHE_DIR")
	}
	if cacheDir == "" {
		cacheDir = os.Getenv("SBX_CONFIG_DIR")
	}
	if cacheDir == "" {
		home, _ := os.UserHomeDir()
		if home == "" {
			home = os.TempDir()
		}
		cacheDir = filepath.Join(home, ".cache", "sbx")
	}
	_ = os.MkdirAll(cacheDir, 0755)
	SetCLICacheDir(cacheDir)

	c := &CLIClient{
		ControlPlaneURL: strings.TrimRight(controlPlaneURL, "/"),
		PoolServiceURL:  strings.TrimRight(poolServiceURL, "/"),
		CacheDir:        cacheDir,
		ClientVersion:   DispenserVersion,
		ClientType:      "sbx-cli/go",
		HTTPClient:      &http.Client{Timeout: 60 * time.Second},
	}
	c.loadPersistedURLs()
	return c
}

func (c *CLIClient) loadPersistedURLs() {
	cfgFile := filepath.Join(c.CacheDir, "endpoints.json")
	b, err := os.ReadFile(cfgFile)
	if err != nil {
		return
	}
	var ep struct {
		ControlPlaneURL string `json:"control_plane_url"`
		PoolServiceURL  string `json:"pool_service_url"`
	}
	if json.Unmarshal(b, &ep) == nil {
		if c.ControlPlaneURL == "" && ep.ControlPlaneURL != "" {
			c.ControlPlaneURL = strings.TrimRight(ep.ControlPlaneURL, "/")
		}
		if c.PoolServiceURL == "" && ep.PoolServiceURL != "" {
			c.PoolServiceURL = strings.TrimRight(ep.PoolServiceURL, "/")
		}
	}
}

// SaveEndpoints persists the discovered or configured ControlPlaneURL and PoolServiceURL in CacheDir.
func (c *CLIClient) SaveEndpoints(controlPlaneURL, poolServiceURL string) {
	if controlPlaneURL != "" {
		c.ControlPlaneURL = strings.TrimRight(controlPlaneURL, "/")
	}
	if poolServiceURL != "" {
		c.PoolServiceURL = strings.TrimRight(poolServiceURL, "/")
	}
	b, _ := json.MarshalIndent(map[string]string{
		"control_plane_url": c.ControlPlaneURL,
		"pool_service_url":  c.PoolServiceURL,
	}, "", "  ")
	_ = os.WriteFile(filepath.Join(c.CacheDir, "endpoints.json"), b, 0644)
}

func (c *CLIClient) candidatesFilePath() string {
	return filepath.Join(c.CacheDir, "candidates.json")
}

func (c *CLIClient) routesFilePath() string {
	return filepath.Join(c.CacheDir, "routes.json")
}

func (c *CLIClient) readRoutesFileLocked() cachedRoutesFile {
	b, err := os.ReadFile(c.routesFilePath())
	var rf cachedRoutesFile
	if err == nil {
		_ = json.Unmarshal(b, &rf)
	}
	if rf.Routes == nil {
		rf.Routes = make(map[string]string)
	}
	if rf.Names == nil {
		rf.Names = make(map[string]string)
	}
	return rf
}

func (c *CLIClient) writeRoutesFileLocked(rf cachedRoutesFile) {
	rf.UpdatedAt = time.Now().UTC()
	b, err := json.MarshalIndent(rf, "", "  ")
	if err != nil {
		return
	}
	tmp := c.routesFilePath() + ".tmp." + NewRandomID("")
	if os.WriteFile(tmp, b, 0644) == nil {
		_ = os.Rename(tmp, c.routesFilePath())
	}
}

// ResolveSandboxID maps a user-provided identifier (either a distributed `sbx-<host>-<hex>` ID or a `name`)
// to its canonical `sandbox_id` using the local `routes.json` name index if available.
func (c *CLIClient) ResolveSandboxID(idOrName string) string {
	idOrName = strings.TrimSpace(idOrName)
	if idOrName == "" || ExtractInitialHostFromSandboxID(idOrName) != "" {
		return idOrName
	}
	c.mu.Lock()
	rf := c.readRoutesFileLocked()
	c.mu.Unlock()
	if _, hasExactRoute := rf.Routes[idOrName]; hasExactRoute {
		return idOrName
	}
	if mappedID := rf.Names[idOrName]; mappedID != "" {
		return mappedID
	}
	return idOrName
}

// ResolveUniqueSandboxID resolves a user-provided `sandbox_id` or human-readable `name`.
// If `idOrName` is a distributed `sbx-<host>-<8hex>` ID, it returns immediately with zero network lookup.
// If `idOrName` is a human-readable `name` and `ControlPlaneURL` is configured, it calls
// `GET /v1/sandboxes/{idOrName}` so the server resolves the name and enforces uniqueness (`HTTP 409 Conflict` if ambiguous).
func (c *CLIClient) ResolveUniqueSandboxID(ctx context.Context, idOrName string) (string, error) {
	idOrName = strings.TrimSpace(idOrName)
	if idOrName == "" || ExtractInitialHostFromSandboxID(idOrName) != "" {
		return idOrName, nil
	}
	if c.ControlPlaneURL != "" {
		var rec SandboxRecord
		u := c.ControlPlaneURL + "/v1/sandboxes/" + url.PathEscape(idOrName)
		status, err := c.doJSON(ctx, http.MethodGet, u, nil, nil, &rec)
		if err == nil && status == http.StatusOK && rec.SandboxID != "" {
			c.SetRouteAndName(rec.SandboxID, rec.Name, rec.HostID)
			return rec.SandboxID, nil
		}
		if status == http.StatusConflict || status == http.StatusUpgradeRequired {
			return "", err
		}
	}
	return c.ResolveSandboxID(idOrName), nil
}

// GetRoute returns the cached host_session_id for sandboxID, or extracts the embedded initial host
// from a distributed `sbx-<host>-<hex>` ID with zero control-plane lookup, or queries the control plane on a cache miss.
func (c *CLIClient) GetRoute(ctx context.Context, sandboxID string) string {
	resolvedID := c.ResolveSandboxID(sandboxID)
	c.mu.Lock()
	rf := c.readRoutesFileLocked()
	hostID := rf.Routes[resolvedID]
	if hostID == "" {
		hostID = rf.Routes[sandboxID]
	}
	c.mu.Unlock()
	if hostID != "" {
		return hostID
	}
	if initialHost := ExtractInitialHostFromSandboxID(resolvedID); initialHost != "" {
		return initialHost
	}
	if c.ControlPlaneURL != "" {
		var rec SandboxRecord
		if status, err := c.doJSON(ctx, http.MethodGet, c.ControlPlaneURL+"/v1/sandboxes/"+url.PathEscape(resolvedID), nil, nil, &rec); err == nil && status == http.StatusOK && rec.HostID != "" {
			c.SetRouteAndName(rec.SandboxID, rec.Name, rec.HostID)
			return rec.HostID
		}
	}
	return ""
}

// SetRoute updates `<sandbox_id> -> <host_id>` in `routes.json`.
func (c *CLIClient) SetRoute(sandboxID, hostID string) {
	c.SetRouteAndName(sandboxID, "", hostID)
}

// SetRouteAndName updates `<sandbox_id> -> <host_id>` and `<name> -> <sandbox_id>` in `routes.json`.
func (c *CLIClient) SetRouteAndName(sandboxID, name, hostID string) {
	if sandboxID == "" || hostID == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	rf := c.readRoutesFileLocked()
	rf.Routes[sandboxID] = hostID
	if name != "" && name != sandboxID {
		rf.Names[name] = sandboxID
		rf.Routes[name] = hostID
	}
	c.writeRoutesFileLocked(rf)
}

func (c *CLIClient) loadCandidatesLocked() (cachedCandidatesFile, bool) {
	b, err := os.ReadFile(c.candidatesFilePath())
	if err != nil {
		return cachedCandidatesFile{}, false
	}
	var cf cachedCandidatesFile
	if json.Unmarshal(b, &cf) != nil {
		return cachedCandidatesFile{}, false
	}
	if c.PoolServiceURL == "" && cf.Response.PoolServiceURL != "" {
		c.PoolServiceURL = strings.TrimRight(cf.Response.PoolServiceURL, "/")
	}
	return cf, true
}

func (c *CLIClient) saveCandidatesLocked(cf cachedCandidatesFile) {
	b, err := json.MarshalIndent(cf, "", "  ")
	if err != nil {
		return
	}
	tmp := c.candidatesFilePath() + ".tmp." + NewRandomID("")
	if os.WriteFile(tmp, b, 0644) == nil {
		_ = os.Rename(tmp, c.candidatesFilePath())
	}
}

// RefreshCandidates fetches `/v1/candidates` from `sandbox-control-plane` and updates `candidates.json`.
func (c *CLIClient) RefreshCandidates(ctx context.Context) (CandidatesResponse, error) {
	if c.ControlPlaneURL == "" {
		return CandidatesResponse{}, errors.New("control_plane_url not configured")
	}
	var resp CandidatesResponse
	status, err := c.doJSON(ctx, http.MethodGet, c.ControlPlaneURL+"/v1/candidates", nil, nil, &resp)
	if err != nil {
		return CandidatesResponse{}, err
	}
	if status != http.StatusOK {
		return CandidatesResponse{}, fmt.Errorf("GET /v1/candidates returned %d", status)
	}
	c.mu.Lock()
	if c.PoolServiceURL == "" && resp.PoolServiceURL != "" {
		c.PoolServiceURL = strings.TrimRight(resp.PoolServiceURL, "/")
	}
	c.saveCandidatesLocked(cachedCandidatesFile{
		Response:      resp,
		FetchedAt:     time.Now().UTC(),
		CreateCounter: 0,
	})
	c.mu.Unlock()
	return resp, nil
}

// GetAdaptiveCandidates returns the cached candidate list if fresh and non-empty,
// or refreshes from `/v1/candidates` when stale or low on free slots.
func (c *CLIClient) GetAdaptiveCandidates(ctx context.Context, forceRefresh bool) []CandidateHost {
	c.mu.Lock()
	cf, ok := c.loadCandidatesLocked()
	c.mu.Unlock()

	totalFree := 0
	for _, ch := range cf.Response.Candidates {
		totalFree += ch.FreeSlots
	}
	ttl := time.Duration(cf.Response.RefreshIntervalSec) * time.Second
	if ttl <= 0 {
		ttl = 15 * time.Second
	}
	// Adaptive refresh: if forceRefresh, cache expired, or local free slots exhausted
	if forceRefresh || !ok || time.Since(cf.FetchedAt) > ttl || totalFree <= 0 {
		if refreshed, err := c.RefreshCandidates(ctx); err == nil {
			return refreshed.Candidates
		}
	}
	return cf.Response.Candidates
}

func (c *CLIClient) decrementOrRemoveCandidate(hostID string, full bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cf, ok := c.loadCandidatesLocked()
	if !ok {
		return
	}
	var updated []CandidateHost
	for _, ch := range cf.Response.Candidates {
		if ch.HostID == hostID {
			if full {
				continue
			}
			ch.FreeSlots--
			ch.ActiveSandboxes++
			if ch.FreeSlots <= 0 {
				continue
			}
		}
		updated = append(updated, ch)
	}
	cf.Response.Candidates = updated
	cf.CreateCounter++
	c.saveCandidatesLocked(cf)
}

func (c *CLIClient) addWarmCandidate(hostID string, freeSlots int, maxDensity int) {
	if hostID == "" || freeSlots <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cf, _ := c.loadCandidatesLocked()
	found := false
	for i := range cf.Response.Candidates {
		if cf.Response.Candidates[i].HostID == hostID {
			cf.Response.Candidates[i].FreeSlots = freeSlots
			found = true
			break
		}
	}
	if !found {
		if maxDensity <= 0 {
			maxDensity = 10
		}
		cf.Response.Candidates = append([]CandidateHost{{
			HostID:          hostID,
			ActiveSandboxes: max(maxDensity-freeSlots, 1),
			FreeSlots:       freeSlots,
			MaxDensity:      maxDensity,
		}}, cf.Response.Candidates...)
	}
	if cf.FetchedAt.IsZero() {
		cf.FetchedAt = time.Now().UTC()
	}
	c.saveCandidatesLocked(cf)
}

// CreateSandbox executes the decentralized creation protocol:
//  1. Tries cached candidate hosts in Best-Fit order (`X-Serverless-Session-Id: <candidate>`).
//  2. On `HTTP 429 Host Full` or `503 Host Draining`, immediately tries the next candidate.
//  3. If all candidates are full (or none exist yet), refreshes once from `/v1/candidates`; if still none,
//     autonomously mints `host-<random>` and calls `sandbox-pool` directly!
func (c *CLIClient) CreateSandbox(ctx context.Context, req CreateSandboxRequest) (SandboxRecord, error) {
	sbxName := strings.TrimSpace(req.Name)
	if sbxName == "" {
		sbxName = strings.TrimSpace(req.SandboxID)
	}
	if sbxName != "" {
		if err := ValidateID(sbxName); err != nil {
			return SandboxRecord{}, err
		}
		req.Name = sbxName
	}
	var candidates []CandidateHost
	if req.TargetHostID == "" {
		candidates = c.GetAdaptiveCandidates(ctx, false)
	}
	if c.PoolServiceURL == "" && c.ControlPlaneURL != "" {
		_, _ = c.RefreshCandidates(ctx)
	}
	if c.PoolServiceURL == "" {
		if c.ControlPlaneURL == "" {
			return SandboxRecord{}, errors.New("CLI not initialized: no Control Plane URL configured. Run 'sbx init <CONTROL_PLANE_URL>' or 'sbx init --project=<PROJECT_ID> --region=<REGION>' first")
		}
		return SandboxRecord{}, errors.New("pool_service_url is not configured (set --pool-url or SBX_POOL_URL, or run 'sbx init <CONTROL_PLANE_URL>')")
	}

	tried := make(map[string]bool)
	tryHost := func(hostID string) (SandboxRecord, int, error) {
		tried[hostID] = true
		var rec SandboxRecord
		headers := map[string]string{
			"X-Serverless-Session-Id": hostID,
		}
		if req.Name != "" {
			headers["X-Sandbox-Id"] = req.Name
		}
		status, respHdr, err := c.doJSONWithHeaders(ctx, http.MethodPost, c.PoolServiceURL+"/__session/create", headers, req, &rec)
		if err != nil {
			return SandboxRecord{}, status, err
		}
		if status == http.StatusOK {
			actualHost := rec.HostID
			if actualHost == "" {
				actualHost = hostID
			}
			c.SetRouteAndName(rec.SandboxID, rec.Name, actualHost)
			if freeStr := respHdr.Get("X-Host-Free-Slots"); freeStr != "" {
				if free, pErr := strconv.Atoi(freeStr); pErr == nil {
					if free <= 0 {
						c.decrementOrRemoveCandidate(actualHost, true)
					} else {
						c.addWarmCandidate(actualHost, free, 10)
					}
				}
			} else {
				c.decrementOrRemoveCandidate(actualHost, false)
			}
			return rec, status, nil
		}
		if status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable {
			c.decrementOrRemoveCandidate(hostID, true)
			if respHdr.Get("X-Host-Memory-Pressure") == "true" {
				return SandboxRecord{}, status, fmt.Errorf("create on host %s returned HTTP %d (host_memory_pressure)", hostID, status)
			}
		}
		return SandboxRecord{}, status, fmt.Errorf("create on host %s returned HTTP %d", hostID, status)
	}

	// Explicit target host pinning (`--host=<host_id>`)
	if req.TargetHostID != "" {
		if err := ValidateID(req.TargetHostID); err != nil {
			return SandboxRecord{}, err
		}
		req.PinnedHost = true
		rec, _, err := tryHost(req.TargetHostID)
		return rec, err
	}

	// Pass 1: Try cached candidates
	for _, cand := range candidates {
		if tried[cand.HostID] {
			continue
		}
		rec, status, err := tryHost(cand.HostID)
		if err == nil && status == http.StatusOK {
			return rec, nil
		}
		if status == http.StatusBadRequest || status == http.StatusNotFound || status == http.StatusConflict || status == http.StatusUpgradeRequired {
			return SandboxRecord{}, err
		}
		if status != http.StatusTooManyRequests && status != http.StatusServiceUnavailable {
			// Still fall through to next candidate on transient error
			continue
		}
	}

	// Pass 2: Force refresh candidates from control plane in case new warm hosts appeared
	freshCandidates := c.GetAdaptiveCandidates(ctx, true)
	for _, cand := range freshCandidates {
		if tried[cand.HostID] {
			continue
		}
		rec, status, err := tryHost(cand.HostID)
		if err == nil && status == http.StatusOK {
			return rec, nil
		}
		if status == http.StatusBadRequest || status == http.StatusNotFound || status == http.StatusConflict || status == http.StatusUpgradeRequired {
			return SandboxRecord{}, err
		}
	}

	// Pass 3: Autonomous CLI session spawning — mint a brand new `host-<random>` session on `sandbox-pool`!
	var lastErr error
	var lastHost string
	for attempt := 0; attempt < 5; attempt++ {
		newHostID := NewRandomID("host")
		lastHost = newHostID
		rec, status, err := tryHost(newHostID)
		if err == nil && status == http.StatusOK {
			return rec, nil
		}
		lastErr = err
		if status == http.StatusBadRequest || status == http.StatusNotFound || status == http.StatusConflict || status == http.StatusUpgradeRequired {
			return SandboxRecord{}, err
		}
	}
	return SandboxRecord{}, fmt.Errorf("autonomous host spawn on %s failed: %w", lastHost, lastErr)
}

// CallSandboxEndpoint sends a request to `/__session/<endpoint>` for `sandboxID`, using the local route cache
// and transparently self-healing & retrying if the host returns `HTTP 409 Conflict` (`X-Sandbox-Host-Session`)
// or if the cached host is unreachable/dead!
func (c *CLIClient) CallSandboxEndpoint(ctx context.Context, method, endpointPath, sandboxID string, reqBody any, respDest any) error {
	if c.PoolServiceURL == "" && c.ControlPlaneURL != "" {
		_, _ = c.RefreshCandidates(ctx)
	}
	if c.PoolServiceURL == "" {
		if c.ControlPlaneURL == "" {
			return errors.New("CLI not initialized: no Control Plane URL configured. Run 'sbx init <CONTROL_PLANE_URL>' or 'sbx init --project=<PROJECT_ID> --region=<REGION>' first")
		}
		return errors.New("pool_service_url is not configured (set --pool-url or SBX_POOL_URL, or run 'sbx init <CONTROL_PLANE_URL>')")
	}

	resolvedID := c.ResolveSandboxID(sandboxID)
	hostID := c.GetRoute(ctx, resolvedID)
	if hostID == "" {
		// Pick a candidate host or mint one so HostServer can resolve/auto-hydrate from Store
		cands := c.GetAdaptiveCandidates(ctx, false)
		if len(cands) > 0 {
			hostID = cands[0].HostID
		} else {
			hostID = NewRandomID("host")
		}
	}

	tried := make(map[string]bool)
	for attempt := 0; attempt < 4; attempt++ {
		tried[hostID] = true
		headers := map[string]string{
			"X-Serverless-Session-Id": hostID,
			"X-Sandbox-Id":            resolvedID,
		}
		status, respHdr, err := c.doJSONWithHeaders(ctx, method, c.PoolServiceURL+endpointPath, headers, reqBody, respDest)
		if err == nil && status == http.StatusOK {
			if actualHost := respHdr.Get("X-Host-Session-Id"); actualHost != "" {
				c.SetRouteAndName(resolvedID, sandboxID, actualHost)
			} else {
				c.SetRouteAndName(resolvedID, sandboxID, hostID)
			}
			return nil
		}
		// Terminal application errors (400 Bad Request, 404 Not Found, 413 Payload Too Large, 426 Upgrade Required) must not retry across hosts
		if status == http.StatusBadRequest || status == http.StatusNotFound || status == http.StatusRequestEntityTooLarge || status == http.StatusUpgradeRequired {
			if err != nil {
				return err
			}
			return fmt.Errorf("%s %s for %s failed with HTTP %d", method, endpointPath, sandboxID, status)
		}
		// Self-heal on HTTP 409 Misdirected Sandbox
		if status == http.StatusConflict {
			redirectHost := strings.TrimSpace(respHdr.Get("X-Sandbox-Host-Session"))
			if redirectHost == "" {
				if err != nil {
					return err
				}
				return fmt.Errorf("%s %s for %s failed with HTTP 409 Conflict", method, endpointPath, sandboxID)
			}
			if redirectHost != hostID && !tried[redirectHost] {
				hostID = redirectHost
				c.SetRouteAndName(resolvedID, sandboxID, hostID)
				continue
			}
		}
		// If host is full (during auto-resume), crashed/unavailable (503/network error), or redirect loop, pick another untried candidate
		if status == http.StatusServiceUnavailable || status == http.StatusTooManyRequests || status == http.StatusConflict || status == 0 {
			if c.ControlPlaneURL != "" {
				var rec SandboxRecord
				if st, cpErr := c.doJSON(ctx, http.MethodGet, c.ControlPlaneURL+"/v1/sandboxes/"+url.PathEscape(resolvedID), nil, nil, &rec); cpErr == nil && st == http.StatusOK && rec.HostID != "" && !tried[rec.HostID] {
					hostID = rec.HostID
					resolvedID = rec.SandboxID
					c.SetRouteAndName(rec.SandboxID, rec.Name, hostID)
					continue
				}
			}
			cands := c.GetAdaptiveCandidates(ctx, true)
			pickedNew := false
			for _, ch := range cands {
				if !tried[ch.HostID] {
					hostID = ch.HostID
					pickedNew = true
					break
				}
			}
			if !pickedNew {
				hostID = NewRandomID("host")
			}
			continue
		}
		if err != nil {
			return err
		}
		return fmt.Errorf("%s %s for %s failed with HTTP %d", method, endpointPath, sandboxID, status)
	}
	return fmt.Errorf("exceeded retry attempts routing sandbox %s to %s", sandboxID, endpointPath)
}

// Exec runs a command in `sandboxID` with automatic route self-healing.
func (c *CLIClient) Exec(ctx context.Context, req ExecRequest) (ExecResponse, error) {
	var resp ExecResponse
	err := c.CallSandboxEndpoint(ctx, http.MethodPost, "/__session/exec", req.SandboxID, req, &resp)
	return resp, err
}

// ExecStream runs a command in `sandboxID` via SSE (`/__session/exec-stream`), invoking `onEvent`
// for each live `ExecStreamEvent` (`start`, `stdout`, `stderr`, `exit`, `error`) as chunks arrive,
// while transparently handling `HTTP 409` `X-Sandbox-Host-Session` route self-healing.
func (c *CLIClient) ExecStream(ctx context.Context, req ExecRequest, onEvent func(ExecStreamEvent)) (ExecResponse, error) {
	if c.PoolServiceURL == "" && c.ControlPlaneURL != "" {
		_, _ = c.RefreshCandidates(ctx)
	}
	if c.PoolServiceURL == "" {
		return ExecResponse{}, errors.New("pool_service_url is not configured")
	}

	resolvedID := c.ResolveSandboxID(req.SandboxID)
	req.SandboxID = resolvedID
	req.Stream = true
	hostID := c.GetRoute(ctx, resolvedID)
	if hostID == "" {
		cands := c.GetAdaptiveCandidates(ctx, false)
		if len(cands) > 0 {
			hostID = cands[0].HostID
		} else {
			hostID = NewRandomID("host")
		}
	}

	bodyBytes, err := json.Marshal(req)
	if err != nil {
		return ExecResponse{}, err
	}

	tried := make(map[string]bool)
	for attempt := 0; attempt < 4; attempt++ {
		tried[hostID] = true
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.PoolServiceURL+"/__session/exec-stream", bytes.NewReader(bodyBytes))
		if err != nil {
			return ExecResponse{}, err
		}
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Accept", "text/event-stream")
		httpReq.Header.Set("X-Serverless-Session-Id", hostID)
		httpReq.Header.Set("X-Sandbox-Id", resolvedID)
		httpReq.Header.Set("X-Sbx-Client-Version", c.clientVersion())
		httpReq.Header.Set("X-Sbx-Client-Type", c.clientType())
		if strings.HasPrefix(c.PoolServiceURL, "https://") {
			if tok, tokErr := GetIDToken(ctx, c.PoolServiceURL); tokErr == nil && tok != "" {
				httpReq.Header.Set("Authorization", "Bearer "+tok)
			}
		}

		resp, err := c.HTTPClient.Do(httpReq)
		if err != nil {
			cands := c.GetAdaptiveCandidates(ctx, true)
			for _, ch := range cands {
				if !tried[ch.HostID] {
					hostID = ch.HostID
					break
				}
			}
			continue
		}

		if resp.StatusCode == http.StatusConflict {
			redirectHost := strings.TrimSpace(resp.Header.Get("X-Sandbox-Host-Session"))
			_ = resp.Body.Close()
			if redirectHost != "" && redirectHost != hostID && !tried[redirectHost] {
				hostID = redirectHost
				c.SetRouteAndName(resolvedID, req.SandboxID, hostID)
				continue
			}
			return ExecResponse{}, fmt.Errorf("exec-stream for %s failed with HTTP 409 Conflict", req.SandboxID)
		}

		if resp.StatusCode != http.StatusOK {
			rawErr, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusTooManyRequests {
				cands := c.GetAdaptiveCandidates(ctx, true)
				for _, ch := range cands {
					if !tried[ch.HostID] {
						hostID = ch.HostID
						break
					}
				}
				continue
			}
			return ExecResponse{}, fmt.Errorf("HTTP %d from exec-stream: %s", resp.StatusCode, strings.TrimSpace(string(rawErr)))
		}

		if actualHost := resp.Header.Get("X-Sandbox-Host-Session"); actualHost != "" {
			hostID = actualHost
		}
		c.SetRouteAndName(resolvedID, req.SandboxID, hostID)

		var outBuf, errBuf strings.Builder
		finalResp := ExecResponse{
			SandboxID: resolvedID,
			HostID:    hostID,
		}
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			if !strings.HasPrefix(line, "data:") {
				continue
			}
			payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if payload == "" {
				continue
			}
			var evt ExecStreamEvent
			if jsonErr := json.Unmarshal([]byte(payload), &evt); jsonErr != nil {
				continue
			}
			if onEvent != nil {
				onEvent(evt)
			}
			switch evt.Type {
			case "start":
				if evt.HostID != "" {
					finalResp.HostID = evt.HostID
				}
				if evt.Epoch > 0 {
					finalResp.Epoch = evt.Epoch
				}
			case "stdout":
				outBuf.WriteString(evt.Data)
			case "stderr":
				errBuf.WriteString(evt.Data)
			case "exit":
				finalResp.ExitCode = evt.ExitCode
				finalResp.DurationMs = evt.DurationMs
				if evt.Epoch > 0 {
					finalResp.Epoch = evt.Epoch
				}
			case "error":
				finalResp.ExitCode = evt.ExitCode
			}
		}
		_ = resp.Body.Close()
		finalResp.Stdout = outBuf.String()
		finalResp.Stderr = errBuf.String()
		return finalResp, nil
	}
	return ExecResponse{}, fmt.Errorf("exceeded retry attempts routing exec-stream for %s", req.SandboxID)
}

func (c *CLIClient) doJSON(ctx context.Context, method, rawURL string, headers map[string]string, reqBody any, respDest any) (int, error) {
	status, _, err := c.doJSONWithHeaders(ctx, method, rawURL, headers, reqBody, respDest)
	return status, err
}

func (c *CLIClient) doJSONWithHeaders(ctx context.Context, method, rawURL string, headers map[string]string, reqBody any, respDest any) (int, http.Header, error) {
	if rawURL == "" || strings.HasPrefix(rawURL, "/") {
		return 0, nil, errors.New("CLI not initialized: no Control Plane URL configured. Run 'sbx init <CONTROL_PLANE_URL>' or 'sbx init --project=<PROJECT_ID> --region=<REGION>' first")
	}
	var bodyReader io.Reader
	if reqBody != nil {
		b, err := json.Marshal(reqBody)
		if err != nil {
			return 0, nil, err
		}
		bodyReader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, rawURL, bodyReader)
	if err != nil {
		return 0, nil, err
	}
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("X-Sbx-Client-Version", c.clientVersion())
	req.Header.Set("X-Sbx-Client-Type", c.clientType())
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	if strings.HasPrefix(rawURL, "https://") {
		if tok, err := GetIDToken(ctx, rawURL); err == nil && tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
	}
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	rawResp, _ := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if resp.StatusCode == http.StatusOK && respDest != nil && len(rawResp) > 0 {
		if err := json.Unmarshal(rawResp, respDest); err != nil {
			return resp.StatusCode, resp.Header, fmt.Errorf("decode response from %s: %w (body=%s)", rawURL, err, string(rawResp))
		}
	}
	isMisdirected409 := resp.StatusCode == http.StatusConflict && strings.TrimSpace(resp.Header.Get("X-Sandbox-Host-Session")) != ""
	if resp.StatusCode >= 400 && !isMisdirected409 && resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode != http.StatusServiceUnavailable {
		return resp.StatusCode, resp.Header, fmt.Errorf("HTTP %d from %s: %s", resp.StatusCode, rawURL, strings.TrimSpace(string(rawResp)))
	}
	return resp.StatusCode, resp.Header, nil
}

// RunCLI parses command-line flags and executes the requested `sbx` (`gcloud run sandbox`) subcommand.
func RunCLI(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	// Support `gcloud run sandbox <subcommand>` prefix invocations
	for len(args) > 0 && (args[0] == "gcloud" || args[0] == "alpha" || args[0] == "beta" || args[0] == "run" || args[0] == "sandbox" || args[0] == "sbx") {
		args = args[1:]
	}

	cpURL := os.Getenv("SBX_CONTROL_PLANE_URL")
	poolURL := os.Getenv("SBX_POOL_URL")
	cacheDir := os.Getenv("SBX_CACHE_DIR")
	if cacheDir == "" {
		cacheDir = os.Getenv("SBX_CONFIG_DIR")
	}
	jsonOut := false

	// Extract global flags that appear before `--`
	var subArgs []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			subArgs = append(subArgs, args[i:]...)
			break
		}
		if a == "--json" {
			jsonOut = true
		} else if strings.HasPrefix(a, "--control-plane-url=") {
			cpURL = strings.TrimPrefix(a, "--control-plane-url=")
		} else if a == "--control-plane-url" && i+1 < len(args) {
			i++
			cpURL = args[i]
		} else if strings.HasPrefix(a, "--pool-url=") {
			poolURL = strings.TrimPrefix(a, "--pool-url=")
		} else if a == "--pool-url" && i+1 < len(args) {
			i++
			poolURL = args[i]
		} else if strings.HasPrefix(a, "--cache-dir=") {
			cacheDir = strings.TrimPrefix(a, "--cache-dir=")
		} else if a == "--cache-dir" && i+1 < len(args) {
			i++
			cacheDir = args[i]
		} else if strings.HasPrefix(a, "--config-dir=") {
			cacheDir = strings.TrimPrefix(a, "--config-dir=")
		} else if a == "--config-dir" && i+1 < len(args) {
			i++
			cacheDir = args[i]
		} else {
			subArgs = append(subArgs, a)
		}
	}

	if len(subArgs) == 0 || subArgs[0] == "help" || subArgs[0] == "--help" || subArgs[0] == "-h" {
		printCLIUsage(stdout)
		return 0
	}

	client := NewCLIClient(cpURL, poolURL, cacheDir)
	sub := subArgs[0]
	rest := subArgs[1:]

	switch sub {
	case "init":
		return cliInit(ctx, client, rest, jsonOut, stdout, stderr)
	case "version", "--version", "-v":
		return cliVersion(ctx, client, jsonOut, stdout)
	case "create":
		return cliCreate(ctx, client, rest, jsonOut, stdout, stderr)
	case "list", "ls":
		return cliList(ctx, client, rest, jsonOut, stdout, stderr)
	case "inspect", "describe":
		return cliInspect(ctx, client, rest, jsonOut, stdout, stderr)
	case "update":
		return cliUpdate(ctx, client, rest, jsonOut, stdout, stderr)
	case "delete", "rm":
		return cliDelete(ctx, client, rest, jsonOut, stdout, stderr)
	case "exec":
		return cliExec(ctx, client, rest, jsonOut, stdout, stderr)
	case "preview-url", "preview":
		return cliPreviewURL(ctx, client, rest, jsonOut, stdout, stderr)
	case "port-forward", "forward":
		return cliPortForward(ctx, client, rest, stdout, stderr)
	case "ssh":
		return cliSSH(ctx, client, rest, stdout, stderr)
	case "cp":
		return cliCP(ctx, client, rest, jsonOut, stdout, stderr)
	case "logs":
		return cliLogs(ctx, client, rest, jsonOut, stdout, stderr)
	case "stop":
		return cliStopOrSuspend(ctx, client, rest, "stop", jsonOut, stdout, stderr)
	case "suspend":
		return cliStopOrSuspend(ctx, client, rest, "suspend", jsonOut, stdout, stderr)
	case "start":
		return cliStartOrResume(ctx, client, rest, "start", jsonOut, stdout, stderr)
	case "resume":
		return cliStartOrResume(ctx, client, rest, "resume", jsonOut, stdout, stderr)
	case "copy", "fork":
		return cliCopy(ctx, client, rest, jsonOut, stdout, stderr)
	case "migrate":
		return cliMigrate(ctx, client, rest, jsonOut, stdout, stderr)
	case "snapshot":
		return cliSnapshot(ctx, client, rest, jsonOut, stdout, stderr)
	case "template", "templates":
		return cliTemplate(ctx, client, rest, jsonOut, stdout, stderr)
	case "config":
		return cliConfig(ctx, client, rest, jsonOut, stdout, stderr)
	case "hosts":
		return cliHosts(ctx, client, rest, jsonOut, stdout, stderr)
	case "stats":
		return cliStats(ctx, client, jsonOut, stdout, stderr)
	case "metrics":
		return cliMetrics(ctx, client, rest, jsonOut, stdout, stderr)
	case "deploy":
		return cliDeploy(ctx, client, rest, stdout, stderr)
	case "ui":
		if hasHelpFlag(rest) {
			fmt.Fprintln(stdout, "Usage: sbx ui [--control-plane-url=URL] [--pool-url=URL] (runs Web Console BFF server on $PORT)")
			return 0
		}
		if client.ControlPlaneURL != "" && os.Getenv("CONTROL_PLANE_URL") == "" {
			_ = os.Setenv("CONTROL_PLANE_URL", client.ControlPlaneURL)
		}
		if client.PoolServiceURL != "" && os.Getenv("POOL_SERVICE_URL") == "" {
			_ = os.Setenv("POOL_SERVICE_URL", client.PoolServiceURL)
		}
		runUIDaemon(ctx)
		return 0
	default:
		fmt.Fprintf(stderr, "Unknown subcommand %q\n", sub)
		printCLIUsage(stderr)
		return 1
	}
}

func cliVersion(ctx context.Context, client *CLIClient, jsonOut bool, stdout io.Writer) int {
	info := map[string]string{
		"client_version":     client.clientVersion(),
		"client_type":        client.clientType(),
		"min_client_version": DefaultMinClientVersion,
	}
	if client.ControlPlaneURL != "" {
		var cfg FleetConfig
		if status, err := client.doJSON(ctx, http.MethodGet, client.ControlPlaneURL+"/v1/config", nil, nil, &cfg); err == nil && status == http.StatusOK {
			if cfg.ServerVersion != "" {
				info["server_version"] = cfg.ServerVersion
			}
			if cfg.MinClientVersion != "" {
				info["min_client_version"] = cfg.MinClientVersion
			}
		}
	}
	if jsonOut {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(info)
		return 0
	}
	if sv := info["server_version"]; sv != "" {
		fmt.Fprintf(stdout, "sbx client v%s (server v%s, min_client_version=%s)\n",
			info["client_version"], sv, info["min_client_version"])
	} else {
		fmt.Fprintf(stdout, "sbx client v%s\n", info["client_version"])
	}
	return 0
}

func printCLIUsage(w io.Writer) {
	fmt.Fprintln(w, `Usage: sbx (or gcloud run sandbox) [GLOBAL_FLAGS] <COMMAND> [ARGS]

Global Flags:
  --control-plane-url=URL   Control Plane base URL (or SBX_CONTROL_PLANE_URL)
  --pool-url=URL            Sandbox Pool base URL (or SBX_POOL_URL)
  --cache-dir=DIR           Directory for candidates.json and routes.json
  --config-dir=DIR          Directory for config.json (or SBX_CONFIG_DIR)
  --json                    Format output as JSON
  -h, --help                Show help for command

Commands:
  init [<CONTROL_PLANE_URL>] [--url=URL] [--project=PROJECT] [--region=REGION]
  version
  create [NAME] [-q|--quiet|--id-only] [--template=ubuntu|python|node|postgres] [--persistent] [--cpu=1] [--memory=1Gi] [--allow-egress=true|false|--no-allow-egress] [--idle-timeout=SEC|--auto-suspend=SEC|--no-auto-suspend] [--env=K=V] [--metadata=K=V] [--host=HOST_ID] [--pinned] [--from-snapshot=SNAP]
  list [--name=NAME] [--prefix=PREFIX] [--status=RUNNING|SUSPENDED]
  inspect <ID_OR_NAME>
  update <ID_OR_NAME> [--cpu=C] [--memory=M] [--allow-egress=true|false|--no-allow-egress] [--idle-timeout=SEC|--auto-suspend=SEC|--no-auto-suspend] [--env=K=V] [--metadata=K=V]
  delete <ID_OR_NAME> [ID2...]
  exec <ID_OR_NAME> [--stream|-s] [--workdir=/workspace] [--background] [--timeout=SEC] [--allow-egress=true|false|--no-allow-egress] [--env=K=V] -- <COMMAND...>
  preview-url <ID_OR_NAME> [--port=8000] [--path=/]
  port-forward <ID_OR_NAME> <LOCAL_PORT>:<REMOTE_PORT>
  ssh <ID_OR_NAME>
  cp <SRC> <DST>               (use <ID_OR_NAME>:/path for remote sandbox path)
  logs <ID_OR_NAME> [--tail=N]
  stop <ID_OR_NAME>
  suspend <ID_OR_NAME>
  start <ID_OR_NAME>
  resume <ID_OR_NAME>
  copy <SRC_ID_OR_NAME> <DST_NAME> [-q|--quiet|--id-only] [--target-host=HOST_ID]
  migrate <ID_OR_NAME|--host=HOST_ID> [--to-host=HOST_ID]
  snapshot create <ID_OR_NAME> [--name=SNAP_ID] [--force] | list | delete <SNAP_ID>
  template list | create <NAME> [--from-snapshot=<SNAP_ID> | --from-sandbox=<ID>] [--force] | delete <NAME>
  config get | set [--max-density=N] [--host-cpu=4] [--host-memory=8Gi] [--default-idle-timeout=SEC|--auto-suspend=SEC] [--min-free-memory-mb=512] [--min-free-memory-pct=15] [--max-memory-overcommit-ratio=2.0] [--hot-memory-threshold-pct=85] [--hot-cpu-threshold-pct=90] [--min-client-version=1.0.0] [--target-revision=REV] [--auto-bin-pack=true|false]
  hosts list | drain <HOST_ID> | simulate-crash <HOST_ID> | compact
  stats
  metrics [--host=HOST_ID] [--sandbox=SANDBOX_ID] [--history]
  deploy [--project=PROJECT] [--region=REGION] [--bucket=BUCKET] [--host-cpu=4] [--host-memory=8Gi]
  ui [--control-plane-url=URL] [--pool-url=URL]`)
}

// parseDurationSecondsFlag parses either integer seconds ("120", "0") or Go duration strings ("2m", "120s", "0s").
func parseDurationSecondsFlag(raw string) (int, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}
	if v, err := strconv.Atoi(s); err == nil {
		if v < 0 {
			return 0, fmt.Errorf("duration cannot be negative: %q", raw)
		}
		return v, nil
	}
	if d, err := time.ParseDuration(s); err == nil {
		if d < 0 {
			return 0, fmt.Errorf("duration cannot be negative: %q", raw)
		}
		return int(d.Seconds()), nil
	}
	return 0, fmt.Errorf("invalid duration or seconds %q (expected integer seconds like 120 or duration like 2m, 30s)", raw)
}

type multiKeyValFlag []string

func (m *multiKeyValFlag) String() string {
	return strings.Join(*m, ",")
}

func (m *multiKeyValFlag) Set(v string) error {
	*m = append(*m, v)
	return nil
}

func (m multiKeyValFlag) ToMap() map[string]string {
	out := make(map[string]string)
	for _, entry := range m {
		for _, pair := range strings.Split(entry, ",") {
			if k, v, ok := strings.Cut(strings.TrimSpace(pair), "="); ok && k != "" {
				out[k] = v
			}
		}
	}
	return out
}

func hasHelpFlag(args []string) bool {
	for _, a := range args {
		if a == "--" {
			return false
		}
		if a == "--help" || a == "-h" {
			return true
		}
	}
	return false
}

func extractPositionalAndFlags(args []string) (positional []string, flagArgs []string) {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			positional = append(positional, args[i:]...)
			break
		}
		if strings.HasPrefix(a, "-") {
			flagArgs = append(flagArgs, a)
			if !strings.Contains(a, "=") && a != "--persistent" && a != "--pinned" && a != "--allow-egress" && a != "--no-allow-egress" && a != "--no-auto-suspend" && a != "--stream" && a != "-s" && a != "--history" && a != "--background" && a != "-d" && a != "--json" && a != "--force" && a != "-q" && a != "--quiet" && a != "--id-only" && a != "--help" && a != "-h" {
				if i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
					i++
					flagArgs = append(flagArgs, args[i])
				}
			}
		} else {
			positional = append(positional, a)
		}
	}
	return positional, flagArgs
}

func cliInit(ctx context.Context, client *CLIClient, args []string, jsonOut bool, stdout, stderr io.Writer) int {
	if hasHelpFlag(args) {
		fmt.Fprintln(stdout, "Usage: sbx init [<CONTROL_PLANE_URL>] [--url=CONTROL_PLANE_URL] [--project=PROJECT] [--region=REGION]")
		return 0
	}
	pos, fl := extractPositionalAndFlags(args)
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	fs.SetOutput(stderr)
	urlFlag := fs.String("url", "", "Control plane URL (https://sandbox-control-plane-...)")
	projectFlag := fs.String("project", "", "GCP Project ID to auto-discover sandbox-control-plane URL")
	regionFlag := fs.String("region", "us-west1", "Cloud Run region for auto-discovery")
	if err := fs.Parse(fl); err != nil {
		return 2
	}

	cpURL := strings.TrimSpace(*urlFlag)
	if cpURL == "" && len(pos) > 0 {
		cpURL = strings.TrimSpace(pos[0])
	}
	if cpURL == "" && client.ControlPlaneURL != "" {
		cpURL = client.ControlPlaneURL
	}
	if cpURL == "" {
		proj := resolveCLIProject(ctx, *projectFlag)
		if proj != "" {
			if out, err := exec.CommandContext(ctx, "gcloud", "run", "services", "describe", "sandbox-control-plane",
				"--project="+proj, "--region="+*regionFlag, "--format=value(status.url)").Output(); err == nil {
				cpURL = strings.TrimSpace(string(out))
			}
			if cpURL == "" {
				fmt.Fprintf(stderr, "Error: Cloud Run service 'sandbox-control-plane' not found in project %q (region %q).\nRun 'sbx deploy --project=%s --region=%s' first to deploy the fleet, or pass <CONTROL_PLANE_URL>.\n", proj, *regionFlag, proj, *regionFlag)
				return 1
			}
		}
	}
	cpURL = strings.TrimRight(cpURL, "/")
	if cpURL == "" {
		fmt.Fprintln(stderr, "Error: control plane URL is required (pass <CONTROL_PLANE_URL> or --project=<GCP_PROJECT>)")
		return 1
	}

	client.ControlPlaneURL = cpURL
	authCached := false
	if strings.HasPrefix(cpURL, "https://") {
		if tok, err := GetIDToken(ctx, cpURL); err == nil && tok != "" {
			authCached = true
		}
	}

	candResp, err := client.RefreshCandidates(ctx)
	if err != nil {
		fmt.Fprintf(stderr, "Error contacting control plane at %s: %v\n", cpURL, err)
		return 1
	}
	if candResp.PoolServiceURL != "" {
		client.PoolServiceURL = strings.TrimRight(candResp.PoolServiceURL, "/")
	}
	client.SaveEndpoints(client.ControlPlaneURL, client.PoolServiceURL)

	hostCPU := candResp.HostCPU
	if hostCPU == "" {
		hostCPU = "4"
	}
	hostMem := candResp.HostMemory
	if hostMem == "" {
		hostMem = "8Gi"
	}
	if jsonOut {
		_ = json.NewEncoder(stdout).Encode(map[string]any{
			"control_plane_url": client.ControlPlaneURL,
			"pool_service_url":  client.PoolServiceURL,
			"cache_dir":         client.CacheDir,
			"max_density":       candResp.MaxDensity,
			"host_cpu":          hostCPU,
			"host_memory":       hostMem,
			"target_revision":   candResp.TargetRevision,
			"candidate_hosts":   len(candResp.Candidates),
			"auth_cached":       authCached,
		})
		return 0
	}
	fmt.Fprintf(stdout, "Initialized sbx configuration in %s\n", client.CacheDir)
	fmt.Fprintf(stdout, "  Control Plane URL: %s\n", client.ControlPlaneURL)
	fmt.Fprintf(stdout, "  Pool Service URL:  %s\n", client.PoolServiceURL)
	fmt.Fprintf(stdout, "  Host Size:         %s vCPU / %s RAM\n", hostCPU, hostMem)
	fmt.Fprintf(stdout, "  Max Density:       %d sandboxes/host\n", candResp.MaxDensity)
	if authCached {
		fmt.Fprintf(stdout, "  Auth Token:        Cached via gcloud (token_cache.json)\n")
	}
	return 0
}

func cliCreate(ctx context.Context, client *CLIClient, args []string, jsonOut bool, stdout, stderr io.Writer) int {
	if hasHelpFlag(args) {
		fmt.Fprintln(stdout, "Usage: sbx create [NAME] [--name=NAME] [-q|--quiet|--id-only] [--template=ubuntu|python|node|postgres] [--persistent] [--cpu=1] [--memory=1Gi] [--allow-egress=true|false|--no-allow-egress] [--host=HOST_ID] [--pinned] [--from-snapshot=SNAP_ID] [--idle-timeout=SEC|--auto-suspend=SEC|--no-auto-suspend] [--env=K=V] [--metadata=K=V]")
		return 0
	}
	pos, fl := extractPositionalAndFlags(args)
	fs := flag.NewFlagSet("create", flag.ContinueOnError)
	fs.SetOutput(stderr)
	nameFlag := fs.String("name", "", "Human-readable sandbox name (host generates unique sandbox_id)")
	tpl := fs.String("template", "ubuntu", "Sandbox template (ubuntu, python, node, postgres, or custom)")
	persistent := fs.Bool("persistent", false, "Persist writable rootfs-upper overlay & /persistent to GCS")
	cpu := fs.String("cpu", "1", "CPU allocation")
	mem := fs.String("memory", "1Gi", "Memory allocation")
	egress := fs.Bool("allow-egress", true, "Allow outbound internet egress")
	noEgress := fs.Bool("no-allow-egress", false, "Disable outbound internet egress")
	fromSnap := fs.String("from-snapshot", "", "Initialize from snapshot ID")
	idleTimeout := fs.String("idle-timeout", "", "Auto-suspend idle timeout in seconds or duration (0=disabled)")
	autoSuspend := fs.String("auto-suspend", "", "Alias for --idle-timeout in seconds or duration (0=disabled)")
	noAutoSuspend := fs.Bool("no-auto-suspend", false, "Disable automatic idle suspend")
	targetHost := fs.String("host", "", "Place sandbox on a specific host session ID")
	pinned := fs.Bool("pinned", false, "Pin sandbox to its host so bin-packing never migrates it")
	force := fs.Bool("force", false, "Force creation")
	quietShort := fs.Bool("q", false, "Print only sandbox_id to stdout")
	quietLong := fs.Bool("quiet", false, "Print only sandbox_id to stdout")
	idOnly := fs.Bool("id-only", false, "Print only sandbox_id to stdout")
	var envFlags, metaFlags multiKeyValFlag
	fs.Var(&envFlags, "env", "KEY=VAL environment variables (repeatable or comma-separated)")
	fs.Var(&metaFlags, "metadata", "KEY=VAL metadata labels (repeatable or comma-separated)")
	if err := fs.Parse(fl); err != nil {
		return 2
	}

	sbxName := strings.TrimSpace(*nameFlag)
	if sbxName == "" && len(pos) > 0 {
		sbxName = pos[0]
	}

	allowEgressVal := *egress
	if *noEgress {
		allowEgressVal = false
	}

	effectiveIdle := 0
	disableAutoSuspend := false
	if strings.TrimSpace(*autoSuspend) != "" {
		v, pErr := parseDurationSecondsFlag(*autoSuspend)
		if pErr != nil {
			fmt.Fprintf(stderr, "Error parsing --auto-suspend: %v\n", pErr)
			return 2
		}
		effectiveIdle = v
		disableAutoSuspend = (effectiveIdle == 0)
	}
	if strings.TrimSpace(*idleTimeout) != "" {
		v, pErr := parseDurationSecondsFlag(*idleTimeout)
		if pErr != nil {
			fmt.Fprintf(stderr, "Error parsing --idle-timeout: %v\n", pErr)
			return 2
		}
		effectiveIdle = v
		disableAutoSuspend = (effectiveIdle == 0)
	}
	if *noAutoSuspend {
		effectiveIdle = 0
		disableAutoSuspend = true
	}

	rec, err := client.CreateSandbox(ctx, CreateSandboxRequest{
		Name:               sbxName,
		Template:           *tpl,
		Persistent:         *persistent,
		PinnedHost:         *pinned,
		TargetHostID:       *targetHost,
		Force:              *force,
		CPU:                *cpu,
		Memory:             *mem,
		AllowEgress:        allowEgressVal,
		FromSnapshotID:     *fromSnap,
		Env:                envFlags.ToMap(),
		Metadata:           metaFlags.ToMap(),
		IdleTimeoutSec:     effectiveIdle,
		DisableAutoSuspend: disableAutoSuspend,
	})
	if err != nil {
		fmt.Fprintf(stderr, "Error creating sandbox: %v\n", err)
		return 1
	}
	if *quietShort || *quietLong || *idOnly {
		fmt.Fprintln(stdout, rec.SandboxID)
		return 0
	}
	if jsonOut {
		_ = json.NewEncoder(stdout).Encode(rec)
		return 0
	}
	idleDisp := "off"
	if rec.IdleTimeoutSec > 0 {
		idleDisp = fmt.Sprintf("%ds", rec.IdleTimeoutSec)
	}
	fmt.Fprintf(stdout, "Created sandbox %s (name=%s) on host %s (template=%s, status=%s, epoch=%d, persistent=%v, idle_timeout=%s)\n",
		rec.SandboxID, rec.Name, rec.HostID, rec.Template, rec.Status, rec.Epoch, rec.Persistent, idleDisp)
	return 0
}

func cliList(ctx context.Context, client *CLIClient, args []string, jsonOut bool, stdout, stderr io.Writer) int {
	if hasHelpFlag(args) {
		fmt.Fprintln(stdout, "Usage: sbx list [--name=NAME] [--prefix=PREFIX] [--status=RUNNING|SUSPENDED]")
		return 0
	}
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	fs.SetOutput(stderr)
	nameFilter := fs.String("name", "", "Filter by sandbox name")
	prefix := fs.String("prefix", "", "Filter by sandbox ID or name prefix")
	statusFilter := fs.String("status", "", "Filter by status")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	q := url.Values{}
	if *nameFilter != "" {
		q.Set("name", *nameFilter)
	}
	if *prefix != "" {
		q.Set("prefix", *prefix)
	}
	if *statusFilter != "" {
		q.Set("status", *statusFilter)
	}
	u := client.ControlPlaneURL + "/v1/sandboxes"
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	var res struct {
		Sandboxes []SandboxRecord `json:"sandboxes"`
		Count     int             `json:"count"`
	}
	if _, err := client.doJSON(ctx, http.MethodGet, u, nil, nil, &res); err != nil {
		fmt.Fprintf(stderr, "Error listing sandboxes: %v\n", err)
		return 1
	}
	if jsonOut {
		_ = json.NewEncoder(stdout).Encode(res)
		return 0
	}
	tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "SANDBOX_ID\tNAME\tSTATUS\tTEMPLATE\tHOST_ID\tEPOCH\tPERSISTENT\tIDLE_TIMEOUT\tMIGRATIONS\tRECOVERIES")
	for _, s := range res.Sandboxes {
		dispName := s.Name
		if dispName == "" {
			dispName = s.SandboxID
		}
		idleDisp := "off"
		if s.IdleTimeoutSec > 0 {
			idleDisp = fmt.Sprintf("%ds", s.IdleTimeoutSec)
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%d\t%v\t%s\t%d\t%d\n",
			s.SandboxID, dispName, s.Status, s.Template, s.HostID, s.Epoch, s.Persistent, idleDisp, s.MigrationCount, s.CrashRecoveries)
	}
	_ = tw.Flush()
	return 0
}

func cliInspect(ctx context.Context, client *CLIClient, args []string, _ bool, stdout, stderr io.Writer) int {
	if hasHelpFlag(args) {
		fmt.Fprintln(stdout, "Usage: sbx inspect <SANDBOX_ID>")
		return 0
	}
	pos, fl := extractPositionalAndFlags(args)
	if len(fl) > 0 {
		fmt.Fprintf(stderr, "Unknown flag %s\n", fl[0])
		return 2
	}
	if len(pos) == 0 {
		fmt.Fprintln(stderr, "Usage: sbx inspect <SANDBOX_ID>")
		return 1
	}
	sbxID, rErr := client.ResolveUniqueSandboxID(ctx, pos[0])
	if rErr != nil {
		fmt.Fprintf(stderr, "Error inspecting %s: %v\n", pos[0], rErr)
		return 1
	}
	var rec SandboxRecord
	err := client.CallSandboxEndpoint(ctx, http.MethodGet, "/__session/inspect?sandbox_id="+url.QueryEscape(sbxID), sbxID, nil, &rec)
	if err != nil && client.ControlPlaneURL != "" {
		_, err = client.doJSON(ctx, http.MethodGet, client.ControlPlaneURL+"/v1/sandboxes/"+url.PathEscape(sbxID), nil, nil, &rec)
	}
	if err != nil {
		fmt.Fprintf(stderr, "Error inspecting %s: %v\n", pos[0], err)
		return 1
	}
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(rec)
	return 0
}

func cliUpdate(ctx context.Context, client *CLIClient, args []string, jsonOut bool, stdout, stderr io.Writer) int {
	if hasHelpFlag(args) {
		fmt.Fprintln(stdout, "Usage: sbx update <SANDBOX_ID> [--cpu=C] [--memory=M] [--allow-egress=true|false|--no-allow-egress] [--idle-timeout=SEC|--auto-suspend=SEC|--no-auto-suspend] [--env=K=V] [--metadata=K=V]")
		return 0
	}
	pos, fl := extractPositionalAndFlags(args)
	if len(pos) == 0 {
		fmt.Fprintln(stderr, "Usage: sbx update <SANDBOX_ID> [--cpu=C] [--memory=M] [--allow-egress=true|false|--no-allow-egress] [--idle-timeout=SEC|--auto-suspend=SEC|--no-auto-suspend] [--env=K=V] [--metadata=K=V]")
		return 1
	}
	sbxID, rErr := client.ResolveUniqueSandboxID(ctx, pos[0])
	if rErr != nil {
		fmt.Fprintf(stderr, "Error updating %s: %v\n", pos[0], rErr)
		return 1
	}
	fs := flag.NewFlagSet("update", flag.ContinueOnError)
	fs.SetOutput(stderr)
	cpu := fs.String("cpu", "", "CPU limit")
	mem := fs.String("memory", "", "Memory limit")
	idle := fs.String("idle-timeout", "", "Idle timeout seconds or duration (0=disabled)")
	autoSuspend := fs.String("auto-suspend", "", "Alias for --idle-timeout seconds or duration (0=disabled)")
	noAutoSuspend := fs.Bool("no-auto-suspend", false, "Disable automatic idle suspend")
	allowEgressFlag := fs.String("allow-egress", "", "Allow outbound internet egress (true/false)")
	noAllowEgressFlag := fs.Bool("no-allow-egress", false, "Disable outbound internet egress")
	var envFlags, metaFlags multiKeyValFlag
	fs.Var(&envFlags, "env", "KEY=VAL pairs")
	fs.Var(&metaFlags, "metadata", "KEY=VAL metadata pairs")
	// Normalize bare --allow-egress to --allow-egress=true
	var normFl []string
	for _, f := range fl {
		if f == "--allow-egress" {
			normFl = append(normFl, "--allow-egress=true")
		} else {
			normFl = append(normFl, f)
		}
	}
	if err := fs.Parse(normFl); err != nil {
		return 2
	}

	payload := map[string]any{"sandbox_id": sbxID}
	if *cpu != "" {
		payload["cpu"] = *cpu
	}
	if *mem != "" {
		payload["memory"] = *mem
	}
	if strings.TrimSpace(*autoSuspend) != "" {
		v, pErr := parseDurationSecondsFlag(*autoSuspend)
		if pErr != nil {
			fmt.Fprintf(stderr, "Error parsing --auto-suspend: %v\n", pErr)
			return 2
		}
		payload["idle_timeout_sec"] = v
		payload["disable_auto_suspend"] = (v == 0)
	}
	if strings.TrimSpace(*idle) != "" {
		v, pErr := parseDurationSecondsFlag(*idle)
		if pErr != nil {
			fmt.Fprintf(stderr, "Error parsing --idle-timeout: %v\n", pErr)
			return 2
		}
		payload["idle_timeout_sec"] = v
		payload["disable_auto_suspend"] = (v == 0)
	}
	if *noAutoSuspend {
		payload["idle_timeout_sec"] = 0
		payload["disable_auto_suspend"] = true
	}
	if *noAllowEgressFlag {
		payload["allow_egress"] = false
	} else if *allowEgressFlag != "" {
		payload["allow_egress"] = strings.EqualFold(*allowEgressFlag, "true") || *allowEgressFlag == "1"
	}
	if len(envFlags) > 0 {
		payload["env"] = envFlags.ToMap()
	}
	if len(metaFlags) > 0 {
		payload["metadata"] = metaFlags.ToMap()
	}
	var rec SandboxRecord
	if err := client.CallSandboxEndpoint(ctx, http.MethodPost, "/__session/update", sbxID, payload, &rec); err != nil {
		fmt.Fprintf(stderr, "Error updating %s: %v\n", pos[0], err)
		return 1
	}
	if jsonOut {
		_ = json.NewEncoder(stdout).Encode(rec)
		return 0
	}
	fmt.Fprintf(stdout, "Updated sandbox %s (cpu=%s, memory=%s, idle_timeout=%ds)\n", rec.SandboxID, rec.CPU, rec.Memory, rec.IdleTimeoutSec)
	return 0
}

func cliDelete(ctx context.Context, client *CLIClient, args []string, jsonOut bool, stdout, stderr io.Writer) int {
	if hasHelpFlag(args) {
		fmt.Fprintln(stdout, "Usage: sbx delete <SANDBOX_ID> [SANDBOX_ID2...]")
		return 0
	}
	pos, fl := extractPositionalAndFlags(args)
	fs := flag.NewFlagSet("delete", flag.ContinueOnError)
	fs.SetOutput(stderr)
	_ = fs.Bool("force", false, "Force delete")
	if err := fs.Parse(fl); err != nil {
		return 2
	}
	if len(pos) == 0 {
		fmt.Fprintln(stderr, "Usage: sbx delete <SANDBOX_ID> [SANDBOX_ID2...]")
		return 1
	}
	code := 0
	for _, rawID := range pos {
		sbxID, rErr := client.ResolveUniqueSandboxID(ctx, rawID)
		if rErr != nil {
			fmt.Fprintf(stderr, "Error deleting %s: %v\n", rawID, rErr)
			code = 1
			continue
		}
		var res map[string]any
		err := client.CallSandboxEndpoint(ctx, http.MethodPost, "/__session/delete", sbxID, map[string]string{"sandbox_id": sbxID}, &res)
		if err != nil && client.ControlPlaneURL != "" {
			_, err = client.doJSON(ctx, http.MethodDelete, client.ControlPlaneURL+"/v1/sandboxes/"+url.PathEscape(sbxID), nil, nil, &res)
		}
		if err != nil {
			fmt.Fprintf(stderr, "Error deleting %s: %v\n", rawID, err)
			code = 1
			continue
		}
		if jsonOut {
			_ = json.NewEncoder(stdout).Encode(res)
		} else {
			fmt.Fprintf(stdout, "Deleted sandbox %s\n", sbxID)
		}
	}
	return code
}

func cliExec(ctx context.Context, client *CLIClient, args []string, jsonOut bool, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "Usage: sbx exec <SANDBOX_ID> [--stream|-s] [--workdir=/workspace] [--background] [--timeout=SEC] [--allow-egress=true|false|--no-allow-egress] [--env=K=V] -- <COMMAND...>")
		return 1
	}
	var sbxID string
	workdir := "/workspace"
	bg := false
	streamOut := false
	timeoutSec := 60
	var allowEgressOverride *bool
	var envFlags multiKeyValFlag
	var cmdParts []string

	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			cmdParts = append(cmdParts, args[i+1:]...)
			break
		}
		if (a == "-h" || a == "--help") && sbxID == "" {
			fmt.Fprintln(stdout, "Usage: sbx exec <SANDBOX_ID> [--stream|-s] [--workdir=/workspace] [--background] [--timeout=SEC] [--allow-egress=true|false|--no-allow-egress] [--env=K=V] -- <COMMAND...>")
			return 0
		}
		if a == "--background" || a == "-d" {
			bg = true
		} else if a == "--stream" || a == "-s" {
			streamOut = true
		} else if strings.HasPrefix(a, "--stream=") {
			valStr := strings.TrimPrefix(a, "--stream=")
			streamOut = strings.EqualFold(valStr, "true") || valStr == "1"
		} else if a == "--allow-egress" {
			t := true
			allowEgressOverride = &t
		} else if a == "--no-allow-egress" {
			f := false
			allowEgressOverride = &f
		} else if strings.HasPrefix(a, "--allow-egress=") {
			valStr := strings.TrimPrefix(a, "--allow-egress=")
			v := strings.EqualFold(valStr, "true") || valStr == "1"
			allowEgressOverride = &v
		} else if strings.HasPrefix(a, "--workdir=") {
			workdir = strings.TrimPrefix(a, "--workdir=")
		} else if a == "--workdir" && i+1 < len(args) {
			i++
			workdir = args[i]
		} else if strings.HasPrefix(a, "--timeout=") {
			v, tErr := parseDurationSecondsFlag(strings.TrimPrefix(a, "--timeout="))
			if tErr != nil || v <= 0 {
				fmt.Fprintf(stderr, "Invalid --timeout value %q: must be positive seconds or duration (e.g. 30, 30s, 2m)\n", strings.TrimPrefix(a, "--timeout="))
				return 2
			}
			timeoutSec = v
		} else if a == "--timeout" && i+1 < len(args) {
			i++
			v, tErr := parseDurationSecondsFlag(args[i])
			if tErr != nil || v <= 0 {
				fmt.Fprintf(stderr, "Invalid --timeout value %q: must be positive seconds or duration (e.g. 30, 30s, 2m)\n", args[i])
				return 2
			}
			timeoutSec = v
		} else if strings.HasPrefix(a, "--env=") {
			_ = envFlags.Set(strings.TrimPrefix(a, "--env="))
		} else if a == "--env" && i+1 < len(args) {
			i++
			_ = envFlags.Set(args[i])
		} else if strings.HasPrefix(a, "-") {
			if sbxID == "" {
				fmt.Fprintf(stderr, "Unknown flag %s\n", a)
				return 2
			}
			// If sandbox ID was already parsed and no `--` was used, treat unknown flag before command as flag error
			if len(cmdParts) == 0 {
				fmt.Fprintf(stderr, "Unknown flag %s (use -- before command arguments starting with -)\n", a)
				return 2
			}
		} else {
			if sbxID == "" {
				sbxID = a
			} else {
				cmdParts = append(cmdParts, args[i:]...)
				break
			}
		}
	}
	if sbxID == "" {
		fmt.Fprintln(stderr, "Error: sandbox ID is required for exec")
		return 1
	}
	if len(cmdParts) == 0 {
		fmt.Fprintln(stderr, "Error: command is required for exec")
		return 1
	}
	if streamOut && bg {
		fmt.Fprintln(stderr, "Error: --stream cannot be combined with --background")
		return 2
	}
	resolvedID, rErr := client.ResolveUniqueSandboxID(ctx, sbxID)
	if rErr != nil {
		fmt.Fprintf(stderr, "Error executing in %s: %v\n", sbxID, rErr)
		return 1
	}

	req := ExecRequest{
		SandboxID:   resolvedID,
		Command:     cmdParts,
		Workdir:     workdir,
		Env:         envFlags.ToMap(),
		Background:  bg,
		Stream:      streamOut,
		TimeoutSec:  timeoutSec,
		AllowEgress: allowEgressOverride,
	}

	if streamOut {
		resp, err := client.ExecStream(ctx, req, func(evt ExecStreamEvent) {
			if jsonOut {
				_ = json.NewEncoder(stdout).Encode(evt)
				return
			}
			switch evt.Type {
			case "stdout":
				fmt.Fprint(stdout, evt.Data)
			case "stderr":
				fmt.Fprint(stderr, evt.Data)
			}
		})
		if err != nil {
			fmt.Fprintf(stderr, "Error streaming exec in %s: %v\n", sbxID, err)
			return 1
		}
		return resp.ExitCode
	}

	resp, err := client.Exec(ctx, req)
	if err != nil {
		fmt.Fprintf(stderr, "Error executing in %s: %v\n", sbxID, err)
		return 1
	}
	if jsonOut {
		_ = json.NewEncoder(stdout).Encode(resp)
		return resp.ExitCode
	}
	if resp.Stdout != "" {
		fmt.Fprint(stdout, resp.Stdout)
	}
	if resp.Stderr != "" {
		fmt.Fprint(stderr, resp.Stderr)
	}
	return resp.ExitCode
}

func cliPreviewURL(ctx context.Context, client *CLIClient, args []string, jsonOut bool, stdout, stderr io.Writer) int {
	if hasHelpFlag(args) {
		fmt.Fprintln(stdout, "Usage: sbx preview-url <SANDBOX_ID_OR_NAME> [--port=8000] [--path=/]")
		return 0
	}
	pos, fl := extractPositionalAndFlags(args)
	fs := flag.NewFlagSet("preview-url", flag.ContinueOnError)
	fs.SetOutput(stderr)
	port := fs.Int("port", 8000, "Target TCP port inside the sandbox")
	subPath := fs.String("path", "/", "URL path to append")
	if err := fs.Parse(fl); err != nil {
		return 2
	}
	if len(pos) == 0 {
		fmt.Fprintln(stderr, "Usage: sbx preview-url <SANDBOX_ID_OR_NAME> [--port=8000] [--path=/]")
		return 1
	}
	if len(pos) > 1 && *port == 8000 {
		p, pErr := strconv.Atoi(pos[1])
		if pErr != nil || p < 1 || p > 65535 {
			fmt.Fprintf(stderr, "Error: invalid port %q (must be between 1 and 65535)\n", pos[1])
			return 1
		}
		*port = p
	}
	if *port < 1 || *port > 65535 {
		fmt.Fprintf(stderr, "Error: invalid port %d (must be between 1 and 65535)\n", *port)
		return 1
	}
	sbxID, rErr := client.ResolveUniqueSandboxID(ctx, pos[0])
	if rErr != nil {
		fmt.Fprintf(stderr, "Error resolving %s: %v\n", pos[0], rErr)
		return 1
	}
	p := *subPath
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	if client.PoolServiceURL == "" && client.ControlPlaneURL != "" {
		_, _ = client.RefreshCandidates(ctx)
	}
	baseURL := strings.TrimRight(client.ControlPlaneURL, "/")
	if baseURL == "" {
		baseURL = strings.TrimRight(client.PoolServiceURL, "/")
	}
	previewURL := fmt.Sprintf("%s/proxy/%s/%d%s", baseURL, sbxID, *port, p)
	var poolPreviewURL string
	if poolBase := strings.TrimRight(client.PoolServiceURL, "/"); poolBase != "" {
		poolPreviewURL = fmt.Sprintf("%s/proxy/%s/%d%s", poolBase, sbxID, *port, p)
	}
	hostID := client.GetRoute(ctx, sbxID)
	if jsonOut {
		_ = json.NewEncoder(stdout).Encode(map[string]any{
			"sandbox_id":   sbxID,
			"port":         *port,
			"preview_url":  previewURL,
			"host_id":      hostID,
			"pool_url":     poolPreviewURL,
			"curl_example": fmt.Sprintf("curl -H \"Authorization: Bearer $(gcloud auth print-identity-token)\" %q", previewURL),
		})
		return 0
	}
	fmt.Fprintln(stdout, previewURL)
	return 0
}

func cliPortForward(ctx context.Context, client *CLIClient, args []string, stdout, stderr io.Writer) int {
	if hasHelpFlag(args) {
		fmt.Fprintln(stdout, "Usage: sbx port-forward <SANDBOX_ID_OR_NAME> <LOCAL_PORT>[:<REMOTE_PORT>]")
		return 0
	}
	pos, fl := extractPositionalAndFlags(args)
	fs := flag.NewFlagSet("port-forward", flag.ContinueOnError)
	fs.SetOutput(stderr)
	portFlag := fs.Int("port", 0, "Remote port inside sandbox (optional shorthand)")
	if err := fs.Parse(fl); err != nil {
		return 2
	}
	if len(pos) == 0 || (len(pos) < 2 && *portFlag <= 0) {
		fmt.Fprintln(stderr, "Usage: sbx port-forward <SANDBOX_ID_OR_NAME> <LOCAL_PORT>[:<REMOTE_PORT>]")
		return 1
	}
	sbxID, rErr := client.ResolveUniqueSandboxID(ctx, pos[0])
	if rErr != nil {
		fmt.Fprintf(stderr, "Error resolving %s: %v\n", pos[0], rErr)
		return 1
	}
	var localPort, remotePort int
	if len(pos) >= 2 {
		localPortStr, remotePortStr, hasColon := strings.Cut(pos[1], ":")
		if !hasColon {
			remotePortStr = localPortStr
		}
		var lErr, rPErr error
		localPort, lErr = strconv.Atoi(localPortStr)
		remotePort, rPErr = strconv.Atoi(remotePortStr)
		if lErr != nil || rPErr != nil || localPort < 1 || localPort > 65535 || remotePort < 1 || remotePort > 65535 {
			fmt.Fprintf(stderr, "Invalid port mapping %q; expected <LOCAL_PORT>:<REMOTE_PORT> (1..65535)\n", pos[1])
			return 1
		}
	} else {
		if *portFlag < 1 || *portFlag > 65535 {
			fmt.Fprintf(stderr, "Invalid --port %d (must be 1..65535)\n", *portFlag)
			return 1
		}
		localPort = *portFlag
		remotePort = *portFlag
	}

	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", localPort))
	if err != nil {
		fmt.Fprintf(stderr, "Error listening on 127.0.0.1:%d: %v\n", localPort, err)
		return 1
	}
	defer ln.Close()
	fmt.Fprintf(stdout, "Forwarding %s -> %s:%d\n", ln.Addr().String(), sbxID, remotePort)

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		localConn, acceptErr := ln.Accept()
		if acceptErr != nil {
			if ctx.Err() != nil {
				return 0
			}
			return 1
		}
		go func(c net.Conn) {
			defer c.Close()
			_ = forwardLocalConnToSandbox(ctx, client, sbxID, remotePort, c)
		}(localConn)
	}
}

func forwardLocalConnToSandbox(ctx context.Context, client *CLIClient, sandboxID string, remotePort int, localConn net.Conn) error {
	defer localConn.Close()
	if client.PoolServiceURL == "" && client.ControlPlaneURL != "" {
		_, _ = client.RefreshCandidates(ctx)
	}
	baseURL := strings.TrimRight(client.PoolServiceURL, "/")
	if baseURL == "" {
		baseURL = strings.TrimRight(client.ControlPlaneURL, "/")
	}
	if baseURL == "" {
		return errors.New("neither pool_service_url nor control_plane_url is configured")
	}
	parsedPool, err := url.Parse(baseURL)
	if err != nil {
		return err
	}
	dialAddr := parsedPool.Host
	if parsedPool.Port() == "" {
		if strings.EqualFold(parsedPool.Scheme, "https") {
			dialAddr = net.JoinHostPort(parsedPool.Hostname(), "443")
		} else {
			dialAddr = net.JoinHostPort(parsedPool.Hostname(), "80")
		}
	}

	hostID := client.GetRoute(ctx, sandboxID)
	if hostID == "" {
		cands := client.GetAdaptiveCandidates(ctx, false)
		if len(cands) > 0 {
			hostID = cands[0].HostID
		} else {
			hostID = NewRandomID("host")
		}
	}

	for attempt := 0; attempt < 3; attempt++ {
		var rawConn net.Conn
		var dErr error
		if strings.EqualFold(parsedPool.Scheme, "https") {
			rawConn, dErr = tls.DialWithDialer(&net.Dialer{Timeout: 10 * time.Second}, "tcp", dialAddr, &tls.Config{
				ServerName: parsedPool.Hostname(),
				NextProtos: []string{"http/1.1"},
			})
		} else {
			rawConn, dErr = net.DialTimeout("tcp", dialAddr, 10*time.Second)
		}
		if dErr != nil {
			return dErr
		}
		authHdr := ""
		if strings.EqualFold(parsedPool.Scheme, "https") {
			if tok, tokErr := GetIDToken(ctx, baseURL); tokErr == nil && tok != "" {
				authHdr = "Authorization: Bearer " + tok + "\r\n"
			}
		}
		reqPath := fmt.Sprintf("/__session/port-forward?sandbox_id=%s&port=%d", url.QueryEscape(sandboxID), remotePort)
		_, _ = fmt.Fprintf(rawConn, "GET %s HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nX-Serverless-Session-Id: %s\r\nX-Sandbox-Id: %s\r\nX-Sandbox-Forward-Port: %d\r\nX-Sbx-Client-Version: %s\r\n%s\r\n",
			reqPath, parsedPool.Host, hostID, sandboxID, remotePort, client.clientVersion(), authHdr)
		br := bufio.NewReader(rawConn)
		resp, rErr := http.ReadResponse(br, nil)
		if rErr != nil {
			_ = rawConn.Close()
			return rErr
		}
		if resp.StatusCode == http.StatusConflict || resp.StatusCode == http.StatusNotFound {
			redirect := strings.TrimSpace(resp.Header.Get("X-Sandbox-Host-Session"))
			if redirect == "" {
				redirect = strings.TrimSpace(resp.Header.Get("X-Sandbox-Current-Host"))
			}
			_ = resp.Body.Close()
			_ = rawConn.Close()
			if redirect == "" && client.ControlPlaneURL != "" {
				var inspected SandboxRecord
				if _, iErr := client.doJSON(ctx, http.MethodGet, client.ControlPlaneURL+"/v1/sandboxes/"+url.PathEscape(sandboxID), nil, nil, &inspected); iErr == nil && inspected.HostID != "" {
					redirect = inspected.HostID
				}
			}
			if redirect != "" && redirect != hostID {
				hostID = redirect
				client.SetRouteAndName(sandboxID, sandboxID, hostID)
				continue
			}
			return fmt.Errorf("port-forward failed with HTTP %d", resp.StatusCode)
		}
		if resp.StatusCode != http.StatusSwitchingProtocols && resp.StatusCode != http.StatusOK {
			_ = rawConn.Close()
			return fmt.Errorf("port-forward returned HTTP %d", resp.StatusCode)
		}
		if br.Buffered() > 0 {
			buf := make([]byte, br.Buffered())
			if n, _ := br.Read(buf); n > 0 {
				_, _ = localConn.Write(buf[:n])
			}
		}
		done := make(chan struct{}, 2)
		go func() {
			_, _ = io.Copy(rawConn, localConn)
			done <- struct{}{}
		}()
		go func() {
			_, _ = io.Copy(localConn, rawConn)
			done <- struct{}{}
		}()
		<-done
		_ = rawConn.Close()
		return nil
	}
	return fmt.Errorf("exceeded port-forward redirects")
}

func cliSSH(ctx context.Context, client *CLIClient, args []string, stdout, stderr io.Writer) int {
	if hasHelpFlag(args) {
		fmt.Fprintln(stdout, "Usage: sbx ssh <SANDBOX_ID>")
		return 0
	}
	if len(args) == 0 {
		fmt.Fprintln(stderr, "Usage: sbx ssh <SANDBOX_ID>")
		return 1
	}
	sbxID, rErr := client.ResolveUniqueSandboxID(ctx, args[0])
	if rErr != nil {
		fmt.Fprintf(stderr, "Error: %v\n", rErr)
		return 1
	}
	fmt.Fprintf(stdout, "Connected to interactive session on sandbox %s (type 'exit' to quit)\n", sbxID)
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Fprintf(stdout, "%s:/workspace# ", sbxID)
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "exit" || line == "quit" {
			break
		}
		if line == "" {
			continue
		}
		resp, err := client.Exec(ctx, ExecRequest{
			SandboxID: sbxID,
			Command:   []string{"sh", "-c", line},
		})
		if err != nil {
			fmt.Fprintf(stderr, "Error: %v\n", err)
			continue
		}
		if resp.Stdout != "" {
			fmt.Fprint(stdout, resp.Stdout)
		}
		if resp.Stderr != "" {
			fmt.Fprint(stderr, resp.Stderr)
		}
	}
	return 0
}

func cliCP(ctx context.Context, client *CLIClient, args []string, jsonOut bool, stdout, stderr io.Writer) int {
	if hasHelpFlag(args) {
		fmt.Fprintln(stdout, "Usage: sbx cp <LOCAL_PATH> <SANDBOX_ID>:<REMOTE_PATH>  or  sbx cp <SANDBOX_ID>:<REMOTE_PATH> <LOCAL_PATH>")
		return 0
	}
	if len(args) < 2 {
		fmt.Fprintln(stderr, "Usage: sbx cp <LOCAL_PATH> <SANDBOX_ID>:<REMOTE_PATH>  or  sbx cp <SANDBOX_ID>:<REMOTE_PATH> <LOCAL_PATH>")
		return 1
	}
	src, dst := args[0], args[1]
	if rawID, remotePath, ok := strings.Cut(dst, ":"); ok && rawID != "" && remotePath != "" {
		sbxID, rErr := client.ResolveUniqueSandboxID(ctx, rawID)
		if rErr != nil {
			fmt.Fprintf(stderr, "Upload failed: %v\n", rErr)
			return 1
		}
		data, err := os.ReadFile(src)
		if err != nil {
			fmt.Fprintf(stderr, "Error reading local file %s: %v\n", src, err)
			return 1
		}
		var resp FileCopyResponse
		err = client.CallSandboxEndpoint(ctx, http.MethodPost, "/__session/cp", sbxID, FileCopyRequest{
			SandboxID: sbxID,
			Direction: "upload",
			Path:      remotePath,
			Content:   data,
		}, &resp)
		if err != nil {
			fmt.Fprintf(stderr, "Upload failed: %v\n", err)
			return 1
		}
		if jsonOut {
			_ = json.NewEncoder(stdout).Encode(resp)
		} else {
			fmt.Fprintf(stdout, "Uploaded %d bytes to %s:%s\n", resp.SizeBytes, resp.SandboxID, remotePath)
		}
		return 0
	}
	if rawID, remotePath, ok := strings.Cut(src, ":"); ok && rawID != "" && remotePath != "" {
		sbxID, rErr := client.ResolveUniqueSandboxID(ctx, rawID)
		if rErr != nil {
			fmt.Fprintf(stderr, "Download failed: %v\n", rErr)
			return 1
		}
		var resp FileCopyResponse
		err := client.CallSandboxEndpoint(ctx, http.MethodPost, "/__session/cp", sbxID, FileCopyRequest{
			SandboxID: sbxID,
			Direction: "download",
			Path:      remotePath,
		}, &resp)
		if err != nil {
			fmt.Fprintf(stderr, "Download failed: %v\n", err)
			return 1
		}
		if err := os.WriteFile(dst, resp.Content, 0644); err != nil {
			fmt.Fprintf(stderr, "Error writing local file %s: %v\n", dst, err)
			return 1
		}
		if jsonOut {
			_ = json.NewEncoder(stdout).Encode(resp)
		} else {
			fmt.Fprintf(stdout, "Downloaded %d bytes from %s:%s to %s\n", resp.SizeBytes, resp.SandboxID, remotePath, dst)
		}
		return 0
	}
	fmt.Fprintln(stderr, "Error: one of source or destination must be <SANDBOX_ID>:<PATH>")
	return 1
}

func cliLogs(ctx context.Context, client *CLIClient, args []string, jsonOut bool, stdout, stderr io.Writer) int {
	if hasHelpFlag(args) {
		fmt.Fprintln(stdout, "Usage: sbx logs <SANDBOX_ID> [--tail=N]")
		return 0
	}
	pos, fl := extractPositionalAndFlags(args)
	fs := flag.NewFlagSet("logs", flag.ContinueOnError)
	fs.SetOutput(stderr)
	tail := fs.Int("tail", 0, "Number of lines")
	if err := fs.Parse(fl); err != nil {
		return 2
	}
	if len(pos) == 0 {
		fmt.Fprintln(stderr, "Usage: sbx logs <SANDBOX_ID> [--tail=N]")
		return 1
	}
	sbxID, rErr := client.ResolveUniqueSandboxID(ctx, pos[0])
	if rErr != nil {
		fmt.Fprintf(stderr, "Error fetching logs for %s: %v\n", pos[0], rErr)
		return 1
	}
	var res struct {
		SandboxID string `json:"sandbox_id"`
		HostID    string `json:"host_id"`
		Logs      string `json:"logs"`
	}
	ep := "/__session/logs?sandbox_id=" + url.QueryEscape(sbxID)
	if *tail > 0 {
		ep += "&tail=" + strconv.Itoa(*tail)
	}
	if err := client.CallSandboxEndpoint(ctx, http.MethodGet, ep, sbxID, nil, &res); err != nil {
		fmt.Fprintf(stderr, "Error fetching logs for %s: %v\n", pos[0], err)
		return 1
	}
	if jsonOut {
		_ = json.NewEncoder(stdout).Encode(res)
		return 0
	}
	fmt.Fprint(stdout, res.Logs)
	if !strings.HasSuffix(res.Logs, "\n") && res.Logs != "" {
		fmt.Fprintln(stdout)
	}
	return 0
}

func cliStopOrSuspend(ctx context.Context, client *CLIClient, args []string, action string, jsonOut bool, stdout, stderr io.Writer) int {
	if hasHelpFlag(args) {
		fmt.Fprintf(stdout, "Usage: sbx %s <SANDBOX_ID>\n", action)
		return 0
	}
	pos, fl := extractPositionalAndFlags(args)
	if len(fl) > 0 {
		fmt.Fprintf(stderr, "Unknown flag %s\n", fl[0])
		return 2
	}
	if len(pos) == 0 {
		fmt.Fprintf(stderr, "Usage: sbx %s <SANDBOX_ID>\n", action)
		return 1
	}
	sbxID, rErr := client.ResolveUniqueSandboxID(ctx, pos[0])
	if rErr != nil {
		fmt.Fprintf(stderr, "Error on %s %s: %v\n", action, pos[0], rErr)
		return 1
	}
	var rec SandboxRecord
	if err := client.CallSandboxEndpoint(ctx, http.MethodPost, "/__session/"+action, sbxID, map[string]string{"sandbox_id": sbxID}, &rec); err != nil {
		fmt.Fprintf(stderr, "Error on %s %s: %v\n", action, pos[0], err)
		return 1
	}
	if jsonOut {
		_ = json.NewEncoder(stdout).Encode(rec)
		return 0
	}
	fmt.Fprintf(stdout, "Sandbox %s is now %s (overlay saved to %s)\n", rec.SandboxID, rec.Status, rec.OverlayPath)
	return 0
}

func cliStartOrResume(ctx context.Context, client *CLIClient, args []string, action string, jsonOut bool, stdout, stderr io.Writer) int {
	if hasHelpFlag(args) {
		fmt.Fprintf(stdout, "Usage: sbx %s <SANDBOX_ID>\n", action)
		return 0
	}
	pos, fl := extractPositionalAndFlags(args)
	if len(fl) > 0 {
		fmt.Fprintf(stderr, "Unknown flag %s\n", fl[0])
		return 2
	}
	if len(pos) == 0 {
		fmt.Fprintf(stderr, "Usage: sbx %s <SANDBOX_ID>\n", action)
		return 1
	}
	sbxID, rErr := client.ResolveUniqueSandboxID(ctx, pos[0])
	if rErr != nil {
		fmt.Fprintf(stderr, "Error on %s %s: %v\n", action, pos[0], rErr)
		return 1
	}
	var rec SandboxRecord
	if err := client.CallSandboxEndpoint(ctx, http.MethodPost, "/__session/"+action, sbxID, map[string]string{"sandbox_id": sbxID}, &rec); err != nil {
		fmt.Fprintf(stderr, "Error on %s %s: %v\n", action, pos[0], err)
		return 1
	}
	if jsonOut {
		_ = json.NewEncoder(stdout).Encode(rec)
		return 0
	}
	fmt.Fprintf(stdout, "Sandbox %s resumed on host %s (status=%s, epoch=%d)\n", rec.SandboxID, rec.HostID, rec.Status, rec.Epoch)
	return 0
}

func cliCopy(ctx context.Context, client *CLIClient, args []string, jsonOut bool, stdout, stderr io.Writer) int {
	if hasHelpFlag(args) {
		fmt.Fprintln(stdout, "Usage: sbx copy <SOURCE_SANDBOX_ID> <TARGET_NAME> [-q|--quiet|--id-only] [--target-host=HOST_ID]")
		return 0
	}
	pos, fl := extractPositionalAndFlags(args)
	if len(pos) < 2 {
		fmt.Fprintln(stderr, "Usage: sbx copy <SOURCE_SANDBOX_ID> <TARGET_NAME> [-q|--quiet|--id-only] [--target-host=HOST_ID]")
		return 1
	}
	fs := flag.NewFlagSet("copy", flag.ContinueOnError)
	fs.SetOutput(stderr)
	targetHost := fs.String("target-host", "", "Optional target host ID for cross-host copy")
	toHost := fs.String("to-host", "", "Alias for --target-host")
	persistent := fs.Bool("persistent", false, "Make cloned sandbox persistent")
	force := fs.Bool("force", false, "Overwrite target if exists")
	quietShort := fs.Bool("q", false, "Print only sandbox_id to stdout")
	quietLong := fs.Bool("quiet", false, "Print only sandbox_id to stdout")
	idOnly := fs.Bool("id-only", false, "Print only sandbox_id to stdout")
	if err := fs.Parse(fl); err != nil {
		return 2
	}
	dstHost := *targetHost
	if dstHost == "" {
		dstHost = *toHost
	}
	srcID, rErr := client.ResolveUniqueSandboxID(ctx, pos[0])
	if rErr != nil {
		fmt.Fprintf(stderr, "Error copying %s -> %s: %v\n", pos[0], pos[1], rErr)
		return 1
	}

	var rec SandboxRecord
	err := client.CallSandboxEndpoint(ctx, http.MethodPost, "/__session/copy", srcID, CopySandboxRequest{
		SourceSandboxID: srcID,
		TargetSandboxID: pos[1],
		Name:            pos[1],
		TargetHostID:    dstHost,
		Persistent:      *persistent,
		Force:           *force,
	}, &rec)
	if err != nil {
		fmt.Fprintf(stderr, "Error copying %s -> %s: %v\n", pos[0], pos[1], err)
		return 1
	}
	client.SetRouteAndName(rec.SandboxID, rec.Name, rec.HostID)
	if *quietShort || *quietLong || *idOnly {
		fmt.Fprintln(stdout, rec.SandboxID)
		return 0
	}
	if jsonOut {
		_ = json.NewEncoder(stdout).Encode(rec)
		return 0
	}
	if rec.Name != "" && rec.Name != rec.SandboxID {
		fmt.Fprintf(stdout, "Copied sandbox %s -> %s (name=%s) on host %s (method=%s)\n",
			srcID, rec.SandboxID, rec.Name, rec.HostID, rec.Metadata["fork_method"])
	} else {
		fmt.Fprintf(stdout, "Copied sandbox %s -> %s on host %s (method=%s)\n",
			srcID, rec.SandboxID, rec.HostID, rec.Metadata["fork_method"])
	}
	return 0
}

func formatMigratedMap(v any) string {
	m, ok := v.(map[string]any)
	if !ok || len(m) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(m))
	for k, dst := range m {
		parts = append(parts, fmt.Sprintf("%s->%v", k, dst))
	}
	return strings.Join(parts, ", ")
}

func cliMigrate(ctx context.Context, client *CLIClient, args []string, jsonOut bool, stdout, stderr io.Writer) int {
	if hasHelpFlag(args) {
		fmt.Fprintln(stdout, "Usage: sbx migrate <SANDBOX_ID> [--to-host=HOST_ID]  or  sbx migrate --host=<HOST_ID>")
		return 0
	}
	pos, fl := extractPositionalAndFlags(args)
	fs := flag.NewFlagSet("migrate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	hostFlag := fs.String("host", "", "Drain all sandboxes from this host ID")
	toHost := fs.String("to-host", "", "Target host ID")
	targetHost := fs.String("target-host", "", "Alias for --to-host")
	if err := fs.Parse(fl); err != nil {
		return 2
	}

	if *hostFlag != "" {
		var res map[string]any
		_, err := client.doJSON(ctx, http.MethodPost, client.ControlPlaneURL+"/v1/hosts/"+url.PathEscape(*hostFlag)+"/drain", nil, nil, &res)
		if err != nil {
			fmt.Fprintf(stderr, "Error draining host %s: %v\n", *hostFlag, err)
			return 1
		}
		if jsonOut {
			_ = json.NewEncoder(stdout).Encode(res)
		} else {
			fmt.Fprintf(stdout, "Drained host %s (migrated: %s)\n", *hostFlag, formatMigratedMap(res["migrated"]))
		}
		return 0
	}
	if len(pos) == 0 {
		fmt.Fprintln(stderr, "Usage: sbx migrate <SANDBOX_ID> [--to-host=HOST_ID]  or  sbx migrate --host=<HOST_ID>")
		return 1
	}
	sbxID, rErr := client.ResolveUniqueSandboxID(ctx, pos[0])
	if rErr != nil {
		fmt.Fprintf(stderr, "Error migrating %s: %v\n", pos[0], rErr)
		return 1
	}
	explicitTarget := *toHost
	if explicitTarget == "" {
		explicitTarget = *targetHost
	}
	if explicitTarget != "" {
		if err := ValidateID(explicitTarget); err != nil {
			fmt.Fprintf(stderr, "Invalid target host ID %q: %v\n", explicitTarget, err)
			return 1
		}
	}

	cachedPrevHost := client.GetRoute(ctx, sbxID)
	var hydrated SandboxRecord
	err := client.CallSandboxEndpoint(ctx, http.MethodPost, "/__session/migrate", sbxID, MigrateSandboxRequest{
		SandboxID:    sbxID,
		TargetHostID: explicitTarget,
		PinnedHost:   explicitTarget != "",
	}, &hydrated)
	if err != nil && client.ControlPlaneURL != "" {
		_, err = client.doJSON(ctx, http.MethodPost, client.ControlPlaneURL+"/v1/sandboxes/"+url.PathEscape(sbxID)+"/migrate", nil, MigrateSandboxRequest{
			SandboxID:    sbxID,
			TargetHostID: explicitTarget,
			PinnedHost:   explicitTarget != "",
		}, &hydrated)
	}
	if err != nil {
		fmt.Fprintf(stderr, "Error migrating %s: %v\n", pos[0], err)
		return 1
	}
	prevHost := hydrated.PreviousHostID
	if prevHost == "" {
		prevHost = cachedPrevHost
	}
	client.SetRouteAndName(hydrated.SandboxID, hydrated.Name, hydrated.HostID)
	if jsonOut {
		_ = json.NewEncoder(stdout).Encode(hydrated)
	} else {
		fmt.Fprintf(stdout, "Migrated sandbox %s from %s -> %s (epoch=%d)\n", hydrated.SandboxID, prevHost, hydrated.HostID, hydrated.Epoch)
	}
	return 0
}

func cliSnapshot(ctx context.Context, client *CLIClient, args []string, jsonOut bool, stdout, stderr io.Writer) int {
	if hasHelpFlag(args) {
		fmt.Fprintln(stdout, "Usage: sbx snapshot [create <SANDBOX_ID> [--name=SNAP_ID] [--force] | list | delete <SNAP_ID>]")
		return 0
	}
	if len(args) == 0 {
		args = []string{"list"}
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "create":
		pos, fl := extractPositionalAndFlags(rest)
		if len(pos) == 0 {
			fmt.Fprintln(stderr, "Usage: sbx snapshot create <SANDBOX_ID> [--name=SNAPSHOT_ID] [--force]")
			return 1
		}
		fs := flag.NewFlagSet("snapshot create", flag.ContinueOnError)
		fs.SetOutput(stderr)
		name := fs.String("name", "", "Snapshot name/ID")
		force := fs.Bool("force", false, "Overwrite existing snapshot if present")
		if err := fs.Parse(fl); err != nil {
			return 2
		}
		sbxID, rErr := client.ResolveUniqueSandboxID(ctx, pos[0])
		if rErr != nil {
			fmt.Fprintf(stderr, "Error creating snapshot: %v\n", rErr)
			return 1
		}
		var snap SnapshotRecord
		err := client.CallSandboxEndpoint(ctx, http.MethodPost, "/__session/snapshot", sbxID, map[string]any{
			"sandbox_id":  sbxID,
			"snapshot_id": *name,
			"force":       *force,
		}, &snap)
		if err != nil {
			fmt.Fprintf(stderr, "Error creating snapshot: %v\n", err)
			return 1
		}
		if jsonOut {
			_ = json.NewEncoder(stdout).Encode(snap)
		} else {
			fmt.Fprintf(stdout, "Created snapshot %s from %s (%d bytes)\n", snap.SnapshotID, snap.SourceSandbox, snap.SizeBytes)
		}
		return 0
	case "list", "ls":
		var res struct {
			Snapshots []SnapshotRecord `json:"snapshots"`
		}
		if _, err := client.doJSON(ctx, http.MethodGet, client.ControlPlaneURL+"/v1/snapshots", nil, nil, &res); err != nil {
			fmt.Fprintf(stderr, "Error listing snapshots: %v\n", err)
			return 1
		}
		if jsonOut {
			_ = json.NewEncoder(stdout).Encode(res)
			return 0
		}
		tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "SNAPSHOT_ID\tSOURCE_SANDBOX\tTEMPLATE\tSIZE_BYTES\tCREATED_AT")
		for _, s := range res.Snapshots {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\n", s.SnapshotID, s.SourceSandbox, s.Template, s.SizeBytes, s.CreatedAt.Format(time.RFC3339))
		}
		_ = tw.Flush()
		return 0
	case "delete", "rm":
		if len(rest) == 0 {
			fmt.Fprintln(stderr, "Usage: sbx snapshot delete <SNAPSHOT_ID>")
			return 1
		}
		var res map[string]any
		if _, err := client.doJSON(ctx, http.MethodDelete, client.ControlPlaneURL+"/v1/snapshots/"+url.PathEscape(rest[0]), nil, nil, &res); err != nil {
			fmt.Fprintf(stderr, "Error deleting snapshot: %v\n", err)
			return 1
		}
		fmt.Fprintf(stdout, "Deleted snapshot %s\n", rest[0])
		return 0
	default:
		fmt.Fprintf(stderr, "Unknown snapshot subcommand %q\n", sub)
		return 1
	}
}

func cliTemplate(ctx context.Context, client *CLIClient, args []string, jsonOut bool, stdout, stderr io.Writer) int {
	if hasHelpFlag(args) {
		fmt.Fprintln(stdout, "Usage: sbx template [list | create <NAME> [--from-snapshot=SNAP_ID | --from-sandbox=ID] [--force] | delete <NAME>]")
		return 0
	}
	if len(args) == 0 {
		args = []string{"list"}
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "list", "ls":
		var res struct {
			Templates []TemplateSpec `json:"templates"`
		}
		if _, err := client.doJSON(ctx, http.MethodGet, client.ControlPlaneURL+"/v1/templates", nil, nil, &res); err != nil {
			fmt.Fprintf(stderr, "Error listing templates: %v\n", err)
			return 1
		}
		if jsonOut {
			_ = json.NewEncoder(stdout).Encode(res)
			return 0
		}
		tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "NAME\tTYPE\tIMAGE\tSNAPSHOT_ID\tDESCRIPTION")
		for _, t := range res.Templates {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", t.Name, t.Type, t.Image, t.SnapshotID, t.Description)
		}
		_ = tw.Flush()
		return 0
	case "create", "add":
		pos, fl := extractPositionalAndFlags(rest)
		if len(pos) == 0 {
			fmt.Fprintln(stderr, "Usage: sbx template create <NAME> [--from-snapshot=SNAP_ID | --from-sandbox=SANDBOX_ID] [--base-template=ubuntu] [--image=IMAGE] [--description=DESC] [--force]")
			return 1
		}
		fs := flag.NewFlagSet("template create", flag.ContinueOnError)
		fs.SetOutput(stderr)
		fromSnap := fs.String("from-snapshot", "", "Backing snapshot ID")
		fromSbx := fs.String("from-sandbox", "", "Create snapshot from this sandbox ID first")
		baseTpl := fs.String("base-template", "ubuntu", "Base Cloud Run Sandbox Runtime template")
		img := fs.String("image", "", "Container image")
		desc := fs.String("description", "", "Template description")
		force := fs.Bool("force", false, "Overwrite existing template if present")
		if err := fs.Parse(fl); err != nil {
			return 2
		}
		tplName := pos[0]
		u := client.ControlPlaneURL + "/v1/templates"
		if *force {
			u += "?force=true"
		}
		var created TemplateSpec
		_, err := client.doJSON(ctx, http.MethodPost, u, nil, TemplateSpec{
			Name:         tplName,
			BaseTemplate: *baseTpl,
			Image:        *img,
			SnapshotID:   *fromSnap,
			FromSandbox:  *fromSbx,
			Description:  *desc,
			Force:        *force,
		}, &created)
		if err != nil {
			fmt.Fprintf(stderr, "Error creating template: %v\n", err)
			return 1
		}
		if jsonOut {
			_ = json.NewEncoder(stdout).Encode(created)
		} else {
			fmt.Fprintf(stdout, "Registered template %s (type=%s, snapshot=%s)\n", created.Name, created.Type, created.SnapshotID)
		}
		return 0
	case "delete", "rm":
		if len(rest) == 0 {
			fmt.Fprintln(stderr, "Usage: sbx template delete <NAME>")
			return 1
		}
		var res map[string]any
		if _, err := client.doJSON(ctx, http.MethodDelete, client.ControlPlaneURL+"/v1/templates/"+url.PathEscape(rest[0]), nil, nil, &res); err != nil {
			fmt.Fprintf(stderr, "Error deleting template: %v\n", err)
			return 1
		}
		fmt.Fprintf(stdout, "Deleted template %s\n", rest[0])
		return 0
	default:
		fmt.Fprintf(stderr, "Unknown template subcommand %q\n", sub)
		return 1
	}
}

func cliConfig(ctx context.Context, client *CLIClient, args []string, _ bool, stdout, stderr io.Writer) int {
	if hasHelpFlag(args) {
		fmt.Fprintln(stdout, "Usage: sbx config [get | set [--max-density=N] [--host-cpu=4] [--host-memory=8Gi] [--default-idle-timeout=SEC|--auto-suspend=SEC] [--min-free-memory-mb=512] [--min-free-memory-pct=15] [--max-memory-overcommit-ratio=2.0] [--hot-memory-threshold-pct=85] [--hot-cpu-threshold-pct=90] [--min-client-version=1.0.0] [--target-revision=REV] [--auto-bin-pack=true|false]]")
		return 0
	}
	if len(args) == 0 || args[0] == "get" {
		var cfg FleetConfig
		if _, err := client.doJSON(ctx, http.MethodGet, client.ControlPlaneURL+"/v1/config", nil, nil, &cfg); err != nil {
			fmt.Fprintf(stderr, "Error reading config: %v\n", err)
			return 1
		}
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(cfg)
		return 0
	}
	if args[0] == "set" {
		fs := flag.NewFlagSet("config set", flag.ContinueOnError)
		fs.SetOutput(stderr)
		maxD := fs.Int("max-density", 0, "Maximum sandboxes per host session")
		hostCPU := fs.String("host-cpu", "", "Cloud Run sandbox-pool host vCPU allocation (e.g. 2, 4, 8)")
		hostMem := fs.String("host-memory", "", "Cloud Run sandbox-pool host memory allocation (e.g. 4Gi, 8Gi, 16Gi, 32Gi)")
		minClientVer := fs.String("min-client-version", "", "Minimum required sbx client version (e.g. 1.0.0, 1.2.0)")
		project := fs.String("project", "", "Optional GCP Project ID when updating Cloud Run sandbox-pool host size")
		region := fs.String("region", "us-west1", "Cloud Run region when updating sandbox-pool host size")
		targetRev := fs.String("target-revision", "__unset__", "Target revision for rollout")
		poolURL := fs.String("pool-url", "", "Sandbox pool URL")
		syncSec := fs.Int("periodic-sync-sec", 0, "Periodic persistence sync seconds")
		hbSec := fs.Int("heartbeat-timeout-sec", 0, "Watchdog heartbeat timeout seconds")
		autoBP := fs.String("auto-bin-pack", "", "Enable automatic bin-packing (true/false)")
		minFreeMB := fs.Int("min-free-memory-mb", -1, "Minimum free host memory in MB before rejecting new creations")
		minFreePct := fs.Float64("min-free-memory-pct", -1, "Minimum free host memory percentage before rejecting new creations")
		maxMemOvercommit := fs.Float64("max-memory-overcommit-ratio", 0, "Maximum memory overcommit ratio per host (e.g. 2.0)")
		hotMemPct := fs.Float64("hot-memory-threshold-pct", 0, "Hot host memory usage % threshold for proactive live migration")
		hotCPUPct := fs.Float64("hot-cpu-threshold-pct", 0, "Hot host CPU usage % threshold for proactive live migration")
		defaultIdle := fs.String("default-idle-timeout", "", "Fleet-wide default idle timeout in seconds or duration (0=disabled)")
		autoSuspend := fs.String("auto-suspend", "", "Alias for --default-idle-timeout in seconds or duration (0=disabled)")
		if err := fs.Parse(args[1:]); err != nil {
			return 2
		}

		patch := map[string]any{}
		if strings.TrimSpace(*autoSuspend) != "" {
			v, pErr := parseDurationSecondsFlag(*autoSuspend)
			if pErr != nil {
				fmt.Fprintf(stderr, "Error parsing --auto-suspend: %v\n", pErr)
				return 2
			}
			patch["default_idle_timeout_sec"] = v
		}
		if strings.TrimSpace(*defaultIdle) != "" {
			v, pErr := parseDurationSecondsFlag(*defaultIdle)
			if pErr != nil {
				fmt.Fprintf(stderr, "Error parsing --default-idle-timeout: %v\n", pErr)
				return 2
			}
			patch["default_idle_timeout_sec"] = v
		}
		if *maxD > 0 {
			patch["max_density"] = *maxD
		}
		if *hostCPU != "" {
			patch["host_cpu"] = *hostCPU
		}
		if *hostMem != "" {
			patch["host_memory"] = *hostMem
		}
		if *minClientVer != "" {
			patch["min_client_version"] = *minClientVer
		}
		if *project != "" {
			patch["project"] = *project
		}
		if *region != "" {
			patch["region"] = *region
		}
		if *targetRev != "__unset__" {
			patch["target_revision"] = *targetRev
		}
		if *poolURL != "" {
			patch["pool_service_url"] = *poolURL
		}
		if *syncSec > 0 {
			patch["periodic_sync_sec"] = *syncSec
		}
		if *hbSec > 0 {
			patch["heartbeat_timeout_sec"] = *hbSec
		}
		if *autoBP != "" {
			bVal := strings.EqualFold(*autoBP, "true") || *autoBP == "1"
			patch["auto_bin_pack"] = bVal
		}
		if *minFreeMB >= 0 {
			patch["min_free_memory_mb"] = *minFreeMB
		}
		if *minFreePct >= 0 {
			patch["min_free_memory_pct"] = *minFreePct
		}
		if *maxMemOvercommit > 0 {
			patch["max_memory_overcommit_ratio"] = *maxMemOvercommit
		}
		if *hotMemPct > 0 {
			patch["hot_memory_threshold_pct"] = *hotMemPct
		}
		if *hotCPUPct > 0 {
			patch["hot_cpu_threshold_pct"] = *hotCPUPct
		}

		var updated FleetConfig
		if _, err := client.doJSON(ctx, http.MethodPost, client.ControlPlaneURL+"/v1/config", nil, patch, &updated); err != nil {
			fmt.Fprintf(stderr, "Error updating config: %v\n", err)
			return 1
		}
		// Refresh local candidates cache after config change
		_, _ = client.RefreshCandidates(ctx)
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(updated)
		return 0
	}
	fmt.Fprintln(stderr, "Usage: sbx config [get | set [--max-density=N] [--host-cpu=4] [--host-memory=8Gi] [--default-idle-timeout=SEC|--auto-suspend=SEC] [--min-client-version=1.0.0] [--target-revision=REV] [--auto-bin-pack=true|false]]")
	return 1
}

func cliHosts(ctx context.Context, client *CLIClient, args []string, jsonOut bool, stdout, stderr io.Writer) int {
	if hasHelpFlag(args) {
		fmt.Fprintln(stdout, "Usage: sbx hosts [list | drain <HOST_ID> | simulate-crash <HOST_ID> | compact]")
		return 0
	}
	if len(args) == 0 {
		args = []string{"list"}
	}
	switch args[0] {
	case "list", "ls":
		var res struct {
			Hosts []HostRecord `json:"hosts"`
		}
		if _, err := client.doJSON(ctx, http.MethodGet, client.ControlPlaneURL+"/v1/hosts", nil, nil, &res); err != nil {
			fmt.Fprintf(stderr, "Error listing hosts: %v\n", err)
			return 1
		}
		if jsonOut {
			_ = json.NewEncoder(stdout).Encode(res)
			return 0
		}
		tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "HOST_ID\tSTATUS\tACTIVE\tFREE\tMAX\tCPU/RAM\tREVISION\tINSTANCE_STATE\tLAST_HEARTBEAT")
		for _, h := range res.Hosts {
			hCPU := h.HostCPU
			if hCPU == "" {
				hCPU = "4"
			}
			hMem := h.HostMemory
			if hMem == "" {
				hMem = "8Gi"
			}
			fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%d\t%s/%s\t%s\t%s\t%s\n",
				h.HostID, h.Status, h.ActiveSandboxes, h.FreeSlots, h.MaxDensity, hCPU, hMem, h.Revision, h.InstanceState, h.LastHeartbeatAt.Format("15:04:05"))
		}
		_ = tw.Flush()
		return 0
	case "drain":
		if len(args) < 2 {
			fmt.Fprintln(stderr, "Usage: sbx hosts drain <HOST_ID>")
			return 1
		}
		var res map[string]any
		if _, err := client.doJSON(ctx, http.MethodPost, client.ControlPlaneURL+"/v1/hosts/"+url.PathEscape(args[1])+"/drain", nil, nil, &res); err != nil {
			fmt.Fprintf(stderr, "Error draining host %s: %v\n", args[1], err)
			return 1
		}
		if jsonOut {
			_ = json.NewEncoder(stdout).Encode(res)
		} else {
			fmt.Fprintf(stdout, "Drained host %s (migrated: %s)\n", args[1], formatMigratedMap(res["migrated"]))
		}
		return 0
	case "simulate-crash":
		if len(args) < 2 {
			fmt.Fprintln(stderr, "Usage: sbx hosts simulate-crash <HOST_ID>")
			return 1
		}
		var res map[string]any
		if _, err := client.doJSON(ctx, http.MethodPost, client.ControlPlaneURL+"/v1/hosts/"+url.PathEscape(args[1])+"/simulate-crash", nil, nil, &res); err != nil {
			fmt.Fprintf(stderr, "Error simulating crash on %s: %v\n", args[1], err)
			return 1
		}
		if jsonOut {
			_ = json.NewEncoder(stdout).Encode(res)
		} else {
			fmt.Fprintf(stdout, "Simulated crash on host %s and triggered watchdog recovery\n", args[1])
		}
		return 0
	case "compact":
		var res map[string]any
		if _, err := client.doJSON(ctx, http.MethodPost, client.ControlPlaneURL+"/v1/compact", nil, nil, &res); err != nil {
			fmt.Fprintf(stderr, "Error compacting fleet: %v\n", err)
			return 1
		}
		if jsonOut {
			_ = json.NewEncoder(stdout).Encode(res)
		} else {
			fmt.Fprintf(stdout, "Compaction result: drained=%v migrated=%s (%v)\n", res["drained_hosts"], formatMigratedMap(res["migrated_sandboxes"]), res["message"])
		}
		return 0
	default:
		fmt.Fprintf(stderr, "Unknown hosts subcommand %q\n", args[0])
		return 1
	}
}

func cliStats(ctx context.Context, client *CLIClient, jsonOut bool, stdout, stderr io.Writer) int {
	var sum FleetSummary
	if _, err := client.doJSON(ctx, http.MethodGet, client.ControlPlaneURL+"/v1/stats", nil, nil, &sum); err != nil {
		fmt.Fprintf(stderr, "Error fetching stats: %v\n", err)
		return 1
	}
	if jsonOut {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(sum)
		return 0
	}
	fmt.Fprintf(stdout, "Fleet Summary (Leader: %s)\n", sum.LeaderID)
	fmt.Fprintf(stdout, "  Hosts:             %d active / %d draining / %d dead (total=%d)\n", sum.ActiveHosts, sum.DrainingHosts, sum.DeadHosts, sum.TotalHosts)
	fmt.Fprintf(stdout, "  Sandboxes:         %d running / %d suspended (total=%d)\n", sum.RunningSandboxes, sum.SuspendedSandboxes, sum.TotalSandboxes)
	fmt.Fprintf(stdout, "  Capacity Slots:    %d / %d used (%.1f%% bin-pack efficiency, max_density=%d)\n",
		sum.UsedCapacitySlots, sum.TotalCapacitySlots, sum.BinPackEfficiencyPct, sum.Config.MaxDensity)
	fmt.Fprintf(stdout, "  Live Migrations:   %d (Hot-Host Shedding: %d, Memory Rejections: %d)\n", sum.TotalMigrations, sum.TotalHotMigrations, sum.TotalMemoryRejections)
	fmt.Fprintf(stdout, "  Overcommit Ratio:  Memory %.2fx / CPU %.2fx\n", sum.FleetMemoryOvercommitRatio, sum.FleetCPUOvercommitRatio)
	fmt.Fprintf(stdout, "  Crash Recoveries:  %d\n", sum.TotalCrashRecoveries)
	fmt.Fprintf(stdout, "  Fleet Uptime SLA:  %.3f%%\n", sum.FleetUptimeSLAPct)
	return 0
}

func cliMetrics(ctx context.Context, client *CLIClient, args []string, jsonOut bool, stdout, stderr io.Writer) int {
	if hasHelpFlag(args) {
		fmt.Fprintln(stdout, "Usage: sbx metrics [--host=HOST_ID] [--sandbox=SANDBOX_ID] [--history]")
		return 0
	}
	fs := flag.NewFlagSet("metrics", flag.ContinueOnError)
	fs.SetOutput(stderr)
	hostFlag := fs.String("host", "", "Filter metrics by host ID")
	sbxFlag := fs.String("sandbox", "", "Filter metrics by sandbox ID or name")
	historyFlag := fs.Bool("history", false, "Include time-series resource history per host")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	q := url.Values{}
	if *hostFlag != "" {
		q.Set("host", *hostFlag)
	}
	if *sbxFlag != "" {
		q.Set("sandbox", *sbxFlag)
	}
	if *historyFlag {
		q.Set("history", "true")
	}
	u := client.ControlPlaneURL + "/v1/metrics"
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	var res map[string]any
	if _, err := client.doJSON(ctx, http.MethodGet, u, nil, nil, &res); err != nil {
		fmt.Fprintf(stderr, "Error fetching metrics: %v\n", err)
		return 1
	}
	if jsonOut {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(res)
		return 0
	}
	fmt.Fprintf(stdout, "Fleet Resource & Overcommit Metrics\n")
	fmt.Fprintf(stdout, "  Fleet Memory Overcommit Ratio: %v\n", res["fleet_memory_overcommit_ratio"])
	fmt.Fprintf(stdout, "  Fleet CPU Overcommit Ratio:    %v\n", res["fleet_cpu_overcommit_ratio"])
	fmt.Fprintf(stdout, "  Hot-Host Migrations Triggered: %v\n", res["total_hot_migrations"])
	fmt.Fprintf(stdout, "  Memory Admission Rejections:   %v\n", res["total_memory_rejections"])
	if hostsMap, ok := res["hosts"].(map[string]any); ok && len(hostsMap) > 0 {
		fmt.Fprintf(stdout, "\nHost Resource Breakdown:\n")
		for hid, rawH := range hostsMap {
			if hm, ok := rawH.(map[string]any); ok {
				fmt.Fprintf(stdout, "  - %s: state=%v used_mem_pct=%v%% mem_overcommit=%vx cpu_overcommit=%vx cpu_usage=%v%%\n",
					hid, hm["overload_state"], hm["used_memory_pct"], hm["memory_overcommit_ratio"], hm["cpu_overcommit_ratio"], hm["cpu_usage_pct"])
			}
		}
	}
	if recs, ok := res["recommendations"].([]any); ok && len(recs) > 0 {
		fmt.Fprintf(stdout, "\nSizing & Overcommit Recommendations:\n")
		for _, r := range recs {
			fmt.Fprintf(stdout, "  - %v\n", r)
		}
	}
	return 0
}

func cliDeploy(ctx context.Context, client *CLIClient, args []string, stdout, stderr io.Writer) int {
	if hasHelpFlag(args) {
		fmt.Fprintln(stdout, "Usage: sbx deploy [--project=PROJECT] [--region=REGION] [--bucket=BUCKET] [--host-cpu=4] [--host-memory=8Gi]")
		return 0
	}
	fs := flag.NewFlagSet("deploy", flag.ContinueOnError)
	fs.SetOutput(stderr)
	project := fs.String("project", DefaultGCPProject(), "GCP Project ID")
	region := fs.String("region", "us-west1", "Cloud Run Region")
	bucket := fs.String("bucket", "", "GCS Bucket Name (defaults to <project>-sandbox-dispenser)")
	hostCPU := fs.String("host-cpu", "4", "Cloud Run sandbox-pool host vCPU allocation (default 4)")
	hostMem := fs.String("host-memory", "8Gi", "Cloud Run sandbox-pool host memory allocation (default 8Gi)")
	maxDensity := fs.Int("max-density", 10, "Maximum sandboxes per host session (default 10)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	resolvedProj := resolveCLIProject(ctx, *project)
	if resolvedProj != "" {
		*project = resolvedProj
	}
	if *project == "" || *project == "my-gcp-project" {
		if client.ControlPlaneURL == "" {
			fmt.Fprintln(stderr, "Error: GCP project ID is required (pass --project=<GCP_PROJECT> or set GOOGLE_CLOUD_PROJECT)")
			return 1
		}
	}
	if *bucket == "" {
		*bucket = *project + "-sandbox-dispenser"
	}

	// Run deploy.sh from the current directory if present, or extract the embedded
	// deployment bundle when invoked from outside the repo (e.g. after `go install`).
	scriptPath := "./deploy.sh"
	isLoopbackControlPlane := strings.HasPrefix(client.ControlPlaneURL, "http://127.0.0.1") || strings.HasPrefix(client.ControlPlaneURL, "http://localhost")
	if _, err := os.Stat(scriptPath); err != nil && !isLoopbackControlPlane {
		tmpDir, extractErr := extractEmbeddedDeployAssets()
		if extractErr != nil {
			fmt.Fprintf(stderr, "failed to extract embedded deployment assets: %v\n", extractErr)
			return 1
		}
		defer os.RemoveAll(tmpDir)
		scriptPath = filepath.Join(tmpDir, "deploy.sh")
	}

	if _, err := os.Stat(scriptPath); err == nil {
		cmd := exec.CommandContext(ctx, scriptPath, *project, *region, *bucket)
		cmd.Env = append(os.Environ(), "HOST_CPU="+*hostCPU, "HOST_MEMORY="+*hostMem)
		cmd.Stdout = stdout
		cmd.Stderr = stderr
		if err := cmd.Run(); err != nil {
			fmt.Fprintf(stderr, "deploy failed: %v\n", err)
			return 1
		}
		_ = cliInit(ctx, client, []string{"--project=" + *project, "--region=" + *region}, false, stdout, stderr)
	} else if client.ControlPlaneURL == "" {
		fmt.Fprintf(stderr, "deploy.sh not found in current directory and no ControlPlaneURL configured; run from the dispenser package directory or run 'sbx init' first.\n")
		return 1
	}

	if client.ControlPlaneURL != "" {
		var depResp DeployFleetResponse
		if _, err := client.doJSON(ctx, http.MethodPost, client.ControlPlaneURL+"/v1/deploy", nil, DeployFleetRequest{
			Project:    *project,
			Region:     *region,
			Bucket:     *bucket,
			HostCPU:    *hostCPU,
			HostMemory: *hostMem,
			MaxDensity: *maxDensity,
		}, &depResp); err != nil {
			fmt.Fprintf(stderr, "server-side fleet deploy reconciliation failed: %v\n", err)
			return 1
		}
		if depResp.PoolServiceURL != "" {
			client.SaveEndpoints(client.ControlPlaneURL, depResp.PoolServiceURL)
		}
	}
	return 0
}
