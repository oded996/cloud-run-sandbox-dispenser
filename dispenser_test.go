package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// sessionPoolRouter simulates Cloud Run's `run.googleapis.com/scalingMode: session`
// router in hermetic unit tests: it routes requests by `X-Serverless-Session-Id` to existing
// HostServers, and dynamically spawns new HostServer clones when a CLI or control plane
// calls `sandbox-pool` with a newly minted `host-<id>` session header!
type sessionPoolRouter struct {
	mu              sync.Mutex
	t               *testing.T
	store           Store
	maxDensity      int
	controlPlaneURL string
	poolURL         string
	revision        string
	hosts           map[string]*HostServer
}

func newTestFleet(t *testing.T, maxDensity int) (*sessionPoolRouter, *ControlPlaneServer, *httptest.Server, *httptest.Server, *MemoryStore) {
	t.Helper()
	blobDir := t.TempDir()
	store := NewMemoryStore(blobDir)

	router := &sessionPoolRouter{
		t:          t,
		store:      store,
		maxDensity: maxDensity,
		revision:   "sandbox-pool-00001-v1",
		hosts:      make(map[string]*HostServer),
	}
	poolTS := httptest.NewServer(router)
	t.Cleanup(poolTS.Close)

	cfg := DefaultFleetConfig()
	cfg.MaxDensity = maxDensity
	cfg.PoolServiceURL = poolTS.URL
	cfg.BinPackHysteresis = 0

	cp := NewControlPlaneServer(store, cfg)
	cpTS := httptest.NewServer(cp)
	t.Cleanup(func() {
		cp.Close()
		cpTS.Close()
	})

	router.controlPlaneURL = cpTS.URL
	router.poolURL = poolTS.URL
	return router, cp, poolTS, cpTS, store
}

func (r *sessionPoolRouter) getOrCreateHost(hostID string) *HostServer {
	r.mu.Lock()
	defer r.mu.Unlock()
	if hostID == "" {
		hostID = NewRandomID("host")
	}
	if h, ok := r.hosts[hostID]; ok {
		return h
	}
	engRoot := filepath.Join(r.t.TempDir(), hostID)
	eng := NewLocalProcessEngine(engRoot)
	h := NewHostServer(hostID, r.store, eng, r.maxDensity, r.controlPlaneURL, r.poolURL)
	h.revision = r.revision
	r.hosts[hostID] = h
	return h
}

func (r *sessionPoolRouter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	hostID := strings.TrimSpace(req.Header.Get("X-Serverless-Session-Id"))
	h := r.getOrCreateHost(hostID)
	h.ServeHTTP(w, req)
}

func TestValidateIDAndSafeResolvePath(t *testing.T) {
	validIDs := []string{"sbx-1", "host-abc123", "python", "snap_01.v2", "a"}
	for _, id := range validIDs {
		if err := ValidateID(id); err != nil {
			t.Errorf("ValidateID(%q) unexpected error: %v", id, err)
		}
	}

	invalidIDs := []string{"", "../etc/passwd", "sbx..1", "-bad", "bad/slash", "bad;cmd", "bad space", strings.Repeat("a", 70)}
	for _, id := range invalidIDs {
		if err := ValidateID(id); !errors.Is(err, ErrInvalidID) {
			t.Errorf("ValidateID(%q) = %v, want ErrInvalidID", id, err)
		}
	}

	root := "/tmp/sandbox-root"
	if got, err := SafeResolvePath(root, "workspace/hello.txt"); err != nil || got != "/tmp/sandbox-root/workspace/hello.txt" {
		t.Errorf("SafeResolvePath valid failed: got=%q, err=%v", got, err)
	}
	// Verify partial prefix bypass attempt fails if someone passes a trick
	if err := ValidateID("../outside"); err == nil {
		t.Errorf("expected error for ../outside")
	}
}

func TestMemoryStoreAndLocalGCSStoreCAS(t *testing.T) {
	ctx := context.Background()
	stores := []struct {
		name  string
		store Store
	}{
		{"MemoryStore", NewMemoryStore(t.TempDir())},
		{"LocalGCSStore", NewGCSStore("", t.TempDir())},
	}

	for _, tc := range stores {
		t.Run(tc.name, func(t *testing.T) {
			s := tc.store
			rec := SandboxRecord{SandboxID: "sbx-cas", HostID: "host-1", Epoch: 1, Status: StatusRunning}

			// Create-only (expectedGen == 0) must succeed first time
			gen1, err := s.WriteJSONCAS(ctx, "sandboxes/sbx-cas.json", rec, 0)
			if err != nil || gen1 <= 0 {
				t.Fatalf("first WriteJSONCAS(gen=0) failed: gen=%d, err=%v", gen1, err)
			}

			// Second create-only must fail with ErrPreconditionFailed
			if _, err := s.WriteJSONCAS(ctx, "sandboxes/sbx-cas.json", rec, 0); !errors.Is(err, ErrPreconditionFailed) {
				t.Fatalf("duplicate WriteJSONCAS(gen=0) = %v, want ErrPreconditionFailed", err)
			}

			// Update with wrong generation must fail
			if _, err := s.WriteJSONCAS(ctx, "sandboxes/sbx-cas.json", rec, gen1+99); !errors.Is(err, ErrPreconditionFailed) {
				t.Fatalf("WriteJSONCAS(wrong gen) = %v, want ErrPreconditionFailed", err)
			}

			// Update with matching gen1 must succeed
			rec.HostID = "host-2"
			rec.Epoch = 2
			gen2, err := s.WriteJSONCAS(ctx, "sandboxes/sbx-cas.json", rec, gen1)
			if err != nil || gen2 <= gen1 {
				t.Fatalf("WriteJSONCAS(gen1) failed: gen2=%d, err=%v", gen2, err)
			}

			var loaded SandboxRecord
			readGen, exists, err := s.ReadJSON(ctx, "sandboxes/sbx-cas.json", &loaded)
			if err != nil || !exists || readGen != gen2 || loaded.HostID != "host-2" || loaded.Epoch != 2 {
				t.Fatalf("ReadJSON mismatch: exists=%v, gen=%d, loaded=%+v, err=%v", exists, readGen, loaded, err)
			}
		})
	}
}

func TestDecentralizedCLICreationAnd429AutonomousOverflow(t *testing.T) {
	ctx := context.Background()
	// Set maxDensity = 2 sandboxes per host session
	router, cp, poolTS, cpTS, _ := newTestFleet(t, 2)

	// Pre-warm host-01
	h1 := router.getOrCreateHost("host-01")
	_ = h1.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)

	cli := NewCLIClient(cpTS.URL, poolTS.URL, t.TempDir())

	// Create 5 sandboxes: first 2 should pack onto host-01, 3rd should get 429 on host-01 (or see 0 free slots)
	// and autonomously spawn new host session(s)!
	hostCounts := make(map[string]int)
	for i := 1; i <= 5; i++ {
		id := "sbx-burst-" + string(rune('0'+i))
		rec, err := cli.CreateSandbox(ctx, CreateSandboxRequest{
			SandboxID: id,
			Template:  "python",
		})
		if err != nil {
			t.Fatalf("CreateSandbox(%s) failed: %v", id, err)
		}
		if rec.Status != StatusRunning {
			t.Fatalf("sandbox %s status = %s, want RUNNING", id, rec.Status)
		}
		hostCounts[rec.HostID]++
	}

	if hostCounts["host-01"] != 2 {
		t.Fatalf("host-01 count = %d, want 2 (maxDensity=2)", hostCounts["host-01"])
	}
	if len(hostCounts) < 3 {
		t.Fatalf("expected at least 3 distinct hosts for 5 sandboxes at maxDensity=2, got %v", hostCounts)
	}
	for hid, cnt := range hostCounts {
		if cnt > 2 {
			t.Fatalf("host %s exceeded maxDensity=2 with %d sandboxes", hid, cnt)
		}
	}
}

func TestLiveMigrationDrainAnd409SelfHealingRoute(t *testing.T) {
	ctx := context.Background()
	router, cp, poolTS, cpTS, _ := newTestFleet(t, 5)

	h1 := router.getOrCreateHost("host-source")
	h2 := router.getOrCreateHost("host-target")
	_ = h1.SyncStateAndHeartbeat(ctx)
	_ = h2.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)

	cliA := NewCLIClient(cpTS.URL, poolTS.URL, t.TempDir())
	cliB := NewCLIClient(cpTS.URL, poolTS.URL, t.TempDir())

	// Create persistent sandbox on host-source
	rec, err := cliA.CreateSandbox(ctx, CreateSandboxRequest{
		Name:       "sbx-mig-1",
		Template:   "ubuntu",
		Persistent: true,
	})
	if err != nil {
		t.Fatalf("CreateSandbox failed: %v", err)
	}
	// Prime cliB's route cache with the old host ID
	cliB.SetRoute(rec.SandboxID, rec.HostID)

	// Write stateful data inside the sandbox
	execResp, err := cliA.Exec(ctx, ExecRequest{
		SandboxID: rec.SandboxID,
		Command:   []string{"sh", "-c", "echo 'live-migration-state-ok' > /workspace/state.txt"},
	})
	if err != nil || execResp.ExitCode != 0 {
		t.Fatalf("write state failed: resp=%+v, err=%v", execResp, err)
	}

	// Trigger live drain of rec.HostID (simulating SIGTERM or rollout drain)
	migrated, err := cp.TriggerHostDrain(ctx, rec.HostID, "sigterm_test")
	if err != nil {
		t.Fatalf("TriggerHostDrain failed: %v", err)
	}
	newHost := migrated[rec.SandboxID]
	if newHost == "" || newHost == rec.HostID {
		t.Fatalf("expected %s to migrate to a different host, got %q (from %q)", rec.SandboxID, newHost, rec.HostID)
	}

	// Now cliB still has the stale route (`rec.HostID`) cached!
	// Calling Exec via cliB must receive HTTP 409 Conflict from `rec.HostID` with `X-Sandbox-Host-Session: <newHost>`,
	// self-heal `routes.json`, and transparently execute on `newHost`!
	readResp, err := cliB.Exec(ctx, ExecRequest{
		SandboxID: rec.SandboxID,
		Command:   []string{"sh", "-c", "cat /workspace/state.txt"},
	})
	if err != nil {
		t.Fatalf("cliB.Exec after migration failed: %v", err)
	}
	if strings.TrimSpace(readResp.Stdout) != "live-migration-state-ok" {
		t.Fatalf("Stdout after migration = %q, want 'live-migration-state-ok'", readResp.Stdout)
	}
	if readResp.HostID != newHost {
		t.Fatalf("Exec served by %s, want %s", readResp.HostID, newHost)
	}
	if gotRoute := cliB.GetRoute(ctx, rec.SandboxID); gotRoute != newHost {
		t.Fatalf("cliB route cache was not updated: got %s, want %s", gotRoute, newHost)
	}
}

func TestHardCrashWatchdogResurrectionAndPostgresPersistence(t *testing.T) {
	ctx := context.Background()
	router, cp, poolTS, cpTS, _ := newTestFleet(t, 5)

	hCrash := router.getOrCreateHost("host-doomed")
	hHealthy := router.getOrCreateHost("host-healthy")
	_ = hCrash.SyncStateAndHeartbeat(ctx)
	_ = hHealthy.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)

	cli := NewCLIClient(cpTS.URL, poolTS.URL, t.TempDir())
	// Force creation onto host-doomed by making it fuller (Best-Fit) or creating directly
	rec, err := cli.CreateSandbox(ctx, CreateSandboxRequest{
		SandboxID:  "sbx-pg-1",
		Template:   "postgres",
		Persistent: true,
	})
	if err != nil {
		t.Fatalf("CreateSandbox(sbx-pg-1) failed: %v", err)
	}
	initialHost := rec.HostID

	// Write a simulated Postgres transaction log into /pgdata/data/tx.log
	if _, err := cli.Exec(ctx, ExecRequest{
		SandboxID: "sbx-pg-1",
		Command:   []string{"sh", "-c", "echo 'COMMIT_TX_42' > /pgdata/data/tx.log"},
	}); err != nil {
		t.Fatalf("write pgdata failed: %v", err)
	}

	// Simulate sudden hard clone death (`SIGKILL`) on `initialHost` via control plane
	var crashOut, crashErr bytes.Buffer
	if code := RunCLI(ctx, []string{"--control-plane-url=" + cpTS.URL, "--pool-url=" + poolTS.URL, "hosts", "simulate-crash", initialHost}, &crashOut, &crashErr); code != 0 {
		t.Fatalf("simulate-crash CLI failed (code=%d): %s", code, crashErr.String())
	}

	// Verify Watchdog resurrected `sbx-pg-1` onto another host and incremented CrashRecoveries
	res, err := cli.Exec(ctx, ExecRequest{
		SandboxID: "sbx-pg-1",
		Command:   []string{"sh", "-c", "cat /pgdata/data/tx.log"},
	})
	if err != nil {
		t.Fatalf("Exec after crash recovery failed: %v", err)
	}
	if strings.TrimSpace(res.Stdout) != "COMMIT_TX_42" {
		t.Fatalf("pgdata after crash recovery = %q, want 'COMMIT_TX_42'", res.Stdout)
	}
	if res.HostID == initialHost {
		t.Fatalf("expected sbx-pg-1 to be resurrected onto a different host than %s", initialHost)
	}

	sum := cp.BuildFleetSummary()
	if sum.TotalCrashRecoveries < 1 {
		t.Fatalf("expected TotalCrashRecoveries >= 1, got %d", sum.TotalCrashRecoveries)
	}
}

func TestBinPackingCompaction(t *testing.T) {
	ctx := context.Background()
	router, cp, poolTS, cpTS, _ := newTestFleet(t, 4)

	hA := router.getOrCreateHost("host-a")
	hB := router.getOrCreateHost("host-b")
	_ = hA.SyncStateAndHeartbeat(ctx)
	_ = hB.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)

	cli := NewCLIClient(cpTS.URL, poolTS.URL, t.TempDir())

	// Create 1 sandbox specifically on host-a and 2 sandboxes on host-b
	var sbx1 SandboxRecord
	if _, err := cli.doJSON(ctx, http.MethodPost, poolTS.URL+"/__session/create",
		map[string]string{"X-Serverless-Session-Id": "host-a"},
		CreateSandboxRequest{SandboxID: "sbx-sparse-1", Template: "ubuntu"}, &sbx1); err != nil {
		t.Fatalf("create on host-a failed: %v", err)
	}
	for _, id := range []string{"sbx-dense-1", "sbx-dense-2"} {
		var r SandboxRecord
		if _, err := cli.doJSON(ctx, http.MethodPost, poolTS.URL+"/__session/create",
			map[string]string{"X-Serverless-Session-Id": "host-b"},
			CreateSandboxRequest{SandboxID: id, Template: "ubuntu"}, &r); err != nil {
			t.Fatalf("create %s on host-b failed: %v", id, err)
		}
	}
	_ = hA.SyncStateAndHeartbeat(ctx)
	_ = hB.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)

	// Now host-a has 1/4 sandboxes and host-b has 2/4 sandboxes (total 3 sandboxes <= maxDensity 4).
	// Running `CompactFleet` must drain the sparser host (`host-a`) onto `host-b`!
	compactRes, err := cp.CompactFleet(ctx, true)
	if err != nil {
		t.Fatalf("CompactFleet failed: %v", err)
	}
	drained, _ := compactRes["drained_hosts"].([]string)
	if len(drained) != 1 || drained[0] != "host-a" {
		t.Fatalf("expected host-a to be drained by bin-packer, got %+v", compactRes)
	}

	if hA.HostState().ActiveSandboxes != 0 || hA.HostState().InstanceState != "idle" {
		t.Fatalf("expected host-a to have 0 sandboxes and instance_state=idle, got %+v", hA.HostState())
	}
	if hB.HostState().ActiveSandboxes != 3 {
		t.Fatalf("expected host-b to have all 3 consolidated sandboxes, got %d", hB.HostState().ActiveSandboxes)
	}
}

func TestSuspendResumeSnapshotsTemplatesAndCopy(t *testing.T) {
	ctx := context.Background()
	_, _, poolTS, cpTS, _ := newTestFleet(t, 5)
	cacheDir := t.TempDir()

	run := func(args ...string) string {
		var out, errBuf bytes.Buffer
		fullArgs := append([]string{"--control-plane-url=" + cpTS.URL, "--pool-url=" + poolTS.URL, "--cache-dir=" + cacheDir}, args...)
		code := RunCLI(ctx, fullArgs, &out, &errBuf)
		if code != 0 {
			t.Fatalf("RunCLI(%v) exited with %d: stderr=%s stdout=%s", args, code, errBuf.String(), out.String())
		}
		return out.String()
	}

	// 1. Create sandbox
	run("create", "sbx-life", "--template=python", "--persistent")

	// 2. Upload file via `sbx cp`
	localSrc := filepath.Join(t.TempDir(), "greeting.txt")
	_ = os.WriteFile(localSrc, []byte("hello-from-cp"), 0644)
	run("cp", localSrc, "sbx-life:/workspace/greeting.txt")

	// 3. Suspend sandbox (releases slot) and verify auto-resume on `exec`!
	run("suspend", "sbx-life")
	got := run("exec", "sbx-life", "--", "sh", "-c", "cat /workspace/greeting.txt")
	if strings.TrimSpace(got) != "hello-from-cp" {
		t.Fatalf("after suspend + auto-resume on exec, got %q, want 'hello-from-cp'", got)
	}

	// 4. Create snapshot and register custom template from snapshot
	run("snapshot", "create", "sbx-life", "--name=snap-gold")
	run("template", "create", "gold-tpl", "--from-snapshot=snap-gold", "--description=Golden_Template")

	// 5. Create new sandbox from custom template `gold-tpl` and verify file is pre-populated
	run("create", "sbx-from-tpl", "--template=gold-tpl")
	gotTpl := run("exec", "sbx-from-tpl", "--", "sh", "-c", "cat /workspace/greeting.txt")
	if strings.TrimSpace(gotTpl) != "hello-from-cp" {
		t.Fatalf("sandbox created from custom template got %q, want 'hello-from-cp'", gotTpl)
	}

	// 6. Same-host CoW copy (`sbx copy`)
	run("copy", "sbx-life", "sbx-cloned")
	gotClone := run("exec", "sbx-cloned", "--", "sh", "-c", "cat /workspace/greeting.txt")
	if strings.TrimSpace(gotClone) != "hello-from-cp" {
		t.Fatalf("cloned sandbox got %q, want 'hello-from-cp'", gotClone)
	}
}

func TestHostSessionBindingAndNoPhantomHeartbeats(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore(t.TempDir())
	eng := NewLocalProcessEngine(t.TempDir())
	h := NewHostServer("", store, eng, 5, "", "")

	// While unbound (hostID == ""), SyncStateAndHeartbeat must be a no-op
	if err := h.SyncStateAndHeartbeat(ctx); err != nil {
		t.Fatalf("SyncStateAndHeartbeat on unbound host failed: %v", err)
	}
	keys, _ := store.ListObjects(ctx, "hosts/")
	if len(keys) != 0 {
		t.Fatalf("unbound host wrote phantom host records: %v", keys)
	}

	// First request with X-Serverless-Session-Id binds the host once and only once
	req1 := httptest.NewRequest(http.MethodGet, "/__internal/ping", nil)
	req1.Header.Set("X-Serverless-Session-Id", "host-bound-1")
	w1 := httptest.NewRecorder()
	h.ServeHTTP(w1, req1)
	if h.HostState().HostID != "host-bound-1" {
		t.Fatalf("hostID after first session request = %q, want 'host-bound-1'", h.HostState().HostID)
	}

	// Second request with a different header must NOT rename the bound host
	req2 := httptest.NewRequest(http.MethodGet, "/__internal/ping", nil)
	req2.Header.Set("X-Serverless-Session-Id", "host-other-2")
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, req2)
	if h.HostState().HostID != "host-bound-1" {
		t.Fatalf("bound host was renamed to %q, want 'host-bound-1'", h.HostState().HostID)
	}
}

func TestDistributedSandboxIDDuplicateNamesAndZeroLookupRouting(t *testing.T) {
	ctx := context.Background()
	router, cp, poolTS, cpTS, _ := newTestFleet(t, 5)
	h1 := router.getOrCreateHost("host-dup-1")
	h2 := router.getOrCreateHost("host-dup-2")
	_ = h1.SyncStateAndHeartbeat(ctx)
	_ = h2.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)

	cli := NewCLIClient(cpTS.URL, poolTS.URL, t.TempDir())

	// 1. Concurrent creates with the same human-readable Name across hosts must ALL succeed
	// with distinct, host-generated distributed SandboxIDs (`sbx-<host>-<hex>`)!
	const workers = 6
	var wg sync.WaitGroup
	recs := make([]SandboxRecord, workers)
	errs := make([]error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			wcli := NewCLIClient(cpTS.URL, poolTS.URL, t.TempDir())
			recs[idx], errs[idx] = wcli.CreateSandbox(ctx, CreateSandboxRequest{
				Name:     "shared-worker-name",
				Template: "ubuntu",
			})
		}(i)
	}
	wg.Wait()

	seenIDs := make(map[string]bool)
	for i := 0; i < workers; i++ {
		if errs[i] != nil {
			t.Fatalf("worker %d CreateSandbox failed: %v", i, errs[i])
		}
		r := recs[i]
		if r.Name != "shared-worker-name" {
			t.Fatalf("worker %d Name = %q, want 'shared-worker-name'", i, r.Name)
		}
		if gotHost := ExtractInitialHostFromSandboxID(r.SandboxID); gotHost != r.HostID {
			t.Fatalf("ExtractInitialHostFromSandboxID(%q) = %q, want %q", r.SandboxID, gotHost, r.HostID)
		}
		if seenIDs[r.SandboxID] {
			t.Fatalf("duplicate generated SandboxID %q", r.SandboxID)
		}
		seenIDs[r.SandboxID] = true
	}

	// 2. Zero-lookup routing: a fresh CLI with empty routes.json and NO ControlPlaneURL can route
	// directly to `recs[0].SandboxID` by extracting the initial host ID from the sandbox ID!
	zeroLookupCLI := NewCLIClient("", poolTS.URL, t.TempDir())
	execResp, err := zeroLookupCLI.Exec(ctx, ExecRequest{
		SandboxID: recs[0].SandboxID,
		Command:   []string{"sh", "-c", "echo zero-lookup-ok"},
	})
	if err != nil || strings.TrimSpace(execResp.Stdout) != "zero-lookup-ok" {
		t.Fatalf("zeroLookupCLI.Exec(%s) failed: stdout=%q, err=%v", recs[0].SandboxID, execResp.Stdout, err)
	}

	// 3. Listing by `--name=shared-worker-name` returns all 6 sandboxes
	_ = h1.SyncStateAndHeartbeat(ctx)
	_ = h2.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)
	var listRes struct {
		Sandboxes []SandboxRecord `json:"sandboxes"`
		Count     int             `json:"count"`
	}
	if _, err := cli.doJSON(ctx, http.MethodGet, cpTS.URL+"/v1/sandboxes?name=shared-worker-name", nil, nil, &listRes); err != nil {
		t.Fatalf("GET /v1/sandboxes?name=shared-worker-name failed: %v", err)
	}
	if listRes.Count != workers {
		t.Fatalf("list count for shared-worker-name = %d, want %d", listRes.Count, workers)
	}

	// 4. Delete recs[0].SandboxID and verify immediate 404 from control plane
	var delOut, delErr bytes.Buffer
	if code := RunCLI(ctx, []string{"--control-plane-url=" + cpTS.URL, "--pool-url=" + poolTS.URL, "delete", recs[0].SandboxID}, &delOut, &delErr); code != 0 {
		t.Fatalf("delete CLI failed: %s", delErr.String())
	}
	var got SandboxRecord
	status, _ := cli.doJSON(ctx, http.MethodGet, cpTS.URL+"/v1/sandboxes/"+recs[0].SandboxID, nil, nil, &got)
	if status != http.StatusNotFound {
		t.Fatalf("GET /v1/sandboxes/%s after delete returned %d, want 404", recs[0].SandboxID, status)
	}
}

func TestInGuestIdleStateResetAndSymlinkPermissions(t *testing.T) {
	ctx := context.Background()
	router, cp, poolTS, cpTS, _ := newTestFleet(t, 5)
	h1 := router.getOrCreateHost("host-idle-1")
	_ = h1.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)

	cli := NewCLIClient(cpTS.URL, poolTS.URL, t.TempDir())
	if _, err := cli.CreateSandbox(ctx, CreateSandboxRequest{
		SandboxID:  "sbx-idle-1",
		Template:   "ubuntu",
		Persistent: true,
	}); err != nil {
		t.Fatalf("CreateSandbox failed: %v", err)
	}

	// Create executable script (0755) and symlink inside /workspace, then write IDLE to /var/run/instance/state
	if _, err := cli.Exec(ctx, ExecRequest{
		SandboxID: "sbx-idle-1",
		Command: []string{"sh", "-c",
			"printf '#!/bin/sh\\necho exec-perm-ok\\n' > /workspace/run.sh && chmod 0755 /workspace/run.sh && ln -s /workspace/run.sh /workspace/link.sh && echo IDLE > /var/run/instance/state"},
	}); err != nil {
		t.Fatalf("setup script & IDLE signal failed: %v", err)
	}

	// Trigger in-guest idle check on host
	h1.checkInGuestAndIdleTimeouts(ctx)
	if h1.HostState().ActiveSandboxes != 0 {
		t.Fatalf("expected sbx-idle-1 to auto-suspend on IDLE signal, active=%d", h1.HostState().ActiveSandboxes)
	}

	// Subsequent Exec must auto-resume, preserve symlink & 0755 permissions, and reset /var/run/instance/state to ACTIVE!
	res, err := cli.Exec(ctx, ExecRequest{
		SandboxID: "sbx-idle-1",
		Command:   []string{"sh", "-c", "/workspace/link.sh && cat /var/run/instance/state"},
	})
	if err != nil {
		t.Fatalf("Exec after IDLE auto-suspend failed: %v", err)
	}
	if !strings.Contains(res.Stdout, "exec-perm-ok") || !strings.Contains(res.Stdout, "ACTIVE") {
		t.Fatalf("unexpected stdout after auto-resume: %q", res.Stdout)
	}

	// Verify another checkInGuestAndIdleTimeouts pass does NOT re-suspend the resumed sandbox
	h1.checkInGuestAndIdleTimeouts(ctx)
	if h1.HostState().ActiveSandboxes != 1 {
		t.Fatalf("resumed sandbox was erroneously re-suspended! active=%d", h1.HostState().ActiveSandboxes)
	}
}

func TestExecBackgroundAndTimeoutProcessGroup(t *testing.T) {
	ctx := context.Background()
	router, cp, poolTS, cpTS, _ := newTestFleet(t, 5)
	h1 := router.getOrCreateHost("host-bg-1")
	_ = h1.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)

	cli := NewCLIClient(cpTS.URL, poolTS.URL, t.TempDir())
	if _, err := cli.CreateSandbox(ctx, CreateSandboxRequest{
		SandboxID: "sbx-bg-1",
		Template:  "ubuntu",
	}); err != nil {
		t.Fatalf("CreateSandbox failed: %v", err)
	}

	// 1. Background process must survive after HTTP request returns
	if _, err := cli.Exec(ctx, ExecRequest{
		SandboxID:  "sbx-bg-1",
		Background: true,
		Command:    []string{"sh", "-c", "sleep 0.2 && echo bg-alive > /workspace/bg.txt"},
	}); err != nil {
		t.Fatalf("background Exec failed: %v", err)
	}
	time.Sleep(450 * time.Millisecond)
	chk, err := cli.Exec(ctx, ExecRequest{
		SandboxID: "sbx-bg-1",
		Command:   []string{"sh", "-c", "cat /workspace/bg.txt"},
	})
	if err != nil || strings.TrimSpace(chk.Stdout) != "bg-alive" {
		t.Fatalf("background process did not complete: stdout=%q, err=%v", chk.Stdout, err)
	}

	// 2. Foreground command exceeding TimeoutSec must be killed promptly with exit code 124
	start := time.Now()
	toResp, err := cli.Exec(ctx, ExecRequest{
		SandboxID:  "sbx-bg-1",
		TimeoutSec: 1,
		Command:    []string{"sh", "-c", "sleep 10"},
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("timeout Exec returned unexpected transport error: %v", err)
	}
	if toResp.ExitCode != 124 {
		t.Fatalf("timeout Exec ExitCode = %d, want 124", toResp.ExitCode)
	}
	if elapsed > 4*time.Second {
		t.Fatalf("timeout Exec took %v, expected ~1s process-group kill", elapsed)
	}
}

func TestCLIInitAndAmbiguousSandboxName(t *testing.T) {
	ctx := context.Background()
	router, cp, poolTS, cpTS, _ := newTestFleet(t, 5)
	h1 := router.getOrCreateHost("host-init-1")
	_ = h1.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)

	cacheDir := t.TempDir()
	var outBuf, errBuf bytes.Buffer
	code := RunCLI(ctx, []string{"--cache-dir=" + cacheDir, "init", cpTS.URL}, &outBuf, &errBuf)
	if code != 0 {
		t.Fatalf("sbx init failed (code=%d): stderr=%s", code, errBuf.String())
	}
	if !strings.Contains(outBuf.String(), poolTS.URL) {
		t.Fatalf("expected sbx init output to contain discovered pool URL %s, got: %s", poolTS.URL, outBuf.String())
	}

	// Subsequent commands using only --cache-dir (no --control-plane-url or --pool-url) must work via endpoints.json!
	outBuf.Reset()
	errBuf.Reset()
	if code := RunCLI(ctx, []string{"--cache-dir=" + cacheDir, "create", "shared-box", "-q"}, &outBuf, &errBuf); code != 0 {
		t.Fatalf("first create -q failed: %s", errBuf.String())
	}
	id1 := strings.TrimSpace(outBuf.String())
	if ExtractInitialHostFromSandboxID(id1) == "" {
		t.Fatalf("expected create -q to print only distributed sandbox_id, got %q", id1)
	}

	outBuf.Reset()
	errBuf.Reset()
	if code := RunCLI(ctx, []string{"--cache-dir=" + cacheDir, "create", "shared-box", "-q"}, &outBuf, &errBuf); code != 0 {
		t.Fatalf("second create -q failed: %s", errBuf.String())
	}
	id2 := strings.TrimSpace(outBuf.String())
	if id1 == id2 {
		t.Fatalf("expected distinct sandbox_ids for duplicate name, both got %q", id1)
	}
	_ = cp.SyncFromStore(ctx)

	// Passing the ambiguous name "shared-box" to exec must fail with a clear ambiguity error!
	outBuf.Reset()
	errBuf.Reset()
	if code := RunCLI(ctx, []string{"--cache-dir=" + cacheDir, "exec", "shared-box", "--", "echo", "hi"}, &outBuf, &errBuf); code == 0 {
		t.Fatalf("expected exec with ambiguous name 'shared-box' to fail, but succeeded")
	}
	if !strings.Contains(errBuf.String(), "ambiguous sandbox name") {
		t.Fatalf("expected ambiguous sandbox name error, got: %s", errBuf.String())
	}

	// Passing the explicit sandbox_id must succeed!
	outBuf.Reset()
	errBuf.Reset()
	if code := RunCLI(ctx, []string{"--cache-dir=" + cacheDir, "exec", id1, "--", "echo", "explicit-ok"}, &outBuf, &errBuf); code != 0 {
		t.Fatalf("exec with explicit sandbox_id %s failed: %s", id1, errBuf.String())
	}
	if strings.TrimSpace(outBuf.String()) != "explicit-ok" {
		t.Fatalf("unexpected stdout: %q", outBuf.String())
	}

	// Verify sbx copy -q mints a distributed sbx-<host>-<8hex> sandbox_id and allows duplicate copy names
	outBuf.Reset()
	errBuf.Reset()
	if code := RunCLI(ctx, []string{"--cache-dir=" + cacheDir, "copy", id1, "dup-copy-name", "-q"}, &outBuf, &errBuf); code != 0 {
		t.Fatalf("first copy -q failed: %s", errBuf.String())
	}
	copyID1 := strings.TrimSpace(outBuf.String())
	if ExtractInitialHostFromSandboxID(copyID1) == "" || copyID1 == "dup-copy-name" {
		t.Fatalf("expected copy -q to mint distributed sbx-<host>-<8hex> ID, got %q", copyID1)
	}

	outBuf.Reset()
	errBuf.Reset()
	if code := RunCLI(ctx, []string{"--cache-dir=" + cacheDir, "copy", id1, "dup-copy-name", "-q"}, &outBuf, &errBuf); code != 0 {
		t.Fatalf("second copy -q with duplicate target name failed: %s", errBuf.String())
	}
	copyID2 := strings.TrimSpace(outBuf.String())
	if copyID1 == copyID2 || ExtractInitialHostFromSandboxID(copyID2) == "" {
		t.Fatalf("expected distinct distributed sandbox_ids for duplicate copy name, got %q and %q", copyID1, copyID2)
	}

	// Verify sbx logs --tail=2 slices to the last 2 lines
	for _, line := range []string{"line-1", "line-2", "line-3", "line-4"} {
		outBuf.Reset()
		errBuf.Reset()
		if code := RunCLI(ctx, []string{"--cache-dir=" + cacheDir, "exec", id1, "--", "echo", line}, &outBuf, &errBuf); code != 0 {
			t.Fatalf("exec echo %s failed: %s", line, errBuf.String())
		}
	}
	outBuf.Reset()
	errBuf.Reset()
	if code := RunCLI(ctx, []string{"--cache-dir=" + cacheDir, "logs", id1, "--tail=2"}, &outBuf, &errBuf); code != 0 {
		t.Fatalf("logs --tail=2 failed: %s", errBuf.String())
	}
	tailLines := strings.Split(strings.TrimSpace(outBuf.String()), "\n")
	if len(tailLines) != 2 || !strings.Contains(tailLines[1], "line-4") {
		t.Fatalf("expected exactly 2 lines ending with line-4 from logs --tail=2, got %d lines: %q", len(tailLines), outBuf.String())
	}

	// Verify uninitialized CLI prints friendly error instead of unsupported protocol scheme ""
	emptyDir := t.TempDir()
	outBuf.Reset()
	errBuf.Reset()
	if code := RunCLI(ctx, []string{"--cache-dir=" + emptyDir, "--config-dir=" + emptyDir, "list"}, &outBuf, &errBuf); code == 0 {
		t.Fatalf("expected uninitialized CLI list to fail, but succeeded")
	}
	if !strings.Contains(errBuf.String(), "CLI not initialized") {
		t.Fatalf("expected friendly 'CLI not initialized' message, got: %s", errBuf.String())
	}
}

func TestUIServerSecurityHeadersCSRFAndOverview(t *testing.T) {
	ctx := context.Background()
	router, cp, poolTS, cpTS, _ := newTestFleet(t, 5)
	h1 := router.getOrCreateHost("host-ui-1")
	_ = h1.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)

	uiSrv := NewUIServer(cpTS.URL, poolTS.URL, t.TempDir())

	// 1. GET / must return strict security headers, per-request CSP nonce, CSRF cookie, and zero innerHTML
	reqRoot := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8080/", nil)
	wRoot := httptest.NewRecorder()
	uiSrv.ServeHTTP(wRoot, reqRoot)
	if wRoot.Code != http.StatusOK {
		t.Fatalf("GET / status = %d, want 200", wRoot.Code)
	}
	csp := wRoot.Header().Get("Content-Security-Policy")
	if !strings.Contains(csp, "'nonce-") || !strings.Contains(csp, "frame-ancestors 'none'") || !strings.Contains(csp, "style-src-attr 'unsafe-inline'") {
		t.Fatalf("unexpected Content-Security-Policy header: %q", csp)
	}
	if got := wRoot.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("X-Content-Type-Options = %q, want nosniff", got)
	}
	if got := wRoot.Header().Get("X-Frame-Options"); got != "DENY" {
		t.Fatalf("X-Frame-Options = %q, want DENY", got)
	}
	if got := wRoot.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
	bodyHTML := wRoot.Body.String()
	if strings.Contains(bodyHTML, "innerHTML") {
		t.Fatalf("served HTML/JS must not contain innerHTML")
	}
	var csrfCookie *http.Cookie
	for _, c := range wRoot.Result().Cookies() {
		if c.Name == "sbx_csrf" {
			csrfCookie = c
			break
		}
	}
	if csrfCookie == nil || !csrfCookie.HttpOnly || len(csrfCookie.Value) < 32 {
		t.Fatalf("expected HttpOnly sbx_csrf cookie, got %+v", csrfCookie)
	}

	// 2. Disallowed HTTP method (TRACE) must return 405 Method Not Allowed
	reqTrace := httptest.NewRequest(http.MethodTrace, "http://127.0.0.1:8080/api/overview", nil)
	wTrace := httptest.NewRecorder()
	uiSrv.ServeHTTP(wTrace, reqTrace)
	if wTrace.Code != http.StatusMethodNotAllowed {
		t.Fatalf("TRACE /api/overview status = %d, want 405", wTrace.Code)
	}

	// 3. POST /api/sandboxes/create without X-CSRF-Token must return 403 Forbidden
	reqNoCSRF := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8080/api/sandboxes/create",
		strings.NewReader(`{"name":"ui-box","template":"ubuntu"}`))
	reqNoCSRF.AddCookie(csrfCookie)
	wNoCSRF := httptest.NewRecorder()
	uiSrv.ServeHTTP(wNoCSRF, reqNoCSRF)
	if wNoCSRF.Code != http.StatusForbidden {
		t.Fatalf("POST without X-CSRF-Token status = %d, want 403", wNoCSRF.Code)
	}

	// 4. POST /api/sandboxes/create with valid X-CSRF-Token and cookie must succeed
	reqCreate := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8080/api/sandboxes/create",
		strings.NewReader(`{"name":"ui-box","template":"ubuntu","persistent":true}`))
	reqCreate.AddCookie(csrfCookie)
	reqCreate.Header.Set("X-CSRF-Token", csrfCookie.Value)
	wCreate := httptest.NewRecorder()
	uiSrv.ServeHTTP(wCreate, reqCreate)
	if wCreate.Code != http.StatusOK {
		t.Fatalf("POST /api/sandboxes/create with valid CSRF failed (%d): %s", wCreate.Code, wCreate.Body.String())
	}

	// 5. GET /api/overview must aggregate stats, sandboxes, hosts, templates, metrics, and security policy
	_ = h1.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)
	reqOv := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8080/api/overview", nil)
	reqOv.Header.Set("X-Goog-Authenticated-User-Email", "accounts.google.com:admin@example.com")
	wOv := httptest.NewRecorder()
	uiSrv.ServeHTTP(wOv, reqOv)
	if wOv.Code != http.StatusOK || !strings.Contains(wOv.Body.String(), "ui-box") {
		t.Fatalf("GET /api/overview failed (%d): %s", wOv.Code, wOv.Body.String())
	}
	if !strings.Contains(wOv.Body.String(), `"metrics"`) || !strings.Contains(wOv.Body.String(), `"security"`) {
		t.Fatalf("expected GET /api/overview to include metrics and security fields, got: %s", wOv.Body.String())
	}
	if !strings.Contains(bodyHTML, "tab-security") || !strings.Contains(bodyHTML, "Memory Overcommit") {
		t.Fatalf("expected UI HTML to include tab-security and Memory Overcommit observability")
	}
}

func TestClientVersionEnforcementAndUpgradeRequired(t *testing.T) {
	ctx := context.Background()
	router, cp, poolTS, cpTS, _ := newTestFleet(t, 5)
	h1 := router.getOrCreateHost("host-ver-1")
	_ = h1.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)

	if CompareSemver("1.2.0", "1.0.0") <= 0 || CompareSemver("1.0.0", "1.2.0") >= 0 || CompareSemver("v1.2.0", "1.2.0") != 0 {
		t.Fatalf("CompareSemver unexpected ordering")
	}

	cacheDir := t.TempDir()
	var outBuf, errBuf bytes.Buffer
	if code := RunCLI(ctx, []string{"--control-plane-url=" + cpTS.URL, "--pool-url=" + poolTS.URL, "--cache-dir=" + cacheDir, "version"}, &outBuf, &errBuf); code != 0 {
		t.Fatalf("sbx version failed: %s", errBuf.String())
	}
	if !strings.Contains(outBuf.String(), DispenserVersion) {
		t.Fatalf("expected sbx version output to contain %s, got %q", DispenserVersion, outBuf.String())
	}

	// Raise min_client_version to 2.0.0 via POST /v1/config
	cli := NewCLIClient(cpTS.URL, poolTS.URL, cacheDir)
	var updatedCfg FleetConfig
	if status, err := cli.doJSON(ctx, http.MethodPost, cpTS.URL+"/v1/config", nil, map[string]any{
		"min_client_version": "2.0.0",
	}, &updatedCfg); err != nil || status != http.StatusOK {
		t.Fatalf("update min_client_version failed (%d): %v", status, err)
	}
	// Sync host so h1 learns min_client_version=2.0.0 from heartbeat/store
	if err := h1.SyncStateAndHeartbeat(ctx); err != nil {
		t.Fatalf("SyncStateAndHeartbeat with min_client_version=2.0.0 failed: %v", err)
	}

	// Old client (v1.2.0 < 2.0.0) must receive HTTP 426 Upgrade Required from both ControlPlaneServer and HostServer!
	oldCLI := NewCLIClient(cpTS.URL, poolTS.URL, t.TempDir())
	oldCLI.ClientVersion = "1.2.0"
	var listRes map[string]any
	statusCP, errCP := oldCLI.doJSON(ctx, http.MethodGet, cpTS.URL+"/v1/sandboxes", nil, nil, &listRes)
	if statusCP != http.StatusUpgradeRequired || errCP == nil || !strings.Contains(errCP.Error(), "client_upgrade_required") {
		t.Fatalf("expected HTTP 426 client_upgrade_required from ControlPlaneServer, got status=%d err=%v", statusCP, errCP)
	}

	_, errHost := oldCLI.CreateSandbox(ctx, CreateSandboxRequest{
		Name:         "old-cli-box",
		Template:     "ubuntu",
		TargetHostID: "host-ver-1",
	})
	if errHost == nil || !strings.Contains(errHost.Error(), "426") {
		t.Fatalf("expected HTTP 426 from HostServer on old client create, got err=%v", errHost)
	}

	// Upgraded client (v2.0.0) must succeed against both ControlPlaneServer and HostServer!
	newCLI := NewCLIClient(cpTS.URL, poolTS.URL, t.TempDir())
	newCLI.ClientVersion = "2.0.0"
	rec, err := newCLI.CreateSandbox(ctx, CreateSandboxRequest{
		Name:         "new-cli-box",
		Template:     "ubuntu",
		TargetHostID: "host-ver-1",
	})
	if err != nil || rec.Status != StatusRunning {
		t.Fatalf("expected v2.0.0 client CreateSandbox to succeed, got rec=%+v err=%v", rec, err)
	}
}

func TestServerSideTemplateFromSandboxConfigSizingAndDeploy(t *testing.T) {
	ctx := context.Background()
	router, cp, poolTS, cpTS, _ := newTestFleet(t, 5)
	h1 := router.getOrCreateHost("host-srv-1")
	h2 := router.getOrCreateHost("host-srv-2")
	_ = h1.SyncStateAndHeartbeat(ctx)
	_ = h2.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)

	cacheDir := t.TempDir()
	cli := NewCLIClient(cpTS.URL, poolTS.URL, cacheDir)

	// 1. Create a running python sandbox on host-srv-1 and write a marker file
	srcRec, err := cli.CreateSandbox(ctx, CreateSandboxRequest{
		Name:         "py-tpl-source",
		Template:     "python",
		Persistent:   true,
		TargetHostID: "host-srv-1",
	})
	if err != nil {
		t.Fatalf("CreateSandbox(py-tpl-source) failed: %v", err)
	}
	if _, err := cli.Exec(ctx, ExecRequest{
		SandboxID: srcRec.SandboxID,
		Command:   []string{"sh", "-c", "echo 'server-baked-template' > /workspace/baked.txt"},
	}); err != nil {
		t.Fatalf("Exec write marker failed: %v", err)
	}
	_ = h1.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)

	// 2. Create template via `sbx template create --from-sandbox=py-tpl-source` (server-side orchestration!)
	var outBuf, errBuf bytes.Buffer
	if code := RunCLI(ctx, []string{"--control-plane-url=" + cpTS.URL, "--pool-url=" + poolTS.URL, "--cache-dir=" + cacheDir,
		"template", "create", "custom-py-tpl", "--from-sandbox=py-tpl-source", "--description=ServerBaked"}, &outBuf, &errBuf); code != 0 {
		t.Fatalf("template create --from-sandbox failed: %s", errBuf.String())
	}

	// Verify duplicate template creation without --force fails on the server with 409
	outBuf.Reset()
	errBuf.Reset()
	if code := RunCLI(ctx, []string{"--control-plane-url=" + cpTS.URL, "--pool-url=" + poolTS.URL, "--cache-dir=" + cacheDir,
		"template", "create", "custom-py-tpl", "--from-sandbox=py-tpl-source"}, &outBuf, &errBuf); code == 0 {
		t.Fatalf("expected duplicate template create without --force to fail")
	}

	// Verify built-in template override is rejected by the server
	outBuf.Reset()
	errBuf.Reset()
	if code := RunCLI(ctx, []string{"--control-plane-url=" + cpTS.URL, "--pool-url=" + poolTS.URL, "--cache-dir=" + cacheDir,
		"template", "create", "ubuntu"}, &outBuf, &errBuf); code == 0 {
		t.Fatalf("expected overriding built-in template 'ubuntu' to fail")
	}

	// 3. Create a new sandbox from `custom-py-tpl` and verify `/workspace/baked.txt` is present
	fromTplRec, err := cli.CreateSandbox(ctx, CreateSandboxRequest{
		Name:         "from-custom-py",
		Template:     "custom-py-tpl",
		TargetHostID: "host-srv-2",
	})
	if err != nil {
		t.Fatalf("CreateSandbox from custom-py-tpl failed: %v", err)
	}
	execRes, err := cli.Exec(ctx, ExecRequest{
		SandboxID: fromTplRec.SandboxID,
		Command:   []string{"sh", "-c", "cat /workspace/baked.txt"},
	})
	if err != nil || strings.TrimSpace(execRes.Stdout) != "server-baked-template" {
		t.Fatalf("expected baked.txt in sandbox from custom-py-tpl, got stdout=%q err=%v", execRes.Stdout, err)
	}

	// 4. Verify `sbx migrate` uses server-returned `PreviousHostID` without pre-migrate inspect
	outBuf.Reset()
	errBuf.Reset()
	if code := RunCLI(ctx, []string{"--control-plane-url=" + cpTS.URL, "--pool-url=" + poolTS.URL, "--cache-dir=" + cacheDir,
		"migrate", srcRec.SandboxID, "--to-host=host-srv-2"}, &outBuf, &errBuf); code != 0 {
		t.Fatalf("sbx migrate failed: %s", errBuf.String())
	}
	if !strings.Contains(outBuf.String(), "from host-srv-1 -> host-srv-2") {
		t.Fatalf("expected migration output to contain 'from host-srv-1 -> host-srv-2', got: %s", outBuf.String())
	}

	// 5. Verify server-side config sizing (`PoolRolloutTriggered`) and `POST /v1/deploy`
	var cfgResp FleetConfig
	if status, err := cli.doJSON(ctx, http.MethodPost, cpTS.URL+"/v1/config", nil, map[string]any{
		"host_cpu":    "8",
		"host_memory": "16Gi",
	}, &cfgResp); err != nil || status != http.StatusOK {
		t.Fatalf("POST /v1/config failed (%d): %v", status, err)
	}
	if !cfgResp.PoolRolloutTriggered || cfgResp.HostCPU != "8" || cfgResp.HostMemory != "16Gi" {
		t.Fatalf("expected PoolRolloutTriggered=true and 8/16Gi, got %+v", cfgResp)
	}

	var depResp DeployFleetResponse
	if status, err := cli.doJSON(ctx, http.MethodPost, cpTS.URL+"/v1/deploy", nil, DeployFleetRequest{
		Project:    "my-gcp-project",
		Region:     "us-west1",
		HostCPU:    "4",
		HostMemory: "8Gi",
		MaxDensity: 10,
	}, &depResp); err != nil || status != http.StatusOK || depResp.Status != "DEPLOYED" {
		t.Fatalf("POST /v1/deploy failed (%d): resp=%+v err=%v", status, depResp, err)
	}
}

func TestOvercommitObservabilityAndHistoricalMetrics(t *testing.T) {
	ctx := context.Background()
	router, cp, poolTS, cpTS, _ := newTestFleet(t, 10)
	h1 := router.getOrCreateHost("host-obs-1")
	_ = h1.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)

	cacheDir := t.TempDir()
	cli := NewCLIClient(cpTS.URL, poolTS.URL, cacheDir)

	// Create 3 sandboxes requesting 4Gi each on an 8Gi host (12Gi allocated = 1.5x overcommit)
	var sbx1 SandboxRecord
	for i, name := range []string{"sbx-obs-1", "sbx-obs-2", "sbx-obs-3"} {
		rec, err := cli.CreateSandbox(ctx, CreateSandboxRequest{
			Name:         name,
			Template:     "python",
			CPU:          "1",
			Memory:       "4Gi",
			TargetHostID: "host-obs-1",
		})
		if err != nil {
			t.Fatalf("CreateSandbox(%s) failed: %v", name, err)
		}
		if i == 0 {
			sbx1 = rec
		}
	}

	// Simulate 1536 MB memory usage and 45% CPU usage on sbx-obs-1 via update metadata / env
	var updatedSbx SandboxRecord
	if err := cli.CallSandboxEndpoint(ctx, http.MethodPost, "/__session/update", sbx1.SandboxID, map[string]any{
		"sandbox_id": sbx1.SandboxID,
		"metadata": map[string]string{
			"simulated_memory_mb": "1536",
			"simulated_cpu_pct":   "45.0",
		},
	}, &updatedSbx); err != nil {
		t.Fatalf("update simulated load on %s failed: %v", sbx1.SandboxID, err)
	}
	_ = h1.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)

	// Query GET /v1/metrics?host=host-obs-1&history=true
	var metricsResp struct {
		FleetMemoryOvercommitRatio float64          `json:"fleet_memory_overcommit_ratio"`
		FleetCPUOvercommitRatio    float64          `json:"fleet_cpu_overcommit_ratio"`
		Hosts                      map[string]any   `json:"hosts"`
		Recommendations            []string         `json:"recommendations"`
		History                    map[string][]any `json:"history"`
	}
	status, err := cli.doJSON(ctx, http.MethodGet, cpTS.URL+"/v1/metrics?host=host-obs-1&history=true", nil, nil, &metricsResp)
	if err != nil || status != http.StatusOK {
		t.Fatalf("GET /v1/metrics failed (%d): %v", status, err)
	}
	if metricsResp.FleetMemoryOvercommitRatio < 1.4 {
		t.Fatalf("expected FleetMemoryOvercommitRatio >= 1.5 (12Gi/8Gi), got %.2f", metricsResp.FleetMemoryOvercommitRatio)
	}
	if len(metricsResp.Recommendations) == 0 {
		t.Fatalf("expected non-empty sizing/overcommit recommendations from /v1/metrics")
	}
	if len(metricsResp.History["host-obs-1"]) == 0 {
		t.Fatalf("expected historical resource samples for host-obs-1")
	}

	// Verify `sbx metrics --host=host-obs-1 --history` CLI command succeeds
	var outBuf, errBuf bytes.Buffer
	if code := RunCLI(ctx, []string{"--control-plane-url=" + cpTS.URL, "--pool-url=" + poolTS.URL, "--cache-dir=" + cacheDir,
		"metrics", "--host=host-obs-1", "--history"}, &outBuf, &errBuf); code != 0 {
		t.Fatalf("sbx metrics failed (code=%d): %s", code, errBuf.String())
	}
	if !strings.Contains(outBuf.String(), "host-obs-1") || !strings.Contains(outBuf.String(), "Overcommit") {
		t.Fatalf("expected sbx metrics output to include host-obs-1 and Overcommit ratio, got:\n%s", outBuf.String())
	}
}

func TestMemoryPressureAdmissionRejection(t *testing.T) {
	ctx := context.Background()
	router, cp, poolTS, cpTS, _ := newTestFleet(t, 10)
	h1 := router.getOrCreateHost("host-mem-1")
	h2 := router.getOrCreateHost("host-mem-2")
	_ = h1.SyncStateAndHeartbeat(ctx)
	_ = h2.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)

	cacheDir := t.TempDir()
	cli := NewCLIClient(cpTS.URL, poolTS.URL, cacheDir)

	// Set min_free_memory_pct=25% and min_free_memory_mb=1024 via sbx config set
	var outBuf, errBuf bytes.Buffer
	if code := RunCLI(ctx, []string{"--control-plane-url=" + cpTS.URL, "--pool-url=" + poolTS.URL, "--cache-dir=" + cacheDir,
		"config", "set", "--min-free-memory-mb=1024", "--min-free-memory-pct=25", "--hot-memory-threshold-pct=95"}, &outBuf, &errBuf); code != 0 {
		t.Fatalf("sbx config set memory thresholds failed: %s", errBuf.String())
	}
	_ = h1.SyncStateAndHeartbeat(ctx)
	_ = h2.SyncStateAndHeartbeat(ctx)

	// Create first sandbox on host-mem-1 and simulate 6800 MB used out of 8192 MB (~83% used, leaving ~17% free < 25% min_free_memory_pct)
	sbx1, err := cli.CreateSandbox(ctx, CreateSandboxRequest{
		Name:         "mem-hog-1",
		Template:     "ubuntu",
		Memory:       "2Gi",
		TargetHostID: "host-mem-1",
	})
	if err != nil {
		t.Fatalf("CreateSandbox(mem-hog-1) failed: %v", err)
	}
	var upd SandboxRecord
	if err := cli.CallSandboxEndpoint(ctx, http.MethodPost, "/__session/update", sbx1.SandboxID, map[string]any{
		"sandbox_id": sbx1.SandboxID,
		"metadata": map[string]string{
			"simulated_memory_mb": "6800",
		},
	}, &upd); err != nil {
		t.Fatalf("update simulated_memory_mb failed: %v", err)
	}
	_ = h1.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)

	// 1. Explicit pinned create on host-mem-1 must be REJECTED with HTTP 429 host_memory_pressure!
	_, pinErr := cli.CreateSandbox(ctx, CreateSandboxRequest{
		Name:         "mem-rejected",
		Template:     "ubuntu",
		Memory:       "1Gi",
		TargetHostID: "host-mem-1",
	})
	if pinErr == nil || !strings.Contains(pinErr.Error(), "host_memory_pressure") {
		t.Fatalf("expected pinned create on memory-pressured host-mem-1 to fail with host_memory_pressure, got: %v", pinErr)
	}

	// 2. Unpinned create must automatically skip host-mem-1 and succeed on healthy host-mem-2!
	spillRec, spillErr := cli.CreateSandbox(ctx, CreateSandboxRequest{
		Name:     "mem-spillover",
		Template: "ubuntu",
		Memory:   "1Gi",
	})
	if spillErr != nil {
		t.Fatalf("expected unpinned create to spill over to healthy host, got error: %v", spillErr)
	}
	if spillRec.HostID == "host-mem-1" {
		t.Fatalf("expected unpinned create to avoid memory-pressured host-mem-1, but landed on %s", spillRec.HostID)
	}
}

func TestHotHostProactiveLiveMigration(t *testing.T) {
	ctx := context.Background()
	router, cp, poolTS, cpTS, _ := newTestFleet(t, 10)
	hSrc := router.getOrCreateHost("host-hot-src")
	hDst := router.getOrCreateHost("host-hot-dst")
	_ = hSrc.SyncStateAndHeartbeat(ctx)
	_ = hDst.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)

	cacheDir := t.TempDir()
	cli := NewCLIClient(cpTS.URL, poolTS.URL, cacheDir)

	// Configure hot_memory_threshold_pct = 75%
	var cfg FleetConfig
	if _, err := cli.doJSON(ctx, http.MethodPost, cpTS.URL+"/v1/config", nil, map[string]any{
		"hot_memory_threshold_pct": 75.0,
		"min_free_memory_pct":      15.0,
	}, &cfg); err != nil {
		t.Fatalf("configure hot_memory_threshold_pct failed: %v", err)
	}
	_ = hSrc.SyncStateAndHeartbeat(ctx)
	_ = hDst.SyncStateAndHeartbeat(ctx)

	// Place 2 unpinned sandboxes on host-hot-src
	var sbxLight, sbxHeavy SandboxRecord
	if _, err := cli.doJSON(ctx, http.MethodPost, poolTS.URL+"/__session/create",
		map[string]string{"X-Serverless-Session-Id": "host-hot-src"},
		CreateSandboxRequest{Name: "hot-light", Template: "ubuntu", Persistent: true, Memory: "2Gi"}, &sbxLight); err != nil {
		t.Fatalf("create hot-light failed: %v", err)
	}
	if _, err := cli.doJSON(ctx, http.MethodPost, poolTS.URL+"/__session/create",
		map[string]string{"X-Serverless-Session-Id": "host-hot-src"},
		CreateSandboxRequest{Name: "hot-heavy", Template: "ubuntu", Persistent: true, Memory: "4Gi"}, &sbxHeavy); err != nil {
		t.Fatalf("create hot-heavy failed: %v", err)
	}

	// Write stateful data inside hot-heavy
	if _, err := cli.Exec(ctx, ExecRequest{
		SandboxID: sbxHeavy.SandboxID,
		Command:   []string{"sh", "-c", "echo 'hot-migration-rpo-zero' > /workspace/state.txt"},
	}); err != nil {
		t.Fatalf("Exec state write failed: %v", err)
	}

	// Drive host-hot-src memory usage above 75% (hot-light = 2500MB, hot-heavy = 4200MB -> 6700MB / 8192MB = ~81.8% > 75%)
	var u1, u2 SandboxRecord
	_ = cli.CallSandboxEndpoint(ctx, http.MethodPost, "/__session/update", sbxLight.SandboxID, map[string]any{
		"sandbox_id": sbxLight.SandboxID,
		"metadata":   map[string]string{"simulated_memory_mb": "2500"},
	}, &u1)
	_ = cli.CallSandboxEndpoint(ctx, http.MethodPost, "/__session/update", sbxHeavy.SandboxID, map[string]any{
		"sandbox_id": sbxHeavy.SandboxID,
		"metadata":   map[string]string{"simulated_memory_mb": "4200"},
	}, &u2)

	// Trigger proactive overload check on host-hot-src via POST /__internal/check-overload (and background loop)
	var shedResp map[string]any
	status, err := cli.doJSON(ctx, http.MethodPost, poolTS.URL+"/__internal/check-overload",
		map[string]string{"X-Serverless-Session-Id": "host-hot-src"}, nil, &shedResp)
	if err != nil || status != http.StatusOK {
		t.Fatalf("POST /__internal/check-overload failed (%d): %v", status, err)
	}

	// Verify hot-heavy was automatically live-migrated off host-hot-src to host-hot-dst with RPO=0!
	execAfter, err := cli.Exec(ctx, ExecRequest{
		SandboxID: sbxHeavy.SandboxID,
		Command:   []string{"sh", "-c", "cat /workspace/state.txt"},
	})
	if err != nil {
		t.Fatalf("Exec on hot-heavy after proactive hot-host live migration failed: %v", err)
	}
	if strings.TrimSpace(execAfter.Stdout) != "hot-migration-rpo-zero" {
		t.Fatalf("state after hot migration = %q, want 'hot-migration-rpo-zero'", execAfter.Stdout)
	}
	if execAfter.HostID == "host-hot-src" {
		t.Fatalf("expected hot-heavy (%s) to be live-migrated off host-hot-src, still on %s", sbxHeavy.SandboxID, execAfter.HostID)
	}
}

func TestSandboxAllowEgressControl(t *testing.T) {
	ctx := context.Background()
	router, cp, poolTS, cpTS, _ := newTestFleet(t, 10)
	h := router.getOrCreateHost("host-egress-1")
	_ = h.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)

	cacheDir := t.TempDir()
	var stdout, stderr bytes.Buffer

	// 1. Create a sandbox with --allow-egress=false
	code := RunCLI(ctx, []string{
		"--control-plane-url=" + cpTS.URL, "--pool-url=" + poolTS.URL, "--cache-dir=" + cacheDir, "--json",
		"create", "egress-locked-sbx", "--template=python", "--allow-egress=false", "--host=host-egress-1",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("create with --allow-egress=false failed (%d): %s", code, stderr.String())
	}
	var created SandboxRecord
	if err := json.Unmarshal(stdout.Bytes(), &created); err != nil {
		t.Fatalf("decode created sandbox: %v", err)
	}
	if created.AllowEgress {
		t.Fatalf("expected AllowEgress=false on created sandbox, got true")
	}

	// 2. Verify curl/socket outbound non-loopback connection is blocked when allow_egress=false
	stdout.Reset()
	stderr.Reset()
	code = RunCLI(ctx, []string{
		"--control-plane-url=" + cpTS.URL, "--pool-url=" + poolTS.URL, "--cache-dir=" + cacheDir, "--json",
		"exec", created.SandboxID, "--", "curl", "-s", "https://example.com",
	}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("expected curl to external URL to be blocked when allow_egress=false, got exit 0")
	}

	// 3. Verify per-exec override `sbx exec --allow-egress=true` sets SBX_ALLOW_EGRESS=true
	stdout.Reset()
	stderr.Reset()
	code = RunCLI(ctx, []string{
		"--control-plane-url=" + cpTS.URL, "--pool-url=" + poolTS.URL, "--cache-dir=" + cacheDir,
		"exec", created.SandboxID, "--allow-egress=true", "--", "sh", "-c", "echo $SBX_ALLOW_EGRESS",
	}, &stdout, &stderr)
	if code != 0 || strings.TrimSpace(stdout.String()) != "true" {
		t.Fatalf("per-exec --allow-egress=true failed (code=%d): stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}

	// 4. Verify `sbx update <id> --allow-egress=true` updates AllowEgress dynamically
	stdout.Reset()
	stderr.Reset()
	code = RunCLI(ctx, []string{
		"--control-plane-url=" + cpTS.URL, "--pool-url=" + poolTS.URL, "--cache-dir=" + cacheDir, "--json",
		"update", created.SandboxID, "--allow-egress=true",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("sbx update --allow-egress=true failed (%d): %s", code, stderr.String())
	}
	var updated SandboxRecord
	if err := json.Unmarshal(stdout.Bytes(), &updated); err != nil {
		t.Fatalf("decode updated sandbox: %v", err)
	}
	if !updated.AllowEgress {
		t.Fatalf("expected AllowEgress=true after sbx update --allow-egress=true, got false")
	}
}

func TestSecurityPolicyAndSelfRevocationSafeguard(t *testing.T) {
	ctx := context.Background()
	router, cp, poolTS, cpTS, _ := newTestFleet(t, 5)
	h1 := router.getOrCreateHost("host-sec-1")
	_ = h1.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)

	uiSrv := NewUIServer(cpTS.URL, poolTS.URL, t.TempDir())

	// Fetch CSRF cookie from UI
	reqRoot := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8080/", nil)
	reqRoot.Header.Set("X-Goog-Authenticated-User-Email", "accounts.google.com:admin@example.com")
	wRoot := httptest.NewRecorder()
	uiSrv.ServeHTTP(wRoot, reqRoot)
	var csrfCookie *http.Cookie
	for _, c := range wRoot.Result().Cookies() {
		if c.Name == "sbx_csrf" {
			csrfCookie = c
			break
		}
	}
	if csrfCookie == nil {
		t.Fatalf("expected sbx_csrf cookie")
	}

	// 1. GET /api/security/policy must return IAP enabled and seed deployer (user:admin@example.com)
	reqGet := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8080/api/security/policy", nil)
	reqGet.Header.Set("X-Goog-Authenticated-User-Email", "accounts.google.com:admin@example.com")
	wGet := httptest.NewRecorder()
	uiSrv.ServeHTTP(wGet, reqGet)
	if wGet.Code != http.StatusOK {
		t.Fatalf("GET /api/security/policy status = %d, want 200: %s", wGet.Code, wGet.Body.String())
	}
	var pol SecurityPolicy
	if err := json.Unmarshal(wGet.Body.Bytes(), &pol); err != nil {
		t.Fatalf("decode SecurityPolicy: %v", err)
	}
	if !pol.IAPEnabled {
		t.Fatalf("expected IAPEnabled=true by default, got false")
	}
	if pol.CurrentMember != "user:admin@example.com" {
		t.Fatalf("CurrentMember = %q, want user:admin@example.com", pol.CurrentMember)
	}
	hasRuntimeSA := false
	var runtimeSAMember string
	for _, m := range pol.BackendAccessMembers {
		if strings.Contains(m, "-compute@developer.gserviceaccount.com") {
			hasRuntimeSA = true
			runtimeSAMember = m
			break
		}
	}
	if !hasRuntimeSA {
		t.Fatalf("expected BackendAccessMembers to include runtime service account (*-compute@developer.gserviceaccount.com), got %v", pol.BackendAccessMembers)
	}
	hasProtectedSA := false
	for _, m := range pol.ProtectedMembers {
		if m == runtimeSAMember {
			hasProtectedSA = true
			break
		}
	}
	if !hasProtectedSA {
		t.Fatalf("expected ProtectedMembers to include runtime service account %q, got %v", runtimeSAMember, pol.ProtectedMembers)
	}

	// 2. Add another principal to UI access and Backend access
	postPolicy := func(payload string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8080/api/security/policy", strings.NewReader(payload))
		req.AddCookie(csrfCookie)
		req.Header.Set("X-CSRF-Token", csrfCookie.Value)
		req.Header.Set("X-Goog-Authenticated-User-Email", "accounts.google.com:admin@example.com")
		w := httptest.NewRecorder()
		uiSrv.ServeHTTP(w, req)
		return w
	}

	wAddUI := postPolicy(`{"action":"add_ui_member","member":"teammate@example.com"}`)
	if wAddUI.Code != http.StatusOK || !strings.Contains(wAddUI.Body.String(), "user:teammate@example.com") {
		t.Fatalf("add_ui_member failed (%d): %s", wAddUI.Code, wAddUI.Body.String())
	}

	wAddBackend := postPolicy(`{"action":"add_backend_member","member":"teammate@example.com"}`)
	if wAddBackend.Code != http.StatusOK || !strings.Contains(wAddBackend.Body.String(), "user:teammate@example.com") {
		t.Fatalf("add_backend_member failed (%d): %s", wAddBackend.Code, wAddBackend.Body.String())
	}

	// 3. Removing the other member (teammate@example.com) must succeed
	wRemOther := postPolicy(`{"action":"remove_ui_member","member":"user:teammate@example.com"}`)
	if wRemOther.Code != http.StatusOK {
		t.Fatalf("remove_ui_member for teammate failed (%d): %s", wRemOther.Code, wRemOther.Body.String())
	}

	// 4. SAFEGUARD: Removing oneself (admin@example.com) from UI access MUST fail with self_revocation_forbidden!
	wSelfUI := postPolicy(`{"action":"remove_ui_member","member":"user:admin@example.com"}`)
	if wSelfUI.Code != http.StatusBadRequest && wSelfUI.Code != http.StatusForbidden {
		t.Fatalf("expected self-revocation of UI access to be blocked (400/403), got %d: %s", wSelfUI.Code, wSelfUI.Body.String())
	}
	if !strings.Contains(wSelfUI.Body.String(), "self_revocation_forbidden") {
		t.Fatalf("expected self_revocation_forbidden error code, got: %s", wSelfUI.Body.String())
	}

	// 5. SAFEGUARD: Removing oneself (admin@example.com) from Backend access MUST fail with self_revocation_forbidden!
	wSelfBackend := postPolicy(`{"action":"remove_backend_member","member":"admin@example.com"}`)
	if wSelfBackend.Code != http.StatusBadRequest && wSelfBackend.Code != http.StatusForbidden {
		t.Fatalf("expected self-revocation of Backend access to be blocked (400/403), got %d: %s", wSelfBackend.Code, wSelfBackend.Body.String())
	}
	if !strings.Contains(wSelfBackend.Body.String(), "self_revocation_forbidden") {
		t.Fatalf("expected self_revocation_forbidden error code, got: %s", wSelfBackend.Body.String())
	}

	// 5b. SAFEGUARD: Removing the backend runtime service account MUST fail with self_revocation_forbidden!
	wSABackend := postPolicy(`{"action":"remove_backend_member","member":"` + runtimeSAMember + `"}`)
	if wSABackend.Code != http.StatusBadRequest && wSABackend.Code != http.StatusForbidden {
		t.Fatalf("expected removal of runtime service account %s to be blocked (400/403), got %d: %s", runtimeSAMember, wSABackend.Code, wSABackend.Body.String())
	}
	if !strings.Contains(wSABackend.Body.String(), "self_revocation_forbidden") {
		t.Fatalf("expected self_revocation_forbidden error code when removing runtime SA, got: %s", wSABackend.Body.String())
	}

	// 6. SAFEGUARD: Replacing full policy slice without caller or without runtime SA MUST also be blocked!
	wFullReplace := postPolicy(`{"ui_access_members":["user:someone-else@example.com"]}`)
	if wFullReplace.Code != http.StatusBadRequest && wFullReplace.Code != http.StatusForbidden {
		t.Fatalf("expected full policy replacement omitting caller to be blocked, got %d: %s", wFullReplace.Code, wFullReplace.Body.String())
	}
	wFullReplaceBackendNoSA := postPolicy(`{"backend_access_members":["user:admin@example.com"]}`)
	if wFullReplaceBackendNoSA.Code != http.StatusBadRequest && wFullReplaceBackendNoSA.Code != http.StatusForbidden {
		t.Fatalf("expected full backend policy replacement omitting runtime SA to be blocked, got %d: %s", wFullReplaceBackendNoSA.Code, wFullReplaceBackendNoSA.Body.String())
	}
}

func TestAutoScaleDownEmptyHostsAndMultiHostCompact(t *testing.T) {
	ctx := context.Background()
	router, cp, poolTS, cpTS, store := newTestFleet(t, 4)
	cli := NewCLIClient(cpTS.URL, poolTS.URL, t.TempDir())

	// 1. Create a host with 1 sandbox, then delete that sandbox -> host must immediately auto-retire!
	hTemp := router.getOrCreateHost("host-ephemeral")
	_ = hTemp.SyncStateAndHeartbeat(ctx)
	var tempSbx SandboxRecord
	if _, err := cli.doJSON(ctx, http.MethodPost, poolTS.URL+"/__session/create",
		map[string]string{"X-Serverless-Session-Id": "host-ephemeral"},
		CreateSandboxRequest{Name: "temp-sbx", Template: "ubuntu"}, &tempSbx); err != nil {
		t.Fatalf("create on host-ephemeral failed: %v", err)
	}
	_ = hTemp.SyncStateAndHeartbeat(ctx)
	if sum := cp.BuildFleetSummary(); sum.ActiveHosts != 1 {
		t.Fatalf("ActiveHosts after create = %d, want 1", sum.ActiveHosts)
	}

	// Delete the sole sandbox on host-ephemeral
	var delRes map[string]any
	if _, err := cli.doJSON(ctx, http.MethodPost, poolTS.URL+"/__session/delete",
		map[string]string{"X-Serverless-Session-Id": "host-ephemeral"},
		map[string]string{"sandbox_id": tempSbx.SandboxID}, &delRes); err != nil {
		t.Fatalf("delete temp-sbx failed: %v", err)
	}
	if sum := cp.BuildFleetSummary(); sum.ActiveHosts != 0 {
		t.Fatalf("expected host-ephemeral to auto-retire immediately when ActiveSandboxes drops to 0, got ActiveHosts=%d", sum.ActiveHosts)
	}
	// Verify subsequent background heartbeat on retired empty host is a no-op and does not re-register it
	_ = hTemp.SyncStateAndHeartbeat(ctx)
	if sum := cp.BuildFleetSummary(); sum.ActiveHosts != 0 {
		t.Fatalf("retired empty host re-registered on heartbeat! ActiveHosts=%d", sum.ActiveHosts)
	}

	// 2. Verify store-deleted sandbox fencing: if sandboxes/<id>.json is deleted out-of-band in GCS,
	// the next heartbeat from that host fences the phantom sandbox and retires the empty host!
	hPhantom := router.getOrCreateHost("host-phantom")
	var phantomSbx SandboxRecord
	if _, err := cli.doJSON(ctx, http.MethodPost, poolTS.URL+"/__session/create",
		map[string]string{"X-Serverless-Session-Id": "host-phantom"},
		CreateSandboxRequest{Name: "phantom-sbx", Template: "ubuntu"}, &phantomSbx); err != nil {
		t.Fatalf("create on host-phantom failed: %v", err)
	}
	_ = store.DeleteObject(ctx, "sandboxes/"+phantomSbx.SandboxID+".json")
	_ = cp.SyncFromStore(ctx)
	_ = hPhantom.SyncStateAndHeartbeat(ctx)
	if hPhantom.HostState().ActiveSandboxes != 0 {
		t.Fatalf("expected host-phantom to fence deleted sandbox on heartbeat, still has %d active", hPhantom.HostState().ActiveSandboxes)
	}
	if sum := cp.BuildFleetSummary(); sum.ActiveHosts != 0 {
		t.Fatalf("expected host-phantom to retire after fencing its only sandbox, ActiveHosts=%d", sum.ActiveHosts)
	}

	// 3. Multi-host compaction in a SINGLE CompactFleet call:
	// Create 4 sparse hosts (host-s1: 1, host-s2: 1, host-s3: 2, host-s4: 2 = 6 sandboxes total at maxDensity=4)
	// plus 2 idle empty hosts (host-empty-1, host-empty-2).
	// Optimal host count is ceil(6 / 4) = 2 hosts.
	for _, emptyID := range []string{"host-empty-1", "host-empty-2"} {
		he := router.getOrCreateHost(emptyID)
		_ = he.SyncStateAndHeartbeat(ctx)
	}
	layout := []struct {
		hostID string
		count  int
	}{
		{"host-s1", 1},
		{"host-s2", 1},
		{"host-s3", 2},
		{"host-s4", 2},
	}
	for _, item := range layout {
		hs := router.getOrCreateHost(item.hostID)
		for i := 0; i < item.count; i++ {
			var r SandboxRecord
			if _, err := cli.doJSON(ctx, http.MethodPost, poolTS.URL+"/__session/create",
				map[string]string{"X-Serverless-Session-Id": item.hostID},
				CreateSandboxRequest{Name: item.hostID + "-sbx", Template: "ubuntu", PinnedHost: true}, &r); err != nil {
				t.Fatalf("create on %s failed: %v", item.hostID, err)
			}
		}
		_ = hs.SyncStateAndHeartbeat(ctx)
	}
	_ = cp.SyncFromStore(ctx)
	if sum := cp.BuildFleetSummary(); sum.ActiveHosts != 6 || sum.RunningSandboxes != 6 {
		t.Fatalf("before compact: ActiveHosts=%d (want 6), RunningSandboxes=%d (want 6)", sum.ActiveHosts, sum.RunningSandboxes)
	}

	// Single CompactFleet(ctx, true) call must prune the 2 empty hosts AND drain 2 sparse hosts -> leaving 2 hosts!
	compactRes, err := cp.CompactFleet(ctx, true)
	if err != nil {
		t.Fatalf("CompactFleet(true) failed: %v", err)
	}
	drained, _ := compactRes["drained_hosts"].([]string)
	retiredEmpty, _ := compactRes["retired_empty_hosts"].([]string)
	if len(retiredEmpty) != 2 {
		t.Fatalf("expected 2 retired_empty_hosts, got %v (res=%+v)", retiredEmpty, compactRes)
	}
	if len(drained) != 2 {
		t.Fatalf("expected 2 sparse hosts drained in single CompactFleet call, got %v (res=%+v)", drained, compactRes)
	}
	sumAfter := cp.BuildFleetSummary()
	if sumAfter.ActiveHosts != 2 || sumAfter.RunningSandboxes != 6 {
		t.Fatalf("after compact: ActiveHosts=%d (want 2), RunningSandboxes=%d (want 6)", sumAfter.ActiveHosts, sumAfter.RunningSandboxes)
	}
}

func TestSSEStreamingExec(t *testing.T) {
	ctx := context.Background()
	router, cp, poolTS, cpTS, _ := newTestFleet(t, 5)
	h1 := router.getOrCreateHost("host-sse-1")
	h2 := router.getOrCreateHost("host-sse-2")
	_ = h1.SyncStateAndHeartbeat(ctx)
	_ = h2.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)

	cacheDir := t.TempDir()
	cli := NewCLIClient(cpTS.URL, poolTS.URL, cacheDir)

	sbx, err := cli.CreateSandbox(ctx, CreateSandboxRequest{
		Name:         "sse-box",
		Template:     "ubuntu",
		Persistent:   true,
		TargetHostID: "host-sse-1",
	})
	if err != nil {
		t.Fatalf("CreateSandbox(sse-box) failed: %v", err)
	}

	// 1. Verify real-time incremental SSE chunk delivery before command finishes
	var events []ExecStreamEvent
	var chunkTimes []time.Time
	var mu sync.Mutex
	resp, err := cli.ExecStream(ctx, ExecRequest{
		SandboxID: sbx.SandboxID,
		Command: []string{"sh", "-c",
			"echo chunk-one; sleep 0.15; echo err-line >&2; sleep 0.15; echo chunk-two"},
		Stream: true,
	}, func(ev ExecStreamEvent) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, ev)
		if ev.Type == "stdout" {
			chunkTimes = append(chunkTimes, time.Now())
		}
	})
	if err != nil {
		t.Fatalf("ExecStream failed: %v", err)
	}
	if resp.ExitCode != 0 {
		t.Fatalf("ExecStream ExitCode = %d, want 0", resp.ExitCode)
	}
	if !strings.Contains(resp.Stdout, "chunk-one") || !strings.Contains(resp.Stdout, "chunk-two") {
		t.Fatalf("unexpected aggregated Stdout: %q", resp.Stdout)
	}
	if !strings.Contains(resp.Stderr, "err-line") {
		t.Fatalf("unexpected aggregated Stderr: %q", resp.Stderr)
	}
	if len(chunkTimes) >= 2 && chunkTimes[len(chunkTimes)-1].Sub(chunkTimes[0]) < 80*time.Millisecond {
		t.Fatalf("expected incremental streaming delay >= 80ms between first and last stdout chunks, got %v",
			chunkTimes[len(chunkTimes)-1].Sub(chunkTimes[0]))
	}
	hasStart, hasExit := false, false
	for _, ev := range events {
		if ev.Type == "start" {
			hasStart = true
		}
		if ev.Type == "exit" {
			hasExit = true
		}
	}
	if !hasStart || !hasExit {
		t.Fatalf("expected both start and exit SSE events, got %+v", events)
	}

	// 2. Verify CLI `sbx exec --stream` streams stdout/stderr and returns the command's non-zero exit code
	var outBuf, errBuf bytes.Buffer
	code := RunCLI(ctx, []string{
		"--control-plane-url=" + cpTS.URL, "--pool-url=" + poolTS.URL, "--cache-dir=" + cacheDir,
		"exec", "--stream", sbx.SandboxID, "--", "sh", "-c", "echo stream-cli-out && echo stream-cli-err >&2 && exit 7",
	}, &outBuf, &errBuf)
	if code != 7 {
		t.Fatalf("sbx exec --stream exit code = %d, want 7 (stderr=%s)", code, errBuf.String())
	}
	if strings.TrimSpace(outBuf.String()) != "stream-cli-out" {
		t.Fatalf("sbx exec --stream stdout = %q, want 'stream-cli-out'", outBuf.String())
	}
	if !strings.Contains(errBuf.String(), "stream-cli-err") {
		t.Fatalf("sbx exec --stream stderr = %q, want to contain 'stream-cli-err'", errBuf.String())
	}

	// 3. Verify streaming timeout returns ExitCode 124
	toResp, err := cli.ExecStream(ctx, ExecRequest{
		SandboxID:  sbx.SandboxID,
		TimeoutSec: 1,
		Command:    []string{"sh", "-c", "echo before-timeout && sleep 10"},
		Stream:     true,
	}, nil)
	if err != nil {
		t.Fatalf("timeout ExecStream returned transport error: %v", err)
	}
	if toResp.ExitCode != 124 {
		t.Fatalf("timeout ExecStream ExitCode = %d, want 124", toResp.ExitCode)
	}

	// 4. Verify HTTP 409 route self-healing on ExecStream after live migration to host-sse-2
	cliStale := NewCLIClient(cpTS.URL, poolTS.URL, t.TempDir())
	cliStale.SetRoute(sbx.SandboxID, "host-sse-1")

	var migRec SandboxRecord
	if err := cli.CallSandboxEndpoint(ctx, http.MethodPost, "/__session/migrate", sbx.SandboxID, MigrateSandboxRequest{
		SandboxID:    sbx.SandboxID,
		TargetHostID: "host-sse-2",
	}, &migRec); err != nil {
		t.Fatalf("migrate to host-sse-2 failed: %v", err)
	}

	healedResp, err := cliStale.ExecStream(ctx, ExecRequest{
		SandboxID: sbx.SandboxID,
		Command:   []string{"sh", "-c", "echo healed-stream-ok"},
		Stream:    true,
	}, nil)
	if err != nil {
		t.Fatalf("cliStale.ExecStream after migration failed: %v", err)
	}
	if strings.TrimSpace(healedResp.Stdout) != "healed-stream-ok" || healedResp.HostID != "host-sse-2" {
		t.Fatalf("unexpected healed ExecStream response: %+v", healedResp)
	}

	// 5. Verify ExecStream auto-resumes a SUSPENDED sandbox
	var suspRec SandboxRecord
	if err := cli.CallSandboxEndpoint(ctx, http.MethodPost, "/__session/suspend", sbx.SandboxID, map[string]string{"sandbox_id": sbx.SandboxID}, &suspRec); err != nil {
		t.Fatalf("suspend failed: %v", err)
	}
	resumedStream, err := cli.ExecStream(ctx, ExecRequest{
		SandboxID: sbx.SandboxID,
		Command:   []string{"sh", "-c", "echo resumed-via-stream"},
		Stream:    true,
	}, nil)
	if err != nil || strings.TrimSpace(resumedStream.Stdout) != "resumed-via-stream" {
		t.Fatalf("ExecStream on suspended sandbox failed: resp=%+v err=%v", resumedStream, err)
	}
}

func TestMain(m *testing.M) {
	if portStr := strings.TrimSpace(os.Getenv("SBX_TEST_HTTP_SERVER_PORT")); portStr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-App-Port", portStr)
			if r.Method == http.MethodPost {
				raw, _ := io.ReadAll(r.Body)
				w.WriteHeader(http.StatusCreated)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"method": "POST",
					"path":   r.URL.RequestURI(),
					"echo":   string(raw),
				})
				return
			}
			if r.URL.Path == "/redirect" {
				http.Redirect(w, r, "/login", http.StatusFound)
				return
			}
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"method":     r.Method,
				"path":       r.URL.RequestURI(),
				"custom_hdr": r.Header.Get("X-Test-Header"),
				"headers":    r.Header,
			})
		})
		_ = http.ListenAndServe("127.0.0.1:"+portStr, mux)
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestPreviewURLProxyAndPortForward(t *testing.T) {
	ctx := context.Background()
	router, cp, poolTS, cpTS, _ := newTestFleet(t, 5)
	h1 := router.getOrCreateHost("host-web-1")
	h2 := router.getOrCreateHost("host-web-2")
	_ = h1.SyncStateAndHeartbeat(ctx)
	_ = h2.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)

	uiSrv := NewUIServer(cpTS.URL, poolTS.URL, t.TempDir())
	uiTS := httptest.NewServer(uiSrv)
	t.Cleanup(uiTS.Close)

	cacheDir := t.TempDir()
	cli := NewCLIClient(cpTS.URL, poolTS.URL, cacheDir)

	// Pick an available local port for the test HTTP server inside the sandbox
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen failed: %v", err)
	}
	appPort := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

	sbx, err := cli.CreateSandbox(ctx, CreateSandboxRequest{
		Name:         "web-preview-sbx",
		Template:     "python",
		Persistent:   true,
		TargetHostID: "host-web-1",
	})
	if err != nil {
		t.Fatalf("CreateSandbox(web-preview-sbx) failed: %v", err)
	}

	testBin, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable failed: %v", err)
	}

	if _, err := cli.Exec(ctx, ExecRequest{
		SandboxID:  sbx.SandboxID,
		Background: true,
		Command:    []string{"sh", "-c", fmt.Sprintf("SBX_TEST_HTTP_SERVER_PORT=%d exec %s", appPort, shellQuote(testBin))},
	}); err != nil {
		t.Fatalf("start background test HTTP server failed: %v", err)
	}
	_ = h1.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)

	// 1. Test HTTP GET and POST through Control Plane `/proxy/<sandbox_id>/<port>/...` and UI `/proxy/<name>/<port>/...`
	for _, baseURL := range []string{cpTS.URL, uiTS.URL} {
		reqGet, _ := http.NewRequestWithContext(ctx, http.MethodGet,
			fmt.Sprintf("%s/proxy/%s/%d/hello/world?foo=bar", baseURL, sbx.SandboxID, appPort), nil)
		reqGet.Header.Set("X-Test-Header", "preview-ok")
		respGet, err := http.DefaultClient.Do(reqGet)
		if err != nil {
			t.Fatalf("GET preview via %s failed: %v", baseURL, err)
		}
		rawGet, _ := io.ReadAll(respGet.Body)
		respGet.Body.Close()
		if respGet.StatusCode != http.StatusOK {
			logs, _ := h1.engine.ReadLogs(ctx, sbx.SandboxID)
			t.Fatalf("GET preview via %s status = %d, want 200 (body=%s, logs=%s)", baseURL, respGet.StatusCode, string(rawGet), logs)
		}
		if !strings.Contains(string(rawGet), `"/hello/world?foo=bar"`) || !strings.Contains(string(rawGet), `"preview-ok"`) {
			t.Fatalf("unexpected GET preview body from %s: %s", baseURL, string(rawGet))
		}
	}

	// Test POST by human-readable sandbox name (`web-preview-sbx`)
	respPost, err := http.Post(
		fmt.Sprintf("%s/proxy/web-preview-sbx/%d/submit", cpTS.URL, appPort),
		"text/plain",
		strings.NewReader("payload-123"),
	)
	if err != nil {
		t.Fatalf("POST preview by name failed: %v", err)
	}
	rawPost, _ := io.ReadAll(respPost.Body)
	respPost.Body.Close()
	if respPost.StatusCode != http.StatusCreated || !strings.Contains(string(rawPost), `"payload-123"`) {
		t.Fatalf("unexpected POST preview response (%d): %s", respPost.StatusCode, string(rawPost))
	}

	// 2. Test Wake-on-HTTP-Request: suspend `web-preview-sbx`, then send GET /proxy/<id>/<port>/woken
	var suspRec SandboxRecord
	if err := cli.CallSandboxEndpoint(ctx, http.MethodPost, "/__session/suspend", sbx.SandboxID, map[string]string{"sandbox_id": sbx.SandboxID}, &suspRec); err != nil {
		t.Fatalf("suspend web-preview-sbx failed: %v", err)
	}
	if suspRec.Status != StatusSuspended {
		t.Fatalf("expected StatusSuspended, got %s", suspRec.Status)
	}
	_ = cp.SyncFromStore(ctx)

	respWake, err := http.Get(fmt.Sprintf("%s/proxy/%s/%d/woken-by-http", cpTS.URL, sbx.SandboxID, appPort))
	if err != nil {
		t.Fatalf("GET preview on suspended sandbox failed: %v", err)
	}
	rawWake, _ := io.ReadAll(respWake.Body)
	respWake.Body.Close()
	if respWake.StatusCode != http.StatusOK || !strings.Contains(string(rawWake), `"/woken-by-http"`) {
		t.Fatalf("Wake-on-HTTP-Request failed (%d): %s", respWake.StatusCode, string(rawWake))
	}

	// 3. Test CLI `sbx preview-url`
	var outBuf, errBuf bytes.Buffer
	if code := RunCLI(ctx, []string{
		"--control-plane-url=" + cpTS.URL, "--pool-url=" + poolTS.URL, "--cache-dir=" + cacheDir,
		"preview-url", "web-preview-sbx", "--port=" + strconv.Itoa(appPort), "--path=/hello",
	}, &outBuf, &errBuf); code != 0 {
		t.Fatalf("sbx preview-url failed (code=%d): %s", code, errBuf.String())
	}
	wantSubstr := fmt.Sprintf("/proxy/%s/%d/hello", sbx.SandboxID, appPort)
	if !strings.Contains(outBuf.String(), wantSubstr) {
		t.Fatalf("expected preview-url output to contain %q, got %q", wantSubstr, outBuf.String())
	}
}

func TestAutoSuspendOnNoTrafficAndPostgresScaleToZero(t *testing.T) {
	ctx := context.Background()
	router, cp, poolTS, cpTS, _ := newTestFleet(t, 5)
	h1 := router.getOrCreateHost("host-scale0-1")
	_ = h1.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)

	cacheDir := t.TempDir()
	cli := NewCLIClient(cpTS.URL, poolTS.URL, cacheDir)

	// 1. Verify auto-suspend is OFF by default (DefaultIdleTimeout == 0, IdleTimeoutSec == 0)
	if cp.config.DefaultIdleTimeout != 0 {
		t.Fatalf("expected DefaultIdleTimeout == 0 (off by default), got %d", cp.config.DefaultIdleTimeout)
	}
	defaultSbx, err := cli.CreateSandbox(ctx, CreateSandboxRequest{
		Name:         "default-no-suspend",
		Template:     "ubuntu",
		TargetHostID: "host-scale0-1",
	})
	if err != nil {
		t.Fatalf("CreateSandbox(default-no-suspend) failed: %v", err)
	}
	if defaultSbx.IdleTimeoutSec != 0 {
		t.Fatalf("expected default IdleTimeoutSec == 0, got %d", defaultSbx.IdleTimeoutSec)
	}

	// 2. Configure fleet-wide default_idle_timeout_sec = 2 via `sbx config set --default-idle-timeout=2`
	var outBuf, errBuf bytes.Buffer
	if code := RunCLI(ctx, []string{
		"--control-plane-url=" + cpTS.URL, "--pool-url=" + poolTS.URL, "--cache-dir=" + cacheDir,
		"config", "set", "--default-idle-timeout=2",
	}, &outBuf, &errBuf); code != 0 {
		t.Fatalf("sbx config set --default-idle-timeout=2 failed: %s", errBuf.String())
	}
	_ = h1.SyncStateAndHeartbeat(ctx)

	// New sandbox without explicit --idle-timeout must inherit fleet default (2s) and automatically enable Persistent=true!
	inheritedSbx, err := cli.CreateSandbox(ctx, CreateSandboxRequest{
		Name:         "inherited-idle-sbx",
		Template:     "ubuntu",
		TargetHostID: "host-scale0-1",
	})
	if err != nil {
		t.Fatalf("CreateSandbox(inherited-idle-sbx) failed: %v", err)
	}
	if inheritedSbx.IdleTimeoutSec != 2 || !inheritedSbx.Persistent {
		t.Fatalf("expected inherited IdleTimeoutSec=2 and Persistent=true, got IdleTimeoutSec=%d Persistent=%v",
			inheritedSbx.IdleTimeoutSec, inheritedSbx.Persistent)
	}

	// Explicit `--idle-timeout=0` on `sbx create` must override the fleet default (DisableAutoSuspend=true)!
	outBuf.Reset()
	errBuf.Reset()
	if code := RunCLI(ctx, []string{
		"--control-plane-url=" + cpTS.URL, "--pool-url=" + poolTS.URL, "--cache-dir=" + cacheDir, "--json",
		"create", "override-zero-sbx", "--idle-timeout=0", "--host=host-scale0-1",
	}, &outBuf, &errBuf); code != 0 {
		t.Fatalf("sbx create --idle-timeout=0 failed: %s", errBuf.String())
	}
	var overrideSbx SandboxRecord
	if err := json.Unmarshal(outBuf.Bytes(), &overrideSbx); err != nil {
		t.Fatalf("decode override-zero-sbx: %v", err)
	}
	if overrideSbx.IdleTimeoutSec != 0 || !overrideSbx.DisableAutoSuspend {
		t.Fatalf("expected explicit --idle-timeout=0 to set IdleTimeoutSec=0 and DisableAutoSuspend=true, got %+v", overrideSbx)
	}

	// Reset fleet default to 0 and delete the test ubuntu sandboxes so we can test Postgres scale-to-0 on a clean host
	_, _ = cli.doJSON(ctx, http.MethodPost, cpTS.URL+"/v1/config", nil, map[string]any{"default_idle_timeout_sec": 0}, nil)
	for _, id := range []string{defaultSbx.SandboxID, inheritedSbx.SandboxID, overrideSbx.SandboxID} {
		var del map[string]any
		_ = cli.CallSandboxEndpoint(ctx, http.MethodPost, "/__session/delete", id, map[string]string{"sandbox_id": id}, &del)
	}

	// 3. Create a Postgres sandbox with `--auto-suspend=1` (1s idle timeout) on host-pg-zero
	hPg := router.getOrCreateHost("host-pg-zero")
	_ = hPg.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)

	outBuf.Reset()
	errBuf.Reset()
	if code := RunCLI(ctx, []string{
		"--control-plane-url=" + cpTS.URL, "--pool-url=" + poolTS.URL, "--cache-dir=" + cacheDir, "--json",
		"create", "pg-serverless", "--template=postgres", "--auto-suspend=1", "--host=host-pg-zero",
	}, &outBuf, &errBuf); code != 0 {
		t.Fatalf("create pg-serverless --auto-suspend=1 failed: %s", errBuf.String())
	}
	var pgSbx SandboxRecord
	if err := json.Unmarshal(outBuf.Bytes(), &pgSbx); err != nil {
		t.Fatalf("decode pg-serverless: %v", err)
	}
	if pgSbx.IdleTimeoutSec != 1 || !pgSbx.Persistent {
		t.Fatalf("expected pg-serverless IdleTimeoutSec=1 and Persistent=true, got %+v", pgSbx)
	}

	// Write committed database state into /pgdata/data/ledger.sql
	if _, err := cli.Exec(ctx, ExecRequest{
		SandboxID: pgSbx.SandboxID,
		Command:   []string{"sh", "-c", "echo 'INSERT INTO accounts VALUES (1, 5000);' > /pgdata/data/ledger.sql"},
	}); err != nil {
		t.Fatalf("write pgdata state failed: %v", err)
	}

	// 4. Verify active in-flight stream prevents auto-suspend even after 1.1s!
	streamDone := make(chan error, 1)
	go func() {
		_, sErr := cli.ExecStream(ctx, ExecRequest{
			SandboxID: pgSbx.SandboxID,
			Command:   []string{"sh", "-c", "sleep 1.4 && echo stream-finished"},
			Stream:    true,
		}, nil)
		streamDone <- sErr
	}()
	time.Sleep(1150 * time.Millisecond)
	// While ExecStream is in flight (activeConns > 0), checkInGuestAndIdleTimeouts MUST NOT suspend pgSbx!
	hPg.checkInGuestAndIdleTimeouts(ctx)
	if hPg.HostState().ActiveSandboxes != 1 {
		t.Fatalf("expected active in-flight stream to prevent auto-suspend, but ActiveSandboxes=%d", hPg.HostState().ActiveSandboxes)
	}
	if sErr := <-streamDone; sErr != nil {
		t.Fatalf("in-flight ExecStream failed: %v", sErr)
	}

	// 5. Now wait 1.2s with ZERO traffic and run checkInGuestAndIdleTimeouts -> pgSbx must auto-suspend AND host-pg-zero must scale to 0!
	time.Sleep(1200 * time.Millisecond)
	hPg.checkInGuestAndIdleTimeouts(ctx)
	_ = cp.SyncFromStore(ctx)
	if hPg.HostState().ActiveSandboxes != 0 {
		t.Fatalf("expected pg-serverless to auto-suspend after 1.2s of no traffic, ActiveSandboxes=%d", hPg.HostState().ActiveSandboxes)
	}
	if sum := cp.BuildFleetSummary(); sum.ActiveHosts != 0 || sum.SuspendedSandboxes < 1 {
		t.Fatalf("expected fleet to scale down to 0 active hosts when pg-serverless auto-suspended, got ActiveHosts=%d SuspendedSandboxes=%d",
			sum.ActiveHosts, sum.SuspendedSandboxes)
	}

	// 6. Send a new request to pg-serverless -> must automatically wake up from SUSPENDED and preserve /pgdata/data/ledger.sql (RPO=0)!
	wakeResp, err := cli.Exec(ctx, ExecRequest{
		SandboxID: pgSbx.SandboxID,
		Command:   []string{"sh", "-c", "cat /pgdata/data/ledger.sql"},
	})
	if err != nil {
		t.Fatalf("Exec on auto-suspended pg-serverless failed: %v", err)
	}
	if strings.TrimSpace(wakeResp.Stdout) != "INSERT INTO accounts VALUES (1, 5000);" {
		t.Fatalf("pgdata after auto-suspend + wake = %q, want 'INSERT INTO accounts VALUES (1, 5000);'", wakeResp.Stdout)
	}
}

func TestRound1QAFixes(t *testing.T) {
	ctx := context.Background()
	router, cp, poolTS, cpTS, _ := newTestFleet(t, 5)
	h1 := router.getOrCreateHost("host-qa-1")
	_ = h1.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)

	uiSrv := NewUIServer(cpTS.URL, poolTS.URL, t.TempDir())
	uiTS := httptest.NewServer(uiSrv)
	t.Cleanup(uiTS.Close)

	cacheDir := t.TempDir()
	cli := NewCLIClient(cpTS.URL, poolTS.URL, cacheDir)

	// 1. Create sandbox with Go duration string `--auto-suspend=2m` and verify JSON always includes `"idle_timeout_sec"`
	var outBuf, errBuf bytes.Buffer
	if code := RunCLI(ctx, []string{
		"--control-plane-url=" + cpTS.URL, "--pool-url=" + poolTS.URL, "--cache-dir=" + cacheDir, "--json",
		"create", "qa-sbx", "--template=python", "--auto-suspend=2m", "--host=host-qa-1",
	}, &outBuf, &errBuf); code != 0 {
		t.Fatalf("sbx create --auto-suspend=2m failed: %s", errBuf.String())
	}
	var sbx SandboxRecord
	if err := json.Unmarshal(outBuf.Bytes(), &sbx); err != nil {
		t.Fatalf("decode qa-sbx: %v", err)
	}
	if sbx.IdleTimeoutSec != 120 || !sbx.Persistent {
		t.Fatalf("expected IdleTimeoutSec=120 and Persistent=true from --auto-suspend=2m, got %+v", sbx)
	}

	// 2. Test `--no-auto-suspend` on `sbx update` and verify `"idle_timeout_sec": 0` is present in raw JSON
	outBuf.Reset()
	errBuf.Reset()
	if code := RunCLI(ctx, []string{
		"--control-plane-url=" + cpTS.URL, "--pool-url=" + poolTS.URL, "--cache-dir=" + cacheDir, "--json",
		"update", sbx.SandboxID, "--no-auto-suspend",
	}, &outBuf, &errBuf); code != 0 {
		t.Fatalf("sbx update --no-auto-suspend failed: %s", errBuf.String())
	}
	if !strings.Contains(outBuf.String(), `"idle_timeout_sec":0`) && !strings.Contains(outBuf.String(), `"idle_timeout_sec": 0`) {
		t.Fatalf("expected raw JSON to contain idle_timeout_sec: 0, got: %s", outBuf.String())
	}

	// 3. Test SSRF protection in LocalProcessEngine.DialSandboxPort:
	//    (a) Rejects privileged port < 1024 (e.g. port 22)
	if _, err := h1.engine.DialSandboxPort(ctx, sbx, 22); err == nil || !strings.Contains(err.Error(), "privileged port") {
		t.Fatalf("expected DialSandboxPort(22) to fail with privileged port error, got: %v", err)
	}
	//    (b) Rejects non-sandbox localhost listener (e.g., cpTS's listening port owned by the test process)
	cpAddr := strings.TrimPrefix(cpTS.URL, "http://127.0.0.1:")
	cpPort, _ := strconv.Atoi(cpAddr)
	if cpPort >= 1024 {
		respSSRF, err := http.Get(fmt.Sprintf("%s/proxy/%s/%d/v1/config", cpTS.URL, sbx.SandboxID, cpPort))
		if err != nil {
			t.Fatalf("GET SSRF probe failed: %v", err)
		}
		respSSRF.Body.Close()
		if respSSRF.StatusCode != http.StatusBadGateway {
			t.Fatalf("expected SSRF probe to host listener port %d to be rejected with 502 Bad Gateway, got %d", cpPort, respSSRF.StatusCode)
		}
	}

	// 4. Test HTTP 413 Request Entity Too Large when proxy request body exceeds 16 MiB
	largeBody := bytes.Repeat([]byte("A"), (16<<20)+1024)
	for _, baseURL := range []string{cpTS.URL, uiTS.URL} {
		resp413, err := http.Post(
			fmt.Sprintf("%s/proxy/%s/8080/upload", baseURL, sbx.SandboxID),
			"application/octet-stream",
			bytes.NewReader(largeBody),
		)
		if err != nil {
			t.Fatalf("POST >16MiB to %s failed: %v", baseURL, err)
		}
		resp413.Body.Close()
		if resp413.StatusCode != http.StatusRequestEntityTooLarge {
			t.Fatalf("expected HTTP 413 Request Entity Too Large from %s, got %d", baseURL, resp413.StatusCode)
		}
	}

	// 5. Test `sbx exec` with `-h`/`--help` after `<sandbox_id>` passes flag to command, and `--stream --background` fails
	outBuf.Reset()
	errBuf.Reset()
	if code := RunCLI(ctx, []string{
		"--control-plane-url=" + cpTS.URL, "--pool-url=" + poolTS.URL, "--cache-dir=" + cacheDir,
		"exec", sbx.SandboxID, "echo", "--help", "-h",
	}, &outBuf, &errBuf); code != 0 {
		t.Fatalf("sbx exec echo --help -h failed: %s", errBuf.String())
	}
	if strings.TrimSpace(outBuf.String()) != "--help -h" {
		t.Fatalf("expected stdout '--help -h', got %q", outBuf.String())
	}
	outBuf.Reset()
	errBuf.Reset()
	if code := RunCLI(ctx, []string{
		"--control-plane-url=" + cpTS.URL, "--pool-url=" + poolTS.URL, "--cache-dir=" + cacheDir,
		"exec", "--stream", "--background", sbx.SandboxID, "--", "echo", "hi",
	}, &outBuf, &errBuf); code == 0 || !strings.Contains(errBuf.String(), "--stream cannot be combined with --background") {
		t.Fatalf("expected --stream --background to fail with explicit error, got code=%d stderr=%q", code, errBuf.String())
	}

	// 6. Test `/workspace/.sbx_init.sh` autostart on resume AND `handleUpdate` on a SUSPENDED sandbox not leaking into h1.sandboxes
	if _, err := cli.Exec(ctx, ExecRequest{
		SandboxID: sbx.SandboxID,
		Command:   []string{"sh", "-c", "echo 'echo init-ran >> /workspace/init_marker.txt' > /workspace/.sbx_init.sh && chmod +x /workspace/.sbx_init.sh"},
	}); err != nil {
		t.Fatalf("create .sbx_init.sh failed: %v", err)
	}
	var suspRec SandboxRecord
	if err := cli.CallSandboxEndpoint(ctx, http.MethodPost, "/__session/suspend", sbx.SandboxID, map[string]string{"sandbox_id": sbx.SandboxID}, &suspRec); err != nil {
		t.Fatalf("suspend qa-sbx failed: %v", err)
	}
	if h1.HostState().ActiveSandboxes != 0 {
		t.Fatalf("expected 0 ActiveSandboxes after suspend, got %d", h1.HostState().ActiveSandboxes)
	}

	// Updating a SUSPENDED sandbox must NOT leak it into h1.sandboxes (ActiveSandboxes must stay 0)!
	outBuf.Reset()
	errBuf.Reset()
	if code := RunCLI(ctx, []string{
		"--control-plane-url=" + cpTS.URL, "--pool-url=" + poolTS.URL, "--cache-dir=" + cacheDir,
		"update", sbx.SandboxID, "--idle-timeout=45s", "--memory=2Gi",
	}, &outBuf, &errBuf); code != 0 {
		t.Fatalf("sbx update on SUSPENDED sandbox failed: %s", errBuf.String())
	}
	if h1.HostState().ActiveSandboxes != 0 {
		t.Fatalf("expected ActiveSandboxes to remain 0 after updating SUSPENDED sandbox, got %d", h1.HostState().ActiveSandboxes)
	}

	// Re-register h1 as active so auto-resume can place onto h1, then run Exec to trigger auto-resume + .sbx_init.sh
	_ = h1.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)
	initCheck, err := cli.Exec(ctx, ExecRequest{
		SandboxID: sbx.SandboxID,
		Command:   []string{"sh", "-c", "for i in 1 2 3 4 5; do [ -f /workspace/init_marker.txt ] && break; sleep 0.1; done; cat /workspace/init_marker.txt"},
	})
	if err != nil || strings.TrimSpace(initCheck.Stdout) != "init-ran" {
		t.Fatalf("expected /workspace/.sbx_init.sh to run on resume and write 'init-ran', got resp=%+v err=%v", initCheck, err)
	}

	// 7. Test `sbx preview-url` port validation and `sbx port-forward` through ControlPlaneURL / PoolServiceURL
	outBuf.Reset()
	errBuf.Reset()
	if code := RunCLI(ctx, []string{
		"--control-plane-url=" + cpTS.URL, "--pool-url=" + poolTS.URL, "--cache-dir=" + cacheDir,
		"preview-url", sbx.SandboxID, "--port=70000",
	}, &outBuf, &errBuf); code == 0 {
		t.Fatalf("expected preview-url --port=70000 to fail")
	}

	// Start test HTTP server inside qa-sbx and test `forwardLocalConnToSandbox` raw TCP tunnel
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen failed: %v", err)
	}
	appPort := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	testBin, _ := os.Executable()
	if _, err := cli.Exec(ctx, ExecRequest{
		SandboxID:  sbx.SandboxID,
		Background: true,
		Command:    []string{"sh", "-c", fmt.Sprintf("SBX_TEST_HTTP_SERVER_PORT=%d exec %s", appPort, shellQuote(testBin))},
	}); err != nil {
		t.Fatalf("start background server for port-forward failed: %v", err)
	}

	cSide, sSide := net.Pipe()
	defer cSide.Close()
	go forwardLocalConnToSandbox(ctx, cli, sbx.SandboxID, appPort, sSide)
	_, _ = cSide.Write([]byte("GET /tunneled HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
	tunneledBytes, _ := io.ReadAll(cSide)
	if !strings.Contains(string(tunneledBytes), `"/tunneled"`) {
		t.Fatalf("expected raw TCP port-forward tunnel to return '/tunneled', got: %s", string(tunneledBytes))
	}
}

func TestEmbeddedDeployAssetsAndCloudShellProjectDetection(t *testing.T) {
	t.Setenv("GOOGLE_CLOUD_PROJECT", "")
	t.Setenv("GCLOUD_PROJECT", "")
	t.Setenv("DEVSHELL_PROJECT_ID", "cloudshell-test-proj")
	if got := DefaultGCPProject(); got != "cloudshell-test-proj" {
		t.Fatalf("expected DefaultGCPProject() to detect DEVSHELL_PROJECT_ID=cloudshell-test-proj, got %q", got)
	}
	if got := resolveCLIProject(context.Background(), ""); got != "cloudshell-test-proj" {
		t.Fatalf("expected resolveCLIProject() to detect DEVSHELL_PROJECT_ID=cloudshell-test-proj, got %q", got)
	}
	if got := resolveCLIProject(context.Background(), "explicit-proj"); got != "explicit-proj" {
		t.Fatalf("expected resolveCLIProject('explicit-proj') to return 'explicit-proj', got %q", got)
	}

	tmpDir, err := extractEmbeddedDeployAssets()
	if err != nil {
		t.Fatalf("extractEmbeddedDeployAssets failed: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	requiredFiles := []string{
		"go.mod", "main.go", "dispenser.go", "runtime.go", "hostd.go", "controld.go", "cli.go", "ui.go",
		"Dockerfile", "deploy.sh", "service-control-plane.yaml", "service-pool.yaml", "service-ui.yaml",
	}
	for _, f := range requiredFiles {
		info, err := os.Stat(filepath.Join(tmpDir, f))
		if err != nil {
			t.Fatalf("expected embedded file %q to be extracted in %s: %v", f, tmpDir, err)
		}
		if info.Size() == 0 {
			t.Fatalf("expected embedded file %q to be non-empty", f)
		}
		if f == "deploy.sh" && info.Mode().Perm()&0111 == 0 {
			t.Fatalf("expected extracted deploy.sh to be executable, mode=%v", info.Mode())
		}
	}
}

// TestTwoPhasePreCopyLiveMigration verifies Two-Phase Pre-Copy live migration:
//   - Phase 1 (POST /__internal/prestage) copies the base filesystem to the target host while the
//     sandbox keeps RUNNING and serving exec traffic on the source host (zero downtime).
//   - Phase 2 freezes the sandbox briefly, streams only the delta (modified/new files plus
//     deleted paths) directly to the target in POST /__internal/hydrate (Prestaged=true), and cuts over.
func TestTwoPhasePreCopyLiveMigration(t *testing.T) {
	ctx := context.Background()
	router, cp, poolTS, cpTS, store := newTestFleet(t, 5)
	hSrc := router.getOrCreateHost("host-precopy-src")
	hDst := router.getOrCreateHost("host-precopy-dst")
	_ = hSrc.SyncStateAndHeartbeat(ctx)
	_ = hDst.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)

	cli := NewCLIClient(cpTS.URL, poolTS.URL, t.TempDir())
	var sbx SandboxRecord
	if _, err := cli.doJSON(ctx, http.MethodPost, poolTS.URL+"/__session/create",
		map[string]string{"X-Serverless-Session-Id": "host-precopy-src"},
		CreateSandboxRequest{Name: "precopy-1", Template: "ubuntu", Persistent: true}, &sbx); err != nil {
		t.Fatalf("create precopy-1 failed: %v", err)
	}
	if sbx.HostID != "host-precopy-src" {
		t.Fatalf("precopy-1 created on %s, want host-precopy-src", sbx.HostID)
	}
	cli.SetRoute(sbx.SandboxID, sbx.HostID)

	// (a) Base files that are copied during Phase 1 (pre-copy).
	if resp, err := cli.Exec(ctx, ExecRequest{
		SandboxID: sbx.SandboxID,
		Command: []string{"sh", "-c", "echo base-content > /workspace/base.txt && echo delta-v1 > /workspace/delta.txt && " +
			"echo bye > /workspace/gone.txt && mkdir -p /workspace/olddir && echo old > /workspace/olddir/f.txt"},
	}); err != nil || resp.ExitCode != 0 {
		t.Fatalf("seed base files failed: resp=%+v err=%v", resp, err)
	}

	// (b) Mutate the filesystem AFTER Phase 1 completed (and before the freeze): the sandbox must
	// still be RUNNING on the source host and the target must hold a warm prestaged copy.
	var hookMu sync.Mutex
	hookCalls := 0
	var hookErr error
	hSrc.testHookAfterPrestage = func(sandboxID string) {
		hookMu.Lock()
		defer hookMu.Unlock()
		hookCalls++
		hSrc.mu.RLock()
		srcStatus := hSrc.sandboxes[sandboxID].Status
		hSrc.mu.RUnlock()
		if srcStatus != StatusRunning {
			hookErr = fmt.Errorf("source status during pre-copy = %s, want RUNNING (zero downtime)", srcStatus)
			return
		}
		hDst.mu.RLock()
		_, staged := hDst.prestaged[sandboxID]
		_, active := hDst.sandboxes[sandboxID]
		hDst.mu.RUnlock()
		if !staged || active {
			hookErr = fmt.Errorf("target prestaged=%v active=%v after Phase 1, want prestaged=true active=false", staged, active)
			return
		}
		resp, err := cli.Exec(ctx, ExecRequest{
			SandboxID: sandboxID,
			Command: []string{"sh", "-c", "echo delta-v2 > /workspace/delta.txt && rm -f /workspace/gone.txt && " +
				"rm -rf /workspace/olddir && mkdir -p /workspace/newdir && echo fresh > /workspace/newdir/new.txt"},
		})
		if err != nil || resp.ExitCode != 0 || resp.HostID != "host-precopy-src" {
			hookErr = fmt.Errorf("mid-precopy exec failed: resp=%+v err=%v", resp, err)
		}
	}

	// (c) Concurrent exec traffic (reads + writes) during the whole migration.
	stopTraffic := make(chan struct{})
	var trafficWG sync.WaitGroup
	var trafficMu sync.Mutex
	writesOK := map[int]string{}
	var trafficErrs []string
	readsOK := 0
	trafficWG.Add(2)
	go func() {
		defer trafficWG.Done()
		trafficCLI := NewCLIClient(cpTS.URL, poolTS.URL, t.TempDir())
		trafficCLI.SetRoute(sbx.SandboxID, "host-precopy-src")
		for i := 0; ; i++ {
			select {
			case <-stopTraffic:
				return
			default:
			}
			resp, err := trafficCLI.Exec(ctx, ExecRequest{
				SandboxID: sbx.SandboxID,
				Command:   []string{"sh", "-c", fmt.Sprintf("echo w%d > /workspace/conc-%d.txt", i, i)},
			})
			trafficMu.Lock()
			if err != nil || resp.ExitCode != 0 {
				trafficErrs = append(trafficErrs, fmt.Sprintf("write %d: resp=%+v err=%v", i, resp, err))
			} else {
				writesOK[i] = resp.HostID
			}
			trafficMu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
	}()
	go func() {
		defer trafficWG.Done()
		trafficCLI := NewCLIClient(cpTS.URL, poolTS.URL, t.TempDir())
		trafficCLI.SetRoute(sbx.SandboxID, "host-precopy-src")
		for {
			select {
			case <-stopTraffic:
				return
			default:
			}
			resp, err := trafficCLI.Exec(ctx, ExecRequest{
				SandboxID: sbx.SandboxID,
				Command:   []string{"cat", "/workspace/base.txt"},
			})
			trafficMu.Lock()
			if err != nil || strings.TrimSpace(resp.Stdout) != "base-content" {
				trafficErrs = append(trafficErrs, fmt.Sprintf("read: resp=%+v err=%v", resp, err))
			} else {
				readsOK++
			}
			trafficMu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
	}()
	time.Sleep(60 * time.Millisecond)

	var migrated SandboxRecord
	migStart := time.Now()
	if err := cli.CallSandboxEndpoint(ctx, http.MethodPost, "/__session/migrate", sbx.SandboxID, MigrateSandboxRequest{
		SandboxID:    sbx.SandboxID,
		TargetHostID: "host-precopy-dst",
	}, &migrated); err != nil {
		t.Fatalf("two-phase migrate failed: %v", err)
	}
	migWall := time.Since(migStart)
	time.Sleep(60 * time.Millisecond)
	close(stopTraffic)
	trafficWG.Wait()

	hookMu.Lock()
	if hookCalls != 1 || hookErr != nil {
		t.Fatalf("after-prestage hook calls=%d err=%v", hookCalls, hookErr)
	}
	hookMu.Unlock()

	// (d) Verify the prestaged delta cutover.
	if migrated.HostID != "host-precopy-dst" || migrated.Status != StatusRunning {
		t.Fatalf("migrated record = host %s status %s, want host-precopy-dst RUNNING", migrated.HostID, migrated.Status)
	}
	if migrated.LastMigrationMode != MigrationModePrestagedDelta {
		t.Fatalf("LastMigrationMode = %q, want %q", migrated.LastMigrationMode, MigrationModePrestagedDelta)
	}
	if migrated.LastMigrationFreezeMs < 0 || migrated.LastMigrationFreezeMs >= 200 {
		t.Fatalf("LastMigrationFreezeMs = %dms, want < 200ms", migrated.LastMigrationFreezeMs)
	}
	if migrated.LastMigrationTotalMs < migrated.LastMigrationFreezeMs {
		t.Fatalf("LastMigrationTotalMs (%d) < LastMigrationFreezeMs (%d)", migrated.LastMigrationTotalMs, migrated.LastMigrationFreezeMs)
	}
	if migrated.TotalDowntimeMs < migrated.LastMigrationFreezeMs {
		t.Fatalf("TotalDowntimeMs (%d) < LastMigrationFreezeMs (%d)", migrated.TotalDowntimeMs, migrated.LastMigrationFreezeMs)
	}
	if migrated.MigrationCount != 1 {
		t.Fatalf("MigrationCount = %d, want 1", migrated.MigrationCount)
	}
	t.Logf("two-phase migration: freeze=%dms total=%dms wall=%s", migrated.LastMigrationFreezeMs, migrated.LastMigrationTotalMs, migWall)

	// The source-measured freeze (source sbxMu held) is reported in the source's MIGRATE event.
	srcFreeze := int64(-1)
	hSrc.mu.RLock()
	for _, evt := range hSrc.recentEvents {
		if evt.Type == "MIGRATE" && evt.SandboxID == sbx.SandboxID {
			if !strings.Contains(evt.Message, "freeze_ms=") || !strings.Contains(evt.Message, "total_ms=") ||
				!strings.Contains(evt.Message, "mode="+MigrationModePrestagedDelta) {
				t.Errorf("source MIGRATE event missing freeze_ms/total_ms/mode: %q", evt.Message)
			}
			if i := strings.Index(evt.Message, "freeze_ms="); i >= 0 {
				fmt.Sscanf(evt.Message[i:], "freeze_ms=%d", &srcFreeze)
			}
		}
	}
	hSrc.mu.RUnlock()
	if srcFreeze < 0 || srcFreeze >= 200 {
		t.Fatalf("source-measured freeze_ms = %d, want 0 <= freeze < 200ms", srcFreeze)
	}
	dstEvt := false
	hDst.mu.RLock()
	for _, evt := range hDst.recentEvents {
		if evt.Type == "MIGRATE" && evt.SandboxID == sbx.SandboxID &&
			strings.Contains(evt.Message, "freeze_ms=") && strings.Contains(evt.Message, "total_ms=") {
			dstEvt = true
		}
	}
	_, stillStaged := hDst.prestaged[sbx.SandboxID]
	hDst.mu.RUnlock()
	if !dstEvt {
		t.Fatalf("target host has no MIGRATE event with freeze_ms/total_ms")
	}
	if stillStaged {
		t.Fatalf("target still holds a prestaged entry after cutover")
	}
	hSrc.mu.RLock()
	redirect := hSrc.migratedTo[sbx.SandboxID]
	_, srcHas := hSrc.sandboxes[sbx.SandboxID]
	hSrc.mu.RUnlock()
	if redirect != "host-precopy-dst" || srcHas {
		t.Fatalf("source migratedTo=%q stillHas=%v, want redirect to host-precopy-dst", redirect, srcHas)
	}

	trafficMu.Lock()
	if len(trafficErrs) > 0 {
		t.Fatalf("concurrent exec traffic failed during migration (%d errors), first: %s", len(trafficErrs), trafficErrs[0])
	}
	if readsOK == 0 || len(writesOK) == 0 {
		t.Fatalf("expected concurrent traffic during migration: readsOK=%d writesOK=%d", readsOK, len(writesOK))
	}
	sawSrc, sawDst := false, false
	for _, hid := range writesOK {
		sawSrc = sawSrc || hid == "host-precopy-src"
		sawDst = sawDst || hid == "host-precopy-dst"
	}
	writes := make(map[int]string, len(writesOK))
	for k, v := range writesOK {
		writes[k] = v
	}
	trafficMu.Unlock()
	if !sawSrc || !sawDst {
		t.Fatalf("expected concurrent writes served by both source and destination, got %v", writes)
	}

	// Files on the destination host: base intact, delta updated, deletions applied, new files present.
	check := func(cmd, want string) {
		t.Helper()
		resp, err := cli.Exec(ctx, ExecRequest{SandboxID: sbx.SandboxID, Command: []string{"sh", "-c", cmd}})
		if err != nil {
			t.Fatalf("exec %q on destination failed: %v", cmd, err)
		}
		if resp.HostID != "host-precopy-dst" {
			t.Fatalf("exec %q served by %s, want host-precopy-dst", cmd, resp.HostID)
		}
		if strings.TrimSpace(resp.Stdout) != want {
			t.Fatalf("exec %q stdout = %q, want %q", cmd, resp.Stdout, want)
		}
	}
	check("cat /workspace/base.txt", "base-content")
	check("cat /workspace/delta.txt", "delta-v2")
	check("cat /workspace/newdir/new.txt", "fresh")
	check("if [ -e /workspace/gone.txt ]; then echo present; else echo deleted; fi", "deleted")
	check("if [ -e /workspace/olddir ]; then echo present; else echo deleted; fi", "deleted")
	for i, hid := range writes {
		check(fmt.Sprintf("cat /workspace/conc-%d.txt", i), fmt.Sprintf("w%d", i))
		_ = hid
	}

	// GCS durability: the destination asynchronously checkpoints the full overlay after cutover.
	deadline := time.Now().Add(10 * time.Second)
	wantOverlay := fmt.Sprintf("persistence/%s/overlay-e%d.tar", sbx.SandboxID, migrated.Epoch)
	var storeRec SandboxRecord
	for {
		_, _, _ = store.ReadJSON(ctx, "sandboxes/"+sbx.SandboxID+".json", &storeRec)
		if storeRec.OverlayPath == wantOverlay || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if storeRec.HostID != "host-precopy-dst" || storeRec.OverlayPath != wantOverlay {
		t.Fatalf("store record host=%s overlay=%s, want host-precopy-dst + %s", storeRec.HostID, storeRec.OverlayPath, wantOverlay)
	}
	if storeRec.LastMigrationFreezeMs != migrated.LastMigrationFreezeMs || storeRec.LastMigrationMode != MigrationModePrestagedDelta {
		t.Fatalf("store record freeze=%d mode=%q, want %d %q", storeRec.LastMigrationFreezeMs, storeRec.LastMigrationMode,
			migrated.LastMigrationFreezeMs, MigrationModePrestagedDelta)
	}
	localTar, cleanup, err := store.LocalBlobPath(ctx, wantOverlay, false)
	if err != nil {
		t.Fatalf("LocalBlobPath(%s) failed: %v", wantOverlay, err)
	}
	restoreDir := t.TempDir()
	if err := extractTarToDir(localTar, restoreDir); err != nil {
		t.Fatalf("extract async checkpoint failed: %v", err)
	}
	if cleanup != nil {
		_ = cleanup()
	}
	if b, err := os.ReadFile(filepath.Join(restoreDir, "workspace/delta.txt")); err != nil || strings.TrimSpace(string(b)) != "delta-v2" {
		t.Fatalf("async GCS checkpoint delta.txt = %q (err=%v), want delta-v2", string(b), err)
	}
	if _, err := os.Stat(filepath.Join(restoreDir, "workspace/gone.txt")); !os.IsNotExist(err) {
		t.Fatalf("async GCS checkpoint still contains deleted gone.txt (err=%v)", err)
	}
}

// TestTwoPhasePreCopyFallbackToGCS verifies that if the warm prestaged copy is lost before cutover
// (e.g. TTL expiry or target restart), migration falls back to the full GCS stop-and-copy path.
func TestTwoPhasePreCopyFallbackToGCS(t *testing.T) {
	ctx := context.Background()
	router, cp, poolTS, cpTS, _ := newTestFleet(t, 5)
	hSrc := router.getOrCreateHost("host-fb-src")
	hDst := router.getOrCreateHost("host-fb-dst")
	_ = hSrc.SyncStateAndHeartbeat(ctx)
	_ = hDst.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)

	cli := NewCLIClient(cpTS.URL, poolTS.URL, t.TempDir())
	var sbx SandboxRecord
	if _, err := cli.doJSON(ctx, http.MethodPost, poolTS.URL+"/__session/create",
		map[string]string{"X-Serverless-Session-Id": "host-fb-src"},
		CreateSandboxRequest{Name: "precopy-fb", Template: "ubuntu", Persistent: true}, &sbx); err != nil {
		t.Fatalf("create precopy-fb failed: %v", err)
	}
	if resp, err := cli.Exec(ctx, ExecRequest{
		SandboxID: sbx.SandboxID,
		Command:   []string{"sh", "-c", "echo fb-base > /workspace/base.txt && echo fb-gone > /workspace/gone.txt"},
	}); err != nil || resp.ExitCode != 0 {
		t.Fatalf("seed failed: resp=%+v err=%v", resp, err)
	}
	hSrc.testHookAfterPrestage = func(sandboxID string) {
		if _, err := cli.Exec(ctx, ExecRequest{
			SandboxID: sandboxID,
			Command:   []string{"sh", "-c", "echo fb-delta > /workspace/delta.txt && rm -f /workspace/gone.txt"},
		}); err != nil {
			t.Errorf("mid-precopy exec failed: %v", err)
		}
		// Simulate the target losing its warm copy (TTL expiry / instance restart).
		hDst.discardPrestaged(ctx, sandboxID)
	}

	var migrated SandboxRecord
	if err := cli.CallSandboxEndpoint(ctx, http.MethodPost, "/__session/migrate", sbx.SandboxID, MigrateSandboxRequest{
		SandboxID:    sbx.SandboxID,
		TargetHostID: "host-fb-dst",
	}, &migrated); err != nil {
		t.Fatalf("fallback migrate failed: %v", err)
	}
	if migrated.HostID != "host-fb-dst" || migrated.LastMigrationMode != MigrationModeGCSFull {
		t.Fatalf("migrated host=%s mode=%q, want host-fb-dst %q", migrated.HostID, migrated.LastMigrationMode, MigrationModeGCSFull)
	}
	for cmd, want := range map[string]string{
		"cat /workspace/base.txt":  "fb-base",
		"cat /workspace/delta.txt": "fb-delta",
		"if [ -e /workspace/gone.txt ]; then echo present; else echo deleted; fi": "deleted",
	} {
		resp, err := cli.Exec(ctx, ExecRequest{SandboxID: sbx.SandboxID, Command: []string{"sh", "-c", cmd}})
		if err != nil || resp.HostID != "host-fb-dst" || strings.TrimSpace(resp.Stdout) != want {
			t.Fatalf("exec %q after fallback: resp=%+v err=%v, want %q on host-fb-dst", cmd, resp, err, want)
		}
	}
}

// TestDeltaTarExportApplyLocalEngine unit-tests LocalProcessEngine.ExportDeltaTar / ApplyDeltaTar.
func TestDeltaTarExportApplyLocalEngine(t *testing.T) {
	ctx := context.Background()
	src := NewLocalProcessEngine(t.TempDir())
	dst := NewLocalProcessEngine(t.TempDir())
	rec := SandboxRecord{SandboxID: "sbx-delta-unit", Template: "ubuntu"}
	if err := src.Start(ctx, rec, ""); err != nil {
		t.Fatalf("src.Start: %v", err)
	}
	srcRoot, _ := src.sandboxRoot(rec.SandboxID)
	write := func(rel, content string) {
		p := filepath.Join(srcRoot, rel)
		_ = os.MkdirAll(filepath.Dir(p), 0755)
		if err := os.WriteFile(p, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}
	write("workspace/keep.txt", "keep")
	write("workspace/change.txt", "v1")
	write("workspace/remove.txt", "remove-me")
	write("workspace/sub/a.txt", "a")
	write("workspace/backdated.txt", "old-content")
	// Age the base files beyond the delta mtime slack so only real changes are re-sent.
	hourAgo := time.Now().Add(-time.Hour)
	for _, rel := range []string{"workspace/keep.txt", "workspace/change.txt", "workspace/remove.txt", "workspace/sub/a.txt", "workspace/backdated.txt"} {
		_ = os.Chtimes(filepath.Join(srcRoot, rel), hourAgo, hourAgo)
	}

	since := time.Now()
	baseTar := filepath.Join(t.TempDir(), "base.tar")
	if err := src.ExportTar(ctx, rec, baseTar); err != nil {
		t.Fatalf("ExportTar: %v", err)
	}
	if err := dst.Start(ctx, rec, baseTar); err != nil {
		t.Fatalf("dst.Start: %v", err)
	}

	write("workspace/change.txt", "v2")
	write("workspace/added.txt", "added")
	_ = os.Remove(filepath.Join(srcRoot, "workspace/remove.txt"))
	_ = os.RemoveAll(filepath.Join(srcRoot, "workspace/sub"))
	// Backdated rewrite (e.g. `touch -d` / tar extraction): different size, old mtime.
	write("workspace/backdated.txt", "new-longer-content")
	old := time.Now().Add(-24 * time.Hour)
	_ = os.Chtimes(filepath.Join(srcRoot, "workspace/backdated.txt"), old, old)
	// Frequent background exports (post_exec / periodic) after the pre-copy must not evict the
	// pre-copy base manifest (otherwise deletions since the base would be lost).
	for i := 0; i < maxExportManifestsPerSandbox+3; i++ {
		if err := src.ExportTar(ctx, rec, filepath.Join(t.TempDir(), fmt.Sprintf("bg-%d.tar", i))); err != nil {
			t.Fatalf("background ExportTar %d: %v", i, err)
		}
	}

	deltaTar := filepath.Join(t.TempDir(), "delta.tar")
	deleted, err := src.ExportDeltaTar(ctx, rec, since, deltaTar)
	if err != nil {
		t.Fatalf("ExportDeltaTar: %v", err)
	}
	if !containsStr(deleted, "workspace/remove.txt") || !containsStr(deleted, "workspace/sub") {
		t.Fatalf("deletedPaths = %v, want workspace/remove.txt and workspace/sub", deleted)
	}
	m, err := manifestFromTarFile(deltaTar)
	if err != nil {
		t.Fatalf("read delta manifest: %v", err)
	}
	for _, want := range []string{"workspace/change.txt", "workspace/added.txt", "workspace/backdated.txt"} {
		if _, ok := m[want]; !ok {
			t.Fatalf("delta tar missing %s (entries=%v)", want, m)
		}
	}
	if _, ok := m["workspace/keep.txt"]; ok {
		t.Fatalf("delta tar unexpectedly contains unchanged workspace/keep.txt")
	}
	if err := dst.ApplyDeltaTar(ctx, rec, deltaTar, deleted); err != nil {
		t.Fatalf("ApplyDeltaTar: %v", err)
	}
	dstRoot, _ := dst.sandboxRoot(rec.SandboxID)
	for rel, want := range map[string]string{
		"workspace/keep.txt":      "keep",
		"workspace/change.txt":    "v2",
		"workspace/added.txt":     "added",
		"workspace/backdated.txt": "new-longer-content",
	} {
		if b, err := os.ReadFile(filepath.Join(dstRoot, rel)); err != nil || string(b) != want {
			t.Fatalf("dst %s = %q (err=%v), want %q", rel, string(b), err, want)
		}
	}
	for _, rel := range []string{"workspace/remove.txt", "workspace/sub"} {
		if _, err := os.Stat(filepath.Join(dstRoot, rel)); !os.IsNotExist(err) {
			t.Fatalf("dst %s still exists after ApplyDeltaTar (err=%v)", rel, err)
		}
	}
	// Path traversal in deletedPaths must be rejected.
	if err := dst.ApplyDeltaTar(ctx, rec, deltaTar, []string{"../../etc"}); err == nil {
		t.Fatalf("ApplyDeltaTar accepted traversal deleted path")
	}
	// A delta without a prior base export (no manifest) must fail so callers fall back to a full copy.
	fresh := NewLocalProcessEngine(t.TempDir())
	rec2 := SandboxRecord{SandboxID: "sbx-delta-nobase"}
	_ = fresh.Start(ctx, rec2, "")
	if _, err := fresh.ExportDeltaTar(ctx, rec2, time.Now(), filepath.Join(t.TempDir(), "x.tar")); err == nil {
		t.Fatalf("ExportDeltaTar without a base manifest should fail")
	}
}

func containsStr(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// TestUIProxyStripsCloudRunIAPAndServerlessAuthHeaders verifies that the Web UI "Open Preview URL"
// path (sandbox-ui -> sandbox-control-plane -> sandbox-pool -> sandbox app) never forwards the
// browser's IAP / X-Serverless-* credentials or IAP/UI cookies to the next hop (GFE would evaluate
// X-Serverless-Authorization minted for sandbox-ui and reject the hop with 401), while preserving
// application cookies, and that redirects issued by the app stay under the proxy prefix.
func TestUIProxyStripsCloudRunIAPAndServerlessAuthHeaders(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore(t.TempDir())
	router := &sessionPoolRouter{t: t, store: store, maxDensity: 5, revision: "sandbox-pool-00001-v1", hosts: make(map[string]*HostServer)}

	var seenMu sync.Mutex
	var cpSeen, poolSeen []http.Header
	poolTS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/proxy/") {
			seenMu.Lock()
			poolSeen = append(poolSeen, r.Header.Clone())
			seenMu.Unlock()
		}
		router.ServeHTTP(w, r)
	}))
	t.Cleanup(poolTS.Close)
	cfg := DefaultFleetConfig()
	cfg.MaxDensity = 5
	cfg.PoolServiceURL = poolTS.URL
	cfg.BinPackHysteresis = 0
	cp := NewControlPlaneServer(store, cfg)
	cpTS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/proxy/") {
			seenMu.Lock()
			cpSeen = append(cpSeen, r.Header.Clone())
			seenMu.Unlock()
		}
		cp.ServeHTTP(w, r)
	}))
	t.Cleanup(func() {
		cp.Close()
		cpTS.Close()
	})
	router.controlPlaneURL = cpTS.URL
	router.poolURL = poolTS.URL

	h1 := router.getOrCreateHost("host-hdr-1")
	_ = h1.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)
	cli := NewCLIClient(cpTS.URL, poolTS.URL, t.TempDir())

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen failed: %v", err)
	}
	appPort := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	sbx, err := cli.CreateSandbox(ctx, CreateSandboxRequest{Name: "hdr-preview", Template: "python", TargetHostID: "host-hdr-1"})
	if err != nil {
		t.Fatalf("CreateSandbox failed: %v", err)
	}
	testBin, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable failed: %v", err)
	}
	if _, err := cli.Exec(ctx, ExecRequest{
		SandboxID:  sbx.SandboxID,
		Background: true,
		Command:    []string{"sh", "-c", fmt.Sprintf("SBX_TEST_HTTP_SERVER_PORT=%d exec %s", appPort, shellQuote(testBin))},
	}); err != nil {
		t.Fatalf("start background test HTTP server failed: %v", err)
	}
	_ = h1.SyncStateAndHeartbeat(ctx)
	_ = cp.SyncFromStore(ctx)

	uiTS := httptest.NewServer(NewUIServer(cpTS.URL, poolTS.URL, t.TempDir()))
	t.Cleanup(uiTS.Close)
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	doUI := func(path string) (*http.Response, []byte) {
		t.Helper()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, uiTS.URL+path, nil)
		req.Header.Set("X-Serverless-Authorization", "Bearer bad-iap-token")
		req.Header.Set("X-Goog-Iap-Jwt-Assertion", "bad-jwt")
		req.Header.Set("X-Goog-Authenticated-User-Email", "accounts.google.com:dev@example.com")
		req.Header.Set("Authorization", "Bearer browser-token")
		req.Header.Set("Proxy-Authorization", "Basic cHJveHk=")
		req.Header.Set("X-Sandbox-Authorization", "Bearer app-token")
		req.Header.Set("Cookie", "GCP_IAAP_AUTH_TOKEN_123=secret; __Host-sbx_csrf=csrf; app_session=ok")
		resp, err := noRedirect.Do(req)
		if err != nil {
			t.Fatalf("GET %s via UI failed: %v", path, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp, body
	}

	var resp *http.Response
	var body []byte
	for i := 0; i < 30; i++ {
		resp, body = doUI(fmt.Sprintf("/proxy/%s/%d/hello", sbx.SandboxID, appPort))
		if resp.StatusCode == http.StatusOK {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("UI preview status = %d, want 200 (body=%s)", resp.StatusCode, string(body))
	}
	var echoed struct {
		Path    string              `json:"path"`
		Headers map[string][]string `json:"headers"`
	}
	if err := json.Unmarshal(body, &echoed); err != nil {
		t.Fatalf("decode sandbox echo: %v (body=%s)", err, string(body))
	}
	if echoed.Path != "/hello" {
		t.Fatalf("sandbox saw path %q, want /hello", echoed.Path)
	}

	forbidden := []string{"X-Serverless-Authorization", "X-Goog-Iap-Jwt-Assertion", "X-Goog-Authenticated-User-Email", "Proxy-Authorization"}
	checkHop := func(hop string, h http.Header, wantAuth string) {
		t.Helper()
		for _, k := range forbidden {
			if v := h.Values(k); len(v) > 0 {
				t.Fatalf("%s received forbidden header %s=%v", hop, k, v)
			}
		}
		cookies := strings.Join(h.Values("Cookie"), "; ")
		if cookies != "app_session=ok" {
			t.Fatalf("%s Cookie = %q, want only app_session=ok", hop, cookies)
		}
		if got := h.Get("Authorization"); got != wantAuth {
			t.Fatalf("%s Authorization = %q, want %q", hop, got, wantAuth)
		}
	}
	seenMu.Lock()
	if len(cpSeen) == 0 || len(poolSeen) == 0 {
		seenMu.Unlock()
		t.Fatalf("expected proxy hops through control plane and pool, got cp=%d pool=%d", len(cpSeen), len(poolSeen))
	}
	cpHdr, poolHdr := cpSeen[len(cpSeen)-1], poolSeen[len(poolSeen)-1]
	seenMu.Unlock()
	// http:// test services: no ID token is minted, so no Authorization at all on internal hops.
	checkHop("sandbox-control-plane", cpHdr, "")
	checkHop("sandbox-pool", poolHdr, "")
	if got := poolHdr.Values("X-Serverless-Session-Id"); len(got) != 1 || got[0] != "host-hdr-1" {
		t.Fatalf("sandbox-pool X-Serverless-Session-Id = %v, want [host-hdr-1]", got)
	}
	if cpHdr.Get("X-Sandbox-Authorization") != "Bearer app-token" {
		t.Fatalf("control plane lost X-Sandbox-Authorization: %v", cpHdr)
	}
	// The sandbox app sees only its own credentials (X-Sandbox-Authorization -> Authorization).
	sbxHdr := http.Header(echoed.Headers)
	checkHop("sandbox app", sbxHdr, "Bearer app-token")
	if v := sbxHdr.Values("X-Sandbox-Authorization"); len(v) > 0 {
		t.Fatalf("sandbox app received X-Sandbox-Authorization=%v", v)
	}
	if v := sbxHdr.Values("X-Serverless-Session-Id"); len(v) > 0 {
		t.Fatalf("sandbox app received X-Serverless-Session-Id=%v", v)
	}

	// Root-relative redirects from the previewed app stay under the proxy prefix.
	resp, body = doUI(fmt.Sprintf("/proxy/%s/%d/redirect", sbx.SandboxID, appPort))
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("redirect status = %d, want 302 (body=%s)", resp.StatusCode, string(body))
	}
	if want := fmt.Sprintf("/proxy/%s/%d/login", sbx.SandboxID, appPort); resp.Header.Get("Location") != want {
		t.Fatalf("Location = %q, want %q", resp.Header.Get("Location"), want)
	}

	// Unit checks for the shared helpers.
	if got := filterProxyCookieHeader("GCP_IAP_UID=1; a=b;sbx_csrf=x; GCP_IAAP_AUTH_TOKEN_9=z ;c=d"); got != "a=b; c=d" {
		t.Fatalf("filterProxyCookieHeader = %q, want %q", got, "a=b; c=d")
	}
	loc := http.Header{"Location": []string{"//evil.example/x"}}
	rewriteProxyLocation(loc, "s", 80)
	if loc.Get("Location") != "//evil.example/x" {
		t.Fatalf("protocol-relative Location must not be rewritten, got %q", loc.Get("Location"))
	}
}
