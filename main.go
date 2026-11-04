package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// When installed via `go install github.com/oded996/cloud-run-sandbox-dispenser@latest`,
	// best-effort create an `sbx` symlink alongside `cloud-run-sandbox-dispenser` in $GOBIN.
	if exe, err := os.Executable(); err == nil && filepath.Base(exe) == "cloud-run-sandbox-dispenser" {
		sbxLink := filepath.Join(filepath.Dir(exe), "sbx")
		if _, statErr := os.Lstat(sbxLink); os.IsNotExist(statErr) {
			_ = os.Symlink(filepath.Base(exe), sbxLink)
		}
	}

	args := os.Args[1:]
	mode := strings.ToLower(strings.TrimSpace(os.Getenv("DISPENSER_MODE")))
	if len(args) > 0 {
		first := strings.ToLower(args[0])
		if first == "hostd" || first == "controld" || first == "ui" {
			mode = first
			args = args[1:]
		}
	}

	switch mode {
	case "hostd":
		runHostDaemon(ctx)
	case "controld":
		runControlPlaneDaemon(ctx)
	case "ui":
		runUIDaemon(ctx)
	default:
		os.Exit(RunCLI(ctx, args, os.Stdout, os.Stderr))
	}
}

func runHostDaemon(ctx context.Context) {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	bucket := os.Getenv("GCS_BUCKET_NAME")
	mountRoot := os.Getenv("GCS_MOUNT_ROOT")
	if mountRoot == "" {
		mountRoot = "/mnt/gcs"
	}
	maxDensity := 10
	if v, err := strconv.Atoi(os.Getenv("MAX_DENSITY")); err == nil && v > 0 {
		maxDensity = v
	}
	cpURL := os.Getenv("CONTROL_PLANE_URL")
	poolURL := os.Getenv("POOL_SERVICE_URL")
	hostID := os.Getenv("HOST_SESSION_ID")

	store := NewGCSStore(bucket, mountRoot)
	engine := NewDefaultEngine("")
	hostSrv := NewHostServer(hostID, store, engine, maxDensity, cpURL, poolURL)
	hostSrv.StartBackgroundLoops(ctx)

	bindHost := os.Getenv("BIND_HOST")
	if bindHost == "" && os.Getenv("K_SERVICE") == "" {
		bindHost = "127.0.0.1"
	}
	srv := &http.Server{
		Addr:              bindHost + ":" + port,
		Handler:           hostSrv,
		ReadHeaderTimeout: 15 * time.Second,
	}

	log.Printf("[hostd] Starting Cloud Run Sandbox Pool Host Supervisor (%s, revision=%s, max_density=%d) on %s",
		hostSrv.hostID, hostSrv.revision, maxDensity, srv.Addr)

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[hostd] HTTP server error: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	sig := <-sigCh
	log.Printf("[hostd] Received signal %v; initiating coordinated live evacuation of active sandboxes (gracePeriod=600s)...", sig)

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 540*time.Second)
	defer shutCancel()
	migrated, err := hostSrv.DrainAndEvacuate(shutCtx, "sigterm_shutdown", nil)
	log.Printf("[hostd] Evacuated %d sandboxes on SIGTERM (%v); err=%v", len(migrated), migrated, err)

	hostSrv.Close()
	_ = srv.Shutdown(shutCtx)
}

func runControlPlaneDaemon(ctx context.Context) {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	bucket := os.Getenv("GCS_BUCKET_NAME")
	mountRoot := os.Getenv("GCS_MOUNT_ROOT")
	if mountRoot == "" {
		mountRoot = "/mnt/gcs"
	}
	store := NewGCSStore(bucket, mountRoot)

	cfg := DefaultFleetConfig()
	if v, err := strconv.Atoi(os.Getenv("MAX_DENSITY")); err == nil && v > 0 {
		cfg.MaxDensity = v
	}
	if poolURL := os.Getenv("POOL_SERVICE_URL"); poolURL != "" {
		cfg.PoolServiceURL = strings.TrimRight(poolURL, "/")
	}
	if cpURL := os.Getenv("CONTROL_PLANE_URL"); cpURL != "" {
		cfg.ControlPlaneURL = strings.TrimRight(cpURL, "/")
	}

	cpSrv := NewControlPlaneServer(store, cfg)
	cpSrv.StartBackgroundLoops(ctx)

	bindHost := os.Getenv("BIND_HOST")
	if bindHost == "" && os.Getenv("K_SERVICE") == "" {
		bindHost = "127.0.0.1"
	}
	srv := &http.Server{
		Addr:              bindHost + ":" + port,
		Handler:           cpSrv,
		ReadHeaderTimeout: 15 * time.Second,
	}

	log.Printf("[controld] Starting Cloud Run Sandbox Control Plane (%s, pool=%s) on %s",
		cpSrv.instanceID, cpSrv.config.PoolServiceURL, srv.Addr)

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[controld] HTTP server error: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	<-sigCh
	cpSrv.Close()
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()
	_ = srv.Shutdown(shutCtx)
}

func runUIDaemon(ctx context.Context) {
	if hasHelpFlag(os.Args[1:]) {
		_, _ = os.Stdout.WriteString("Usage: sbx ui [--control-plane-url=URL] [--pool-url=URL] (runs Web Console BFF server on $PORT)\n")
		return
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	cpURL := strings.TrimSpace(os.Getenv("CONTROL_PLANE_URL"))
	poolURL := strings.TrimSpace(os.Getenv("POOL_SERVICE_URL"))
	cacheDir := strings.TrimSpace(os.Getenv("SBX_CACHE_DIR"))
	uiSrv := NewUIServer(cpURL, poolURL, cacheDir)

	bindHost := os.Getenv("BIND_HOST")
	if bindHost == "" && os.Getenv("K_SERVICE") == "" {
		bindHost = "127.0.0.1"
	}
	srv := &http.Server{
		Addr:              bindHost + ":" + port,
		Handler:           uiSrv,
		ReadHeaderTimeout: 15 * time.Second,
	}

	log.Printf("[sandbox-ui] Starting Cloud Run Sandbox Web Console (cp=%s, pool=%s) on %s",
		uiSrv.controlPlaneURL, uiSrv.poolServiceURL, srv.Addr)

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[sandbox-ui] HTTP server error: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	select {
	case <-sigCh:
	case <-ctx.Done():
	}
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()
	_ = srv.Shutdown(shutCtx)
}
