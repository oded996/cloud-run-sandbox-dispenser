package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	cloudRunSandboxBin  = "/usr/local/gcp/bin/sandbox"
	cloudRunSandboxSock = "/run/gcp/sandbox/sandbox.sock"
	instanceStatePath   = "/sys/class/instance/state"
	sharedDirPath       = "/mnt/shared"
)

// Base64+Gzipped LD_PRELOAD shim (libfakeuid.so) that maps UID 0 -> UID 999
// in Cloud Run Sandbox Runtime's single-UID user namespace so PostgreSQL 16 runs cleanly.
const libFakeUIDBase64Gzip = "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"

// Base64+Gzipped TCP proxy (tcpproxy) for bridging container netns ports to sandbox veth IPs.
const tcpProxyBase64Gzip = "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"

// BuiltInTemplates returns the built-in sandbox templates supported out of the box.
func BuiltInTemplates() map[string]TemplateSpec {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return map[string]TemplateSpec{
		"ubuntu": {
			Name:        "ubuntu",
			Type:        "prewarmed",
			Image:       "ubuntu:24.04",
			DefaultCmd:  []string{"/bin/sh", "-c", "mkdir -p /workspace /persistent && exec sleep infinity"},
			Description: "Ubuntu 24.04 LTS general-purpose sandbox with curl, git, python3, tar",
			CreatedAt:   now,
		},
		"python": {
			Name:        "python",
			Type:        "prewarmed",
			Image:       "python:3.12-slim",
			DefaultCmd:  []string{"/bin/sh", "-c", "mkdir -p /workspace /persistent && exec sleep infinity"},
			Description: "Python 3.12 runtime sandbox pre-warmed for AI code execution",
			CreatedAt:   now,
		},
		"node": {
			Name:        "node",
			Type:        "prewarmed",
			Image:       "node:22-slim",
			DefaultCmd:  []string{"/bin/sh", "-c", "mkdir -p /workspace /persistent && exec sleep infinity"},
			Description: "Node.js 22 LTS runtime sandbox for JS/TS agents and toolchains",
			CreatedAt:   now,
		},
		"postgres": {
			Name:        "postgres",
			Type:        "prewarmed",
			Image:       "mirror.gcr.io/library/postgres:16",
			DefaultCmd:  []string{"/mnt/shared/bin/start-pg.sh"},
			Description: "PostgreSQL 16 stateful database sandbox with libfakeuid.so & PGDATA=/pgdata/data overlay persistence",
			CreatedAt:   now,
		},
	}
}

// SandboxEngine abstracts the underlying container sandbox runtime (Cloud Run Sandbox Runtime `/usr/local/gcp/bin/sandbox`
// on Cloud Run Gen2, or isolated directory subshells in local/unit-test environments).
type SandboxEngine interface {
	// EngineType returns "cloudrun" or "local".
	EngineType() string
	// Start boots a new sandbox container (optionally importing an overlay tarball).
	Start(ctx context.Context, rec SandboxRecord, importTarPath string) error
	// Exec runs a command inside the sandbox.
	Exec(ctx context.Context, sandboxID string, cmd []string, workdir string, env map[string]string, background bool, timeout time.Duration) (stdout, stderr string, exitCode, pid int, err error)
	// ExecStream runs a command inside the sandbox and emits incremental stdout/stderr chunks to onChunk in real time.
	ExecStream(ctx context.Context, sandboxID string, cmd []string, workdir string, env map[string]string, timeout time.Duration, onChunk func(stream, data string)) (stdout, stderr string, exitCode int, err error)
	// DialSandboxPort establishes a TCP connection to a port listening inside the sandbox (with readiness backoff on resume).
	DialSandboxPort(ctx context.Context, rec SandboxRecord, port int) (net.Conn, error)
	// ExportTar exports the writable filesystem overlay (`rootfs-upper` / `/workspace` / `/pgdata` / `/persistent`) to destTarPath.
	ExportTar(ctx context.Context, rec SandboxRecord, destTarPath string) error
	// ExportDeltaTar exports to destTarPath only the entries created or modified since `since`,
	// relative to the base filesystem manifest captured by the ExportTar call(s) that started at or
	// after `since` (Phase 2 of Two-Phase Pre-Copy live migration). It returns the sandbox-relative
	// paths (e.g. "workspace/old.txt") that existed in that base export but have since been deleted.
	// It fails if no base export at or after `since` is known, so callers fall back to a full copy.
	ExportDeltaTar(ctx context.Context, rec SandboxRecord, since time.Time, destTarPath string) (deletedPaths []string, err error)
	// ApplyDeltaTar removes deletedPaths and extracts deltaTarPath over a prestaged (warm) sandbox,
	// then (re)starts any internal database (e.g. PostgreSQL) that was quiesced during pre-staging.
	ApplyDeltaTar(ctx context.Context, rec SandboxRecord, deltaTarPath string, deletedPaths []string) error
	// Quiesce cleanly flushes and stops any internal database daemon (e.g. PostgreSQL) prior to final suspend/migration export.
	Quiesce(ctx context.Context, sandboxID string) error
	// Fork performs a fast same-host Copy-on-Write clone from sourceSandboxID to targetRec.
	Fork(ctx context.Context, sourceSandboxID string, targetRec SandboxRecord) (forkMethod string, err error)
	// CopyFile uploads or downloads a file inside the sandbox with path boundary validation.
	CopyFile(ctx context.Context, sandboxID string, req FileCopyRequest) (FileCopyResponse, error)
	// ReadLogs returns recent stdout/stderr/system logs for the sandbox.
	ReadLogs(ctx context.Context, sandboxID string) (string, error)
	// AppendLog appends a timestamped line to the sandbox's log file.
	AppendLog(sandboxID, line string)
	// Stop deletes the running sandbox container (after any pre-stop hooks).
	Stop(ctx context.Context, sandboxID string) error
	// IsAlive checks whether the sandbox container is currently running.
	IsAlive(ctx context.Context, sandboxID string) bool
	// CheckInGuestState checks if the guest wrote IDLE/STOP to `/var/run/instance/state`.
	CheckInGuestState(sandboxID string) string
}

// NewDefaultEngine returns a CloudRunSandboxEngine if `/usr/local/gcp/bin/sandbox` and its
// daemon socket `/run/gcp/sandbox/sandbox.sock` exist on the host, or a LocalProcessEngine
// otherwise (for local execution, unit testing, and projects without Cloud Run Sandbox Runtime templates enabled).
func NewDefaultEngine(rootDir string) SandboxEngine {
	if _, err := os.Stat(cloudRunSandboxBin); err == nil && os.Getenv("SBX_FORCE_LOCAL_ENGINE") == "" {
		if _, sockErr := os.Stat(cloudRunSandboxSock); sockErr == nil {
			return NewCloudRunSandboxEngine(sharedDirPath)
		}
	}
	if rootDir == "" {
		rootDir = filepath.Join(os.TempDir(), "sbx-local-engine")
	}
	return NewLocalProcessEngine(rootDir)
}

func containerIDForSandbox(sandboxID string) string {
	sum := sha256.Sum256([]byte(sandboxID))
	return "sbx-" + hex.EncodeToString(sum[:6])
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'"
}

func validEnvKey(k string) bool {
	if k == "" {
		return false
	}
	for i := 0; i < len(k); i++ {
		c := k[i]
		alpha := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
		digit := c >= '0' && c <= '9'
		if i == 0 && !alpha {
			return false
		}
		if !alpha && !digit {
			return false
		}
	}
	return true
}

func decodeGzipBase64ToFile(b64 string, destPath string, mode os.FileMode) error {
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return err
	}
	gz, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return err
	}
	defer gz.Close()
	data, err := io.ReadAll(gz)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
		return err
	}
	return os.WriteFile(destPath, data, mode)
}

// SetInstanceBusyState writes "busy" or "idle" to `/sys/class/instance/state`
// so Cloud Run keeps session clones alive while sandboxes are running and scales to 0 when empty.
func SetInstanceBusyState(busy bool) string {
	val := "idle"
	if busy {
		val = "busy"
	}
	statePath := os.Getenv("SBX_INSTANCE_STATE_FILE")
	if statePath == "" {
		statePath = instanceStatePath
	}
	_ = os.WriteFile(statePath, []byte(val), 0644)
	return val
}

// CloudRunSandboxEngine implements SandboxEngine using Cloud Run's `/usr/local/gcp/bin/sandbox --mode=container`.
type CloudRunSandboxEngine struct {
	sharedDir string
	mu        sync.Mutex
	manifests manifestTracker
}

// NewCloudRunSandboxEngine initializes Cloud Run Sandbox Runtime helper scripts (`libfakeuid.so`, `tcpproxy`, `start-pg.sh`)
// in `/mnt/shared/bin` and returns a ready CloudRunSandboxEngine.
func NewCloudRunSandboxEngine(sharedDir string) *CloudRunSandboxEngine {
	if sharedDir == "" {
		sharedDir = sharedDirPath
	}
	s := &CloudRunSandboxEngine{sharedDir: sharedDir}
	s.ensureHelpers()
	return s
}

// EngineType returns "cloudrun".
func (s *CloudRunSandboxEngine) EngineType() string {
	return "cloudrun"
}

func (s *CloudRunSandboxEngine) ensureHelpers() {
	binDir := filepath.Join(s.sharedDir, "bin")
	_ = os.MkdirAll(binDir, 0777)
	_ = decodeGzipBase64ToFile(libFakeUIDBase64Gzip, filepath.Join(binDir, "libfakeuid.so"), 0755)
	_ = decodeGzipBase64ToFile(tcpProxyBase64Gzip, filepath.Join(binDir, "tcpproxy"), 0755)

	startPgScript := `#!/bin/sh
set -e
export LD_PRELOAD=/mnt/shared/bin/libfakeuid.so
export POSTGRES_PASSWORD=postgres
export PGDATA=/pgdata/data
mkdir -p /workspace /persistent /pgdata/data /var/run/postgresql
chmod 700 /pgdata/data
chmod 777 /var/run/postgresql
rm -f /pgdata/data/postmaster.pid
if [ -d /usr/lib/postgresql ]; then
  PG_BIN=$(find /usr/lib/postgresql -name initdb | head -n 1 | xargs -r dirname)
  if [ -n "$PG_BIN" ]; then
    export PATH="$PG_BIN:$PATH"
    if [ ! -f /pgdata/data/PG_VERSION ]; then
      "$PG_BIN/initdb" -D /pgdata/data -U postgres --auth=trust >/mnt/shared/pg-init.log 2>&1 || true
      echo "host all all 0.0.0.0/0 trust" >> /pgdata/data/pg_hba.conf
      echo "listen_addresses = '*'" >> /pgdata/data/postgresql.conf
    fi
    "$PG_BIN/pg_ctl" -D /pgdata/data -l /pgdata/postgres.log -w start || true
  fi
fi
`
	_ = os.WriteFile(filepath.Join(binDir, "start-pg.sh"), []byte(startPgScript), 0755)
}

func (s *CloudRunSandboxEngine) runCmd(ctx context.Context, stdin []byte, args ...string) (string, string, int, error) {
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf
	err := cmd.Run()
	code := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			code = exitErr.ExitCode()
		} else {
			code = -1
		}
	}
	return outBuf.String(), errBuf.String(), code, err
}

func resolveCloudRunTemplate(tpl string) string {
	t := strings.ToLower(strings.TrimSpace(tpl))
	switch {
	case strings.Contains(t, "python") || t == "py":
		return "python"
	case strings.Contains(t, "node") || t == "js" || t == "ts":
		return "node"
	case strings.Contains(t, "postgres") || t == "pg":
		return "postgres"
	default:
		return "ubuntu"
	}
}

func (s *CloudRunSandboxEngine) stateFileFor(sandboxID string) string {
	return filepath.Join(s.sharedDir, "state-"+containerIDForSandbox(sandboxID))
}

func (s *CloudRunSandboxEngine) logFileFor(sandboxID string) string {
	return filepath.Join(s.sharedDir, "logs-"+containerIDForSandbox(sandboxID)+".log")
}

// Start boots a Cloud Run Sandbox Runtime container sandbox, optionally importing an overlay tar and starting Postgres if requested.
func (s *CloudRunSandboxEngine) Start(ctx context.Context, rec SandboxRecord, importTarPath string) error {
	if err := ValidateID(rec.SandboxID); err != nil {
		return err
	}
	cid := containerIDForSandbox(rec.SandboxID)
	cloudRunTpl := resolveCloudRunTemplate(rec.Template)
	// Fallback to ubuntu if postgres template is not separately defined in service.yaml sandboxes.templates
	stateFile := s.stateFileFor(rec.SandboxID)
	_ = os.MkdirAll(s.sharedDir, 0777)
	_ = os.WriteFile(stateFile, []byte("ACTIVE\n"), 0666)
	_ = os.Chmod(stateFile, 0666)

	buildArgs := func(tplName string, withImport bool) []string {
		args := []string{
			cloudRunSandboxBin,
			"--mode=container",
			"run",
			"--detach",
			"--write",
		}
		if rec.AllowEgress {
			args = append(args, "--allow-egress")
		}
		if withImport && importTarPath != "" {
			if st, err := os.Stat(importTarPath); err == nil && st.Size() > 0 {
				args = append(args, "--import-tar", importTarPath)
			}
		}
		args = append(args,
			"--template", tplName,
			"--env", "PATH=/mnt/shared/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
			"--env", "HOME=/root",
			"--env", "TAR_OPTIONS=--no-same-owner",
			"--env", "SANDBOX_ID="+rec.SandboxID,
			"--env", "INSTANCE_STATE_FILE=/var/run/instance/state",
		)
		for k, v := range rec.Env {
			if validEnvKey(k) {
				args = append(args, "--env", fmt.Sprintf("%s=%s", k, v))
			}
		}
		initScript := "mkdir -p /workspace /persistent /pgdata/data /var/run/instance && ln -sf " + stateFile + " /var/run/instance/state && ln -sf " + stateFile + " /workspace/.instance_state && exec sleep 360000"
		args = append(args, "--", cid, "sh", "-c", initScript)
		return args
	}

	usedImportTarFlag := importTarPath != ""
	args := buildArgs(cloudRunTpl, usedImportTarFlag)
	stdout, stderr, _, err := s.runCmd(ctx, nil, args...)
	if err != nil && cloudRunTpl == "postgres" {
		// If service.yaml only defines ubuntu/python/node, fallback to ubuntu
		cloudRunTpl = "ubuntu"
		args = buildArgs(cloudRunTpl, usedImportTarFlag)
		stdout, stderr, _, err = s.runCmd(ctx, nil, args...)
	}
	if err != nil && usedImportTarFlag && !strings.Contains(stderr, "already exists") {
		// Fallback if --import-tar flag is unsupported or tar format requires in-container extraction
		usedImportTarFlag = false
		args = buildArgs(cloudRunTpl, false)
		stdout, stderr, _, err = s.runCmd(ctx, nil, args...)
	}
	if err != nil && !strings.Contains(stderr, "already exists") {
		return fmt.Errorf("cloudrun sandbox run failed: %w (stdout=%s, stderr=%s)", err, stdout, stderr)
	}

	deadline := time.Now().Add(15 * time.Second)
	live := false
	for time.Now().Before(deadline) {
		if s.IsAlive(ctx, rec.SandboxID) {
			live = true
			break
		}
		time.Sleep(120 * time.Millisecond)
	}
	if !live {
		return fmt.Errorf("cloudrun container %s did not become live within 15s (stderr=%s)", cid, stderr)
	}

	setupCmd := "mkdir -p /workspace /persistent /pgdata/data /var/run/instance && ln -sf " + stateFile + " /var/run/instance/state && ln -sf " + stateFile + " /workspace/.instance_state"
	_, _, _, _ = s.runCmd(ctx, nil, cloudRunSandboxBin, "--mode=container", "exec", "--", cid, "sh", "-c", setupCmd)

	// Always extract /workspace, /pgdata, /persistent from importTarPath if present to guarantee RPO=0 restoration
	// across both host_sandbox_tar and exec_workspace_tar formats.
	if importTarPath != "" {
		if b, rErr := os.ReadFile(importTarPath); rErr == nil && len(b) > 0 {
			_, _, _, _ = s.runCmd(ctx, b, cloudRunSandboxBin, "--mode=container", "exec", "--", cid,
				"sh", "-c", "mkdir -p /workspace /persistent /pgdata/data && tar --no-same-owner --warning=no-file-changed -xf - -C / 2>/dev/null || true")
		}
	}

	if strings.Contains(strings.ToLower(rec.Template), "postgres") {
		_, _, _, _ = s.runCmd(ctx, nil, cloudRunSandboxBin, "--mode=container", "exec", "--", cid, "sh", "-c", "/mnt/shared/bin/start-pg.sh")
	}
	s.AppendLog(rec.SandboxID, fmt.Sprintf("[system] Started sandbox %s (template=%s, container=%s)", rec.SandboxID, rec.Template, cid))
	return nil
}

// Exec executes a command inside the Cloud Run Sandbox Runtime container.
func (s *CloudRunSandboxEngine) Exec(ctx context.Context, sandboxID string, cmd []string, workdir string, env map[string]string, background bool, timeout time.Duration) (string, string, int, int, error) {
	if err := ValidateID(sandboxID); err != nil {
		return "", "", -1, 0, err
	}
	cid := containerIDForSandbox(sandboxID)
	if workdir == "" {
		workdir = "/workspace"
	}
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	var envExports strings.Builder
	envExports.WriteString("export SANDBOX_ID=" + shellQuote(sandboxID) + "; ")
	for k, v := range env {
		if validEnvKey(k) {
			envExports.WriteString("export " + k + "=" + shellQuote(v) + "; ")
		}
	}
	cmdStr := strings.Join(cmd, " ")
	if len(cmd) > 1 && (cmd[0] != "sh" && cmd[0] != "/bin/sh" && cmd[0] != "bash") {
		// Quote individual args if passed as argv slice without shell
		allQuoted := make([]string, len(cmd))
		for i, a := range cmd {
			allQuoted[i] = shellQuote(a)
		}
		if len(cmd) == 3 && (cmd[0] == "sh" || cmd[0] == "/bin/sh" || cmd[0] == "bash") && cmd[1] == "-c" {
			cmdStr = cmd[2]
		} else if !strings.ContainsAny(cmd[0], " ;|&><$") {
			cmdStr = strings.Join(allQuoted, " ")
		}
	} else if len(cmd) == 3 && cmd[1] == "-c" {
		cmdStr = cmd[2]
	}

	quotedWorkdir := shellQuote(workdir)
	if background {
		logFile := s.logFileFor(sandboxID)
		bgCmd := envExports.String() + "mkdir -p " + quotedWorkdir + " && cd " + quotedWorkdir + " && nohup sh -c " + shellQuote(cmdStr) + " >> " + shellQuote(logFile) + " 2>&1 & echo $!"
		stdout, stderr, code, err := s.runCmd(ctx, nil, cloudRunSandboxBin, "--mode=container", "exec", "--", cid, "sh", "-c", bgCmd)
		pid, _ := strconv.Atoi(strings.TrimSpace(stdout))
		s.AppendLog(sandboxID, fmt.Sprintf("$ (bg) %s -> PID %d", cmdStr, pid))
		return stdout, stderr, code, pid, err
	}

	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	fullCmd := envExports.String() + "mkdir -p " + quotedWorkdir + " && cd " + quotedWorkdir + " && " + cmdStr
	stdout, stderr, code, err := s.runCmd(execCtx, nil, cloudRunSandboxBin, "--mode=container", "exec", "--", cid, "sh", "-c", fullCmd)
	s.AppendLog(sandboxID, fmt.Sprintf("$ %s (exit=%d)", cmdStr, code))
	if strings.TrimSpace(stdout) != "" {
		s.AppendLog(sandboxID, stdout)
	}
	if strings.TrimSpace(stderr) != "" {
		s.AppendLog(sandboxID, "[stderr] "+stderr)
	}
	if err != nil && code == -1 {
		return stdout, stderr, code, 0, err
	}
	return stdout, stderr, code, 0, nil
}

func streamCommandPipes(c *exec.Cmd, onChunk func(stream, data string)) (string, string, error) {
	stdoutPipe, err := c.StdoutPipe()
	if err != nil {
		return "", "", err
	}
	stderrPipe, err := c.StderrPipe()
	if err != nil {
		return "", "", err
	}
	if err := c.Start(); err != nil {
		return "", "", err
	}

	var outBuf, errBuf bytes.Buffer
	var cbMu sync.Mutex
	var wg sync.WaitGroup
	readPipe := func(r io.Reader, streamName string, dst *bytes.Buffer) {
		defer wg.Done()
		buf := make([]byte, 4096)
		for {
			n, rErr := r.Read(buf)
			if n > 0 {
				chunk := string(buf[:n])
				dst.Write(buf[:n])
				if onChunk != nil {
					cbMu.Lock()
					onChunk(streamName, chunk)
					cbMu.Unlock()
				}
			}
			if rErr != nil {
				break
			}
		}
	}
	wg.Add(2)
	go readPipe(stdoutPipe, "stdout", &outBuf)
	go readPipe(stderrPipe, "stderr", &errBuf)
	wg.Wait()
	waitErr := c.Wait()
	return outBuf.String(), errBuf.String(), waitErr
}

// ExecStream runs a command inside the Cloud Run Sandbox Runtime container and streams stdout/stderr chunks in real time.
func (s *CloudRunSandboxEngine) ExecStream(ctx context.Context, sandboxID string, cmd []string, workdir string, env map[string]string, timeout time.Duration, onChunk func(stream, data string)) (string, string, int, error) {
	if err := ValidateID(sandboxID); err != nil {
		return "", "", -1, err
	}
	if !s.IsAlive(ctx, sandboxID) {
		return "", "sandbox is not running", 1, fmt.Errorf("sandbox %s is not running", sandboxID)
	}
	cid := containerIDForSandbox(sandboxID)
	if workdir == "" {
		workdir = "/workspace"
	}
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	var envExports strings.Builder
	envExports.WriteString("export SANDBOX_ID=" + shellQuote(sandboxID) + "; ")
	for k, v := range env {
		if validEnvKey(k) {
			envExports.WriteString("export " + k + "=" + shellQuote(v) + "; ")
		}
	}
	cmdStr := strings.Join(cmd, " ")
	if len(cmd) > 1 && (cmd[0] != "sh" && cmd[0] != "/bin/sh" && cmd[0] != "bash") {
		allQuoted := make([]string, len(cmd))
		for i, a := range cmd {
			allQuoted[i] = shellQuote(a)
		}
		if len(cmd) == 3 && (cmd[0] == "sh" || cmd[0] == "/bin/sh" || cmd[0] == "bash") && cmd[1] == "-c" {
			cmdStr = cmd[2]
		} else if !strings.ContainsAny(cmd[0], " ;|&><$") {
			cmdStr = strings.Join(allQuoted, " ")
		}
	} else if len(cmd) == 3 && cmd[1] == "-c" {
		cmdStr = cmd[2]
	}

	quotedWorkdir := shellQuote(workdir)
	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	fullCmd := envExports.String() + "mkdir -p " + quotedWorkdir + " && cd " + quotedWorkdir + " && " + cmdStr
	c := exec.CommandContext(execCtx, cloudRunSandboxBin, "--mode=container", "exec", "--", cid, "sh", "-c", fullCmd)
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	c.Cancel = func() error {
		if c.Process != nil {
			_ = syscall.Kill(-c.Process.Pid, syscall.SIGKILL)
			return c.Process.Kill()
		}
		return nil
	}
	c.WaitDelay = 200 * time.Millisecond

	stdout, stderr, runErr := streamCommandPipes(c, onChunk)
	code := 0
	if errors.Is(execCtx.Err(), context.DeadlineExceeded) {
		code = 124
		if stderr == "" {
			stderr = fmt.Sprintf("command timed out after %s\n", timeout)
		}
		runErr = context.DeadlineExceeded
	} else if runErr != nil {
		if exitErr, ok := runErr.(*exec.ExitError); ok {
			code = exitErr.ExitCode()
		} else {
			code = 1
		}
	}
	s.AppendLog(sandboxID, fmt.Sprintf("$ [stream] %s (exit=%d)", cmdStr, code))
	if strings.TrimSpace(stdout) != "" {
		s.AppendLog(sandboxID, stdout)
	}
	if strings.TrimSpace(stderr) != "" {
		s.AppendLog(sandboxID, "[stderr] "+stderr)
	}
	return stdout, stderr, code, runErr
}

// DialSandboxPort connects to `port` inside the Cloud Run Sandbox Runtime container, bridging via `/mnt/shared` Unix socket if the guest bound to 127.0.0.1.
func (s *CloudRunSandboxEngine) DialSandboxPort(ctx context.Context, rec SandboxRecord, port int) (net.Conn, error) {
	if port <= 0 || port > 65535 {
		return nil, fmt.Errorf("invalid port %d", port)
	}
	cid := containerIDForSandbox(rec.SandboxID)
	ip := strings.TrimSpace(rec.SandboxIP)
	if ip == "" {
		out, _, code, err := s.runCmd(ctx, nil, cloudRunSandboxBin, "--mode=container", "exec", "--", cid, "sh", "-c", "hostname -I 2>/dev/null || ip -4 -o addr show scope global 2>/dev/null | awk '{print $4}' | cut -d/ -f1 | head -n1")
		if err == nil && code == 0 {
			fields := strings.Fields(out)
			if len(fields) > 0 {
				ip = fields[0]
			}
		}
	}
	sockPath := filepath.Join(s.sharedDir, fmt.Sprintf("proxy-%s-%d.sock", cid, port))
	bridgeStarted := false
	deadline := time.Now().Add(5 * time.Second)
	for {
		if ip != "" {
			if conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", ip, port), 250*time.Millisecond); err == nil {
				return conn, nil
			}
		}
		if conn, err := net.DialTimeout("unix", sockPath, 250*time.Millisecond); err == nil {
			return conn, nil
		}
		if !bridgeStarted {
			bridgeStarted = true
			pyBridge := fmt.Sprintf(
				`python3 -c 'import os,socket,threading
s=%q
try: os.unlink(s)
except OSError: pass
u=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
u.bind(s)
os.chmod(s,0o666)
u.listen(64)
def pipe(a,b):
  try:
    while True:
      d=a.recv(16384)
      if not d: break
      b.sendall(d)
  except Exception: pass
  finally:
    try: a.close()
    except Exception: pass
    try: b.close()
    except Exception: pass
while True:
  c,_=u.accept()
  try:
    t=socket.create_connection(("127.0.0.1",%d),timeout=2.0)
    threading.Thread(target=pipe,args=(c,t),daemon=True).start()
    threading.Thread(target=pipe,args=(t,c),daemon=True).start()
  except Exception:
    c.close()
' >/dev/null 2>&1 &`, sockPath, port)
			_, _, _, _ = s.runCmd(ctx, nil, cloudRunSandboxBin, "--mode=container", "exec", "--", cid, "sh", "-c", pyBridge)
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			break
		}
		time.Sleep(80 * time.Millisecond)
	}
	return nil, fmt.Errorf("port %d is not listening in sandbox %s", port, rec.SandboxID)
}

// ExportTar flushes any Postgres state and exports the sandbox's writable filesystem to destTarPath.
func (s *CloudRunSandboxEngine) ExportTar(ctx context.Context, rec SandboxRecord, destTarPath string) error {
	if err := ValidateID(rec.SandboxID); err != nil {
		return err
	}
	cid := containerIDForSandbox(rec.SandboxID)
	if strings.Contains(strings.ToLower(rec.Template), "postgres") || rec.PostgresPort > 0 {
		// Flush/checkpoint PostgreSQL cleanly before tar snapshot
		flushCmd := "if [ -f /pgdata/data/PG_VERSION ]; then PG_BIN=$(find /usr/lib/postgresql -name pg_ctl 2>/dev/null | head -n 1 | xargs -r dirname); if [ -n \"$PG_BIN\" ]; then LD_PRELOAD=/mnt/shared/bin/libfakeuid.so \"$PG_BIN/psql\" -U postgres -c 'CHECKPOINT;' >/dev/null 2>&1 || true; fi; fi; sync"
		_, _, _, _ = s.runCmd(ctx, nil, cloudRunSandboxBin, "--mode=container", "exec", "--", cid, "sh", "-c", flushCmd)
	}

	// Stream /workspace, /pgdata, and /persistent via tar first so paths are canonical across restores,
	// or fallback to `sandbox --mode=container tar <id> --file <destTarPath>`.
	exportStart := time.Now()
	cmd := exec.CommandContext(ctx, cloudRunSandboxBin, "--mode=container", "exec", "--", cid,
		"sh", "-c", "mkdir -p /workspace /persistent /pgdata/data && sync && tar --no-same-owner --warning=no-file-changed -cf - -C / workspace persistent pgdata")
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err == nil && outBuf.Len() > 0 {
		if err := os.MkdirAll(filepath.Dir(destTarPath), 0755); err != nil {
			return err
		}
		if err := os.WriteFile(destTarPath, outBuf.Bytes(), 0644); err != nil {
			return err
		}
		// Remember the archived entry manifest for a later ExportDeltaTar (Two-Phase Pre-Copy).
		if m, err := manifestFromTarReader(bytes.NewReader(outBuf.Bytes())); err == nil {
			s.manifests.record(rec.SandboxID, exportStart, m)
		}
		return nil
	}

	// Fallback to native `sandbox --mode=container tar`
	tCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	_, stderr, _, err := s.runCmd(tCtx, nil, cloudRunSandboxBin, "--mode=container", "tar", cid, "--file", destTarPath)
	if err != nil {
		return fmt.Errorf("export tar for %s failed: %w (stderr=%s)", rec.SandboxID, err, stderr)
	}
	return nil
}

// cloudRunDeltaRoots are the container directories captured by ExportTar / ExportDeltaTar.
var cloudRunDeltaRoots = []string{"workspace", "persistent", "pgdata"}

// ExportDeltaTar lists /workspace, /persistent and /pgdata inside the container, selects the
// entries created or modified since `since` (relative to the base manifests recorded by ExportTar),
// and streams only those entries out of the container with `tar --no-recursion -T -`.
func (s *CloudRunSandboxEngine) ExportDeltaTar(ctx context.Context, rec SandboxRecord, since time.Time, destTarPath string) ([]string, error) {
	if err := ValidateID(rec.SandboxID); err != nil {
		return nil, err
	}
	bases := s.manifests.since(rec.SandboxID, since)
	if len(bases) == 0 {
		return nil, fmt.Errorf("no base export manifest for sandbox %s at or after %s", rec.SandboxID, since.UTC().Format(time.RFC3339Nano))
	}
	cid := containerIDForSandbox(rec.SandboxID)
	// %y type, %s size, %T@ mtime (epoch seconds), %m permission bits (octal), %p path; NUL-separated.
	listCmd := "cd / && sync && find " + strings.Join(cloudRunDeltaRoots, " ") + " -printf '%y\\t%s\\t%T@\\t%m\\t%p\\0' 2>/dev/null; true"
	out, stderr, _, err := s.runCmd(ctx, nil, cloudRunSandboxBin, "--mode=container", "exec", "--", cid, "sh", "-c", listCmd)
	if err != nil {
		return nil, fmt.Errorf("list sandbox files for delta: %w (stderr=%s)", err, stderr)
	}
	current := make(map[string]bool)
	var selected []string
	for _, rawEntry := range strings.Split(out, "\x00") {
		fields := strings.SplitN(rawEntry, "\t", 5)
		if len(fields) != 5 {
			continue
		}
		rel := normalizeDeltaRel(fields[4])
		if rel == "" {
			continue
		}
		sig := fileSig{Type: 'x'}
		switch fields[0] {
		case "f":
			sig.Type = '0'
		case "d":
			sig.Type = '5'
		case "l":
			sig.Type = '2'
		}
		var sockMode os.FileMode
		if fields[0] == "s" {
			sockMode = os.ModeSocket
		}
		if skipTarEntry(rel, sockMode) {
			continue
		}
		if sig.Type == '0' {
			sig.Size, _ = strconv.ParseInt(fields[1], 10, 64)
		}
		if mtime, pErr := strconv.ParseFloat(fields[2], 64); pErr == nil {
			sec := int64(mtime)
			sig.ModTime = time.Unix(sec, int64((mtime-float64(sec))*1e9))
		}
		if mode, pErr := strconv.ParseInt(fields[3], 8, 64); pErr == nil {
			sig.Mode = mode & 0777
		}
		current[rel] = true
		if entryNeedsDelta(rel, sig, bases, since) {
			selected = append(selected, rel)
		}
	}
	if len(current) == 0 {
		return nil, fmt.Errorf("empty file listing for sandbox %s; refusing delta export", rec.SandboxID)
	}
	deleted := deletedSinceBases(current, bases)
	if len(selected) == 0 {
		return deleted, writeEmptyTar(destTarPath)
	}
	sort.Strings(selected)
	stdin := []byte(strings.Join(selected, "\x00") + "\x00")
	tarOut, tarErr, code, err := s.runCmd(ctx, stdin, cloudRunSandboxBin, "--mode=container", "exec", "--", cid,
		"sh", "-c", "cd / && tar --no-same-owner --warning=no-file-changed --ignore-failed-read --no-recursion --null -T - -cf -")
	// GNU tar exits 1 when a file changed while being read; the archive is still valid.
	if (err != nil && code != 1) || len(tarOut) == 0 {
		return nil, fmt.Errorf("delta tar export for %s failed: %v (exit=%d, stderr=%s)", rec.SandboxID, err, code, tarErr)
	}
	if err := os.MkdirAll(filepath.Dir(destTarPath), 0755); err != nil {
		return nil, err
	}
	if err := os.WriteFile(destTarPath, []byte(tarOut), 0644); err != nil {
		return nil, err
	}
	return deleted, nil
}

// ApplyDeltaTar removes deletedPaths and extracts deltaTarPath inside the prestaged container,
// then restarts PostgreSQL for postgres sandboxes (it is quiesced during pre-staging).
func (s *CloudRunSandboxEngine) ApplyDeltaTar(ctx context.Context, rec SandboxRecord, deltaTarPath string, deletedPaths []string) error {
	if err := ValidateID(rec.SandboxID); err != nil {
		return err
	}
	rels, err := validateDeletedPaths(deletedPaths)
	if err != nil {
		return err
	}
	for _, rel := range rels {
		allowed := false
		for _, root := range cloudRunDeltaRoots {
			if strings.HasPrefix(rel, root+"/") {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Errorf("%w: deleted path %q is outside %v", ErrPathTraversal, rel, cloudRunDeltaRoots)
		}
	}
	cid := containerIDForSandbox(rec.SandboxID)
	if len(rels) > 0 {
		stdin := []byte(strings.Join(rels, "\x00") + "\x00")
		if _, stderr, _, err := s.runCmd(ctx, stdin, cloudRunSandboxBin, "--mode=container", "exec", "--", cid,
			"sh", "-c", "cd / && xargs -0 -r rm -rf --"); err != nil {
			return fmt.Errorf("apply delta deletions: %w (stderr=%s)", err, stderr)
		}
	}
	if deltaTarPath != "" {
		b, err := os.ReadFile(deltaTarPath)
		if err != nil {
			return err
		}
		if len(b) > 0 {
			if _, stderr, _, err := s.runCmd(ctx, b, cloudRunSandboxBin, "--mode=container", "exec", "--", cid,
				"sh", "-c", "cd / && tar --no-same-owner -xf - -C /"); err != nil {
				return fmt.Errorf("apply delta tar: %w (stderr=%s)", err, stderr)
			}
		}
	}
	stateFile := s.stateFileFor(rec.SandboxID)
	_ = os.WriteFile(stateFile, []byte("ACTIVE\n"), 0666)
	if strings.Contains(strings.ToLower(rec.Template), "postgres") {
		_, _, _, _ = s.runCmd(ctx, nil, cloudRunSandboxBin, "--mode=container", "exec", "--", cid, "sh", "-c", "/mnt/shared/bin/start-pg.sh")
	}
	s.AppendLog(rec.SandboxID, fmt.Sprintf("[system] Applied pre-copy delta (%d deleted paths)", len(rels)))
	return nil
}

// Quiesce cleanly stops PostgreSQL inside the Cloud Run Sandbox Runtime container prior to suspend or migration.
func (s *CloudRunSandboxEngine) Quiesce(ctx context.Context, sandboxID string) error {
	if err := ValidateID(sandboxID); err != nil {
		return err
	}
	cid := containerIDForSandbox(sandboxID)
	stopPg := "if [ -f /pgdata/data/PG_VERSION ]; then PG_BIN=$(find /usr/lib/postgresql -name pg_ctl 2>/dev/null | head -n 1 | xargs -r dirname); if [ -n \"$PG_BIN\" ]; then LD_PRELOAD=/mnt/shared/bin/libfakeuid.so \"$PG_BIN/pg_ctl\" -D /pgdata/data -m fast -w stop >/dev/null 2>&1 || true; fi; fi; sync"
	_, _, _, _ = s.runCmd(ctx, nil, cloudRunSandboxBin, "--mode=container", "exec", "--", cid, "sh", "-c", stopPg)
	return nil
}

// Fork performs a same-host CoW fork via `sandbox --mode=container fork` with tar fallback.
func (s *CloudRunSandboxEngine) Fork(ctx context.Context, sourceSandboxID string, targetRec SandboxRecord) (string, error) {
	if err := ValidateID(sourceSandboxID); err != nil {
		return "", err
	}
	if err := ValidateID(targetRec.SandboxID); err != nil {
		return "", err
	}
	srcCID := containerIDForSandbox(sourceSandboxID)
	dstCID := containerIDForSandbox(targetRec.SandboxID)
	if s.IsAlive(ctx, sourceSandboxID) {
		forkArgs := []string{cloudRunSandboxBin, "--mode=container", "fork", "--detach"}
		if targetRec.AllowEgress {
			forkArgs = append(forkArgs, "--allow-egress")
		}
		forkArgs = append(forkArgs, srcCID, dstCID)
		if _, _, _, err := s.runCmd(ctx, nil, forkArgs...); err == nil {
			dstState := s.stateFileFor(targetRec.SandboxID)
			_ = os.WriteFile(dstState, []byte("ACTIVE\n"), 0666)
			_ = os.Chmod(dstState, 0666)
			rebind := "mkdir -p /workspace /persistent /var/run/instance && ln -sf " + dstState + " /var/run/instance/state && ln -sf " + dstState + " /workspace/.instance_state"
			_, _, _, _ = s.runCmd(ctx, nil, cloudRunSandboxBin, "--mode=container", "exec", "--", dstCID, "sh", "-c", rebind)
			s.AppendLog(targetRec.SandboxID, fmt.Sprintf("[system] CoW forked %s from %s", targetRec.SandboxID, sourceSandboxID))
			return "cloudrun_cow_fork", nil
		}
	}
	// Fallback to tar export + Start
	tmpTar := filepath.Join(os.TempDir(), "fork-"+NewRandomID("")+".tar")
	defer os.Remove(tmpTar)
	if err := s.ExportTar(ctx, SandboxRecord{SandboxID: sourceSandboxID, Template: targetRec.Template}, tmpTar); err != nil {
		return "", err
	}
	if err := s.Start(ctx, targetRec, tmpTar); err != nil {
		return "", err
	}
	return "tar_overlay_clone", nil
}

// CopyFile uploads or downloads a file inside the Cloud Run Sandbox Runtime container, enforcing safe path resolution.
func (s *CloudRunSandboxEngine) CopyFile(ctx context.Context, sandboxID string, req FileCopyRequest) (FileCopyResponse, error) {
	if err := ValidateID(sandboxID); err != nil {
		return FileCopyResponse{}, err
	}
	// Validate target path inside the container's allowed root
	cleanPath, err := SafeResolvePath("/", req.Path)
	if err != nil {
		return FileCopyResponse{}, err
	}
	if strings.Contains(req.Path, "..") {
		return FileCopyResponse{}, fmt.Errorf("%w: %q contains '..'", ErrPathTraversal, req.Path)
	}
	cid := containerIDForSandbox(sandboxID)
	if strings.EqualFold(req.Direction, "download") {
		stdout, stderr, code, err := s.runCmd(ctx, nil, cloudRunSandboxBin, "--mode=container", "exec", "--", cid, "cat", cleanPath)
		if err != nil || code != 0 {
			return FileCopyResponse{}, fmt.Errorf("download %s failed: %s", cleanPath, stderr)
		}
		return FileCopyResponse{
			SandboxID: sandboxID,
			Path:      cleanPath,
			Content:   []byte(stdout),
			SizeBytes: int64(len(stdout)),
		}, nil
	}
	dir := filepath.Dir(cleanPath)
	writeCmd := "mkdir -p " + shellQuote(dir) + " && cat > " + shellQuote(cleanPath)
	_, stderr, code, err := s.runCmd(ctx, req.Content, cloudRunSandboxBin, "--mode=container", "exec", "--", cid, "sh", "-c", writeCmd)
	if err != nil || code != 0 {
		return FileCopyResponse{}, fmt.Errorf("upload %s failed: %s", cleanPath, stderr)
	}
	s.AppendLog(sandboxID, fmt.Sprintf("[cp] Uploaded %d bytes to %s", len(req.Content), cleanPath))
	return FileCopyResponse{
		SandboxID: sandboxID,
		Path:      cleanPath,
		SizeBytes: int64(len(req.Content)),
	}, nil
}

// ReadLogs returns the sandbox log buffer.
func (s *CloudRunSandboxEngine) ReadLogs(_ context.Context, sandboxID string) (string, error) {
	if err := ValidateID(sandboxID); err != nil {
		return "", err
	}
	b, err := os.ReadFile(s.logFileFor(sandboxID))
	if os.IsNotExist(err) {
		return "", nil
	}
	return string(b), err
}

// AppendLog writes a timestamped log line for the sandbox.
func (s *CloudRunSandboxEngine) AppendLog(sandboxID, line string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = os.MkdirAll(s.sharedDir, 0777)
	ts := fmt.Sprintf("[%s] %s\n", time.Now().UTC().Format("15:04:05.000"), strings.TrimRight(line, "\n"))
	f, err := os.OpenFile(s.logFileFor(sandboxID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err == nil {
		_, _ = f.WriteString(ts)
		_ = f.Close()
	}
}

// Stop deletes the Cloud Run Sandbox Runtime container.
func (s *CloudRunSandboxEngine) Stop(ctx context.Context, sandboxID string) error {
	if err := ValidateID(sandboxID); err != nil {
		return err
	}
	cid := containerIDForSandbox(sandboxID)
	_, _, _, _ = s.runCmd(ctx, nil, cloudRunSandboxBin, "--mode=container", "delete", cid)
	_ = os.Remove(s.stateFileFor(sandboxID))
	s.manifests.forget(sandboxID)
	return nil
}

// IsAlive probes whether the Cloud Run Sandbox Runtime container is live.
func (s *CloudRunSandboxEngine) IsAlive(ctx context.Context, sandboxID string) bool {
	if ValidateID(sandboxID) != nil {
		return false
	}
	cid := containerIDForSandbox(sandboxID)
	cCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, _, code, err := s.runCmd(cCtx, nil, cloudRunSandboxBin, "--mode=container", "exec", "--", cid, "true")
	return err == nil && code == 0
}

// CheckInGuestState reads `/mnt/shared/state-<cid>` to detect in-guest IDLE or STOP signals.
func (s *CloudRunSandboxEngine) CheckInGuestState(sandboxID string) string {
	b, err := os.ReadFile(s.stateFileFor(sandboxID))
	if err != nil {
		return ""
	}
	return strings.ToUpper(strings.TrimSpace(string(b)))
}

// LocalProcessEngine implements SandboxEngine using isolated local directories and subshells.
// It is used in hermetic unit tests and in `--mode=local` when `/usr/local/gcp/bin/sandbox` is not installed.
type LocalProcessEngine struct {
	rootDir     string
	mu          sync.RWMutex
	alive       map[string]bool
	allowEgress map[string]bool
	logs        map[string][]string
	bgPIDs      map[string][]int
	pgPorts     map[string]int
	manifests   manifestTracker
}

// NewLocalProcessEngine creates a hermetic local engine rooted at rootDir.
func NewLocalProcessEngine(rootDir string) *LocalProcessEngine {
	_ = os.MkdirAll(rootDir, 0755)
	if os.Getuid() == 0 {
		binDir := filepath.Join(sharedDirPath, "bin")
		_ = os.MkdirAll(binDir, 0777)
		_ = decodeGzipBase64ToFile(libFakeUIDBase64Gzip, filepath.Join(binDir, "libfakeuid.so"), 0755)
		_ = decodeGzipBase64ToFile(tcpProxyBase64Gzip, filepath.Join(binDir, "tcpproxy"), 0755)
	}
	return &LocalProcessEngine{
		rootDir:     rootDir,
		alive:       make(map[string]bool),
		allowEgress: make(map[string]bool),
		logs:        make(map[string][]string),
		bgPIDs:      make(map[string][]int),
		pgPorts:     make(map[string]int),
		manifests:   manifestTracker{m: make(map[string][]exportManifest)},
	}
}

// SetAllowEgress updates the egress policy for a running sandbox in LocalProcessEngine.
func (l *LocalProcessEngine) SetAllowEgress(sandboxID string, allow bool) {
	l.mu.Lock()
	l.allowEgress[sandboxID] = allow
	l.mu.Unlock()
}

// EngineType returns "local".
func (l *LocalProcessEngine) EngineType() string {
	return "local"
}

func (l *LocalProcessEngine) sandboxRoot(sandboxID string) (string, error) {
	if err := ValidateID(sandboxID); err != nil {
		return "", err
	}
	return SafeResolvePath(l.rootDir, sandboxID)
}

func (l *LocalProcessEngine) postgresPortFor(sandboxID string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if p, ok := l.pgPorts[sandboxID]; ok && p > 0 {
		return p
	}
	sum := sha256.Sum256([]byte(sandboxID))
	port := 15432 + (int(sum[0])<<8|int(sum[1]))%10000
	used := make(map[int]bool)
	for _, p := range l.pgPorts {
		used[p] = true
	}
	for used[port] {
		port++
	}
	l.pgPorts[sandboxID] = port
	return port
}

func findLocalPostgresBinDir() string {
	for _, ver := range []string{"16", "15", "17", "14"} {
		cand := "/usr/lib/postgresql/" + ver + "/bin"
		if _, err := os.Stat(filepath.Join(cand, "pg_ctl")); err == nil {
			return cand
		}
	}
	return ""
}

// Start initializes an isolated directory tree for the sandbox and extracts importTarPath if provided.
func (l *LocalProcessEngine) Start(_ context.Context, rec SandboxRecord, importTarPath string) error {
	sbxRoot, err := l.sandboxRoot(rec.SandboxID)
	if err != nil {
		return err
	}
	for _, sub := range []string{"workspace", "persistent", "pgdata/data", "pgdata/run", "var/run/instance", "var/log", "root", "tmp"} {
		if err := os.MkdirAll(filepath.Join(sbxRoot, sub), 0755); err != nil {
			return err
		}
	}
	_ = os.Chmod(filepath.Join(sbxRoot, "root"), 0700)
	_ = os.Chmod(filepath.Join(sbxRoot, "tmp"), 0777)

	if importTarPath != "" {
		if err := extractTarToDir(importTarPath, sbxRoot); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("extract import tar: %w", err)
		}
	}

	// Write ACTIVE state AFTER extracting importTarPath so resuming an IDLE-suspended sandbox
	// never immediately re-triggers IDLE auto-suspend!
	stateFile := filepath.Join(sbxRoot, "var/run/instance/state")
	_ = os.WriteFile(stateFile, []byte("ACTIVE\n"), 0644)
	_ = os.WriteFile(filepath.Join(sbxRoot, "workspace/.instance_state"), []byte("ACTIVE\n"), 0644)

	if err := l.startLocalPostgresIfNeeded(rec, sbxRoot, importTarPath != ""); err != nil {
		return err
	}
	l.mu.Lock()
	l.alive[rec.SandboxID] = true
	l.allowEgress[rec.SandboxID] = rec.AllowEgress
	l.mu.Unlock()
	ensureLocalEgressGuards(sbxRoot)
	l.AppendLog(rec.SandboxID, fmt.Sprintf("[system] Started sandbox %s (template=%s, allow_egress=%v)", rec.SandboxID, rec.Template, rec.AllowEgress))
	return nil
}

// startLocalPostgresIfNeeded (re)starts the sandbox-local PostgreSQL daemon if this is a postgres
// sandbox (template or restored PG_VERSION). imported=true means pgdata was restored from a tar.
func (l *LocalProcessEngine) startLocalPostgresIfNeeded(rec SandboxRecord, sbxRoot string, imported bool) error {
	isPg := strings.Contains(strings.ToLower(rec.Template), "postgres")
	if !isPg {
		if _, statErr := os.Stat(filepath.Join(sbxRoot, "pgdata/data/PG_VERSION")); statErr == nil {
			isPg = true
		}
	}
	if isPg {
		pgDataDir := filepath.Join(sbxRoot, "pgdata/data")
		pgRunDir := filepath.Join(sbxRoot, "pgdata/run")
		pgLogFile := filepath.Join(sbxRoot, "pgdata/postgres.log")
		_ = os.Remove(filepath.Join(pgDataDir, "postmaster.pid"))
		_ = os.Remove(filepath.Join(pgDataDir, "postmaster.opts"))
		pgBinDir := findLocalPostgresBinDir()
		if os.Getuid() == 0 && pgBinDir != "" && (os.Getenv("K_SERVICE") != "" || os.Getenv("SBX_ENABLE_REAL_POSTGRES") == "1") {
			port := l.postgresPortFor(rec.SandboxID)
			shellBin := "/usr/bin/env"
			_, confErr := os.Stat(filepath.Join(pgDataDir, "postgresql.conf"))
			_, verErr := os.Stat(filepath.Join(pgDataDir, "PG_VERSION"))
			if os.IsNotExist(confErr) || os.IsNotExist(verErr) {
				if imported && rec.Epoch > 1 {
					return fmt.Errorf("refusing to re-initdb hydrated postgres sandbox %s (epoch=%d): missing PG_VERSION or postgresql.conf after tar restore", rec.SandboxID, rec.Epoch)
				}
				_ = os.RemoveAll(pgDataDir)
				_ = os.MkdirAll(pgDataDir, 0700)
			}
			_ = os.MkdirAll(pgRunDir, 0777)
			_ = os.Chmod(pgRunDir, 0777)
			initAndStart := fmt.Sprintf(
				"chown -R postgres:postgres %s && chmod 700 %s && chmod 777 %s && "+
					"if [ ! -f %s/postgresql.conf ] || [ ! -f %s/PG_VERSION ]; then rm -rf %s/* && su -s /bin/sh postgres -c %s; fi && "+
					"su -s /bin/sh postgres -c %s",
				shellQuote(filepath.Join(sbxRoot, "pgdata")),
				shellQuote(pgDataDir),
				shellQuote(pgRunDir),
				shellQuote(pgDataDir),
				shellQuote(pgDataDir),
				shellQuote(pgDataDir),
				shellQuote(fmt.Sprintf("%s/initdb -D %s -U postgres --auth=trust --no-instructions >/dev/null 2>&1", pgBinDir, shellQuote(pgDataDir))),
				shellQuote(fmt.Sprintf("%s/pg_ctl -D %s -l %s -o %s -w start >/dev/null 2>&1", pgBinDir, shellQuote(pgDataDir), shellQuote(pgLogFile), shellQuote(fmt.Sprintf("-k %s -p %d", pgRunDir, port)))),
			)
			_ = exec.Command(shellBin, "sh", "-c", initAndStart).Run()
		} else {
			pgVer := filepath.Join(pgDataDir, "PG_VERSION")
			if _, err := os.Stat(pgVer); os.IsNotExist(err) {
				_ = os.WriteFile(pgVer, []byte("16\n"), 0644)
			}
		}
	}
	return nil
}

func ensureLocalEgressGuards(sbxRoot string) {
	pyGuardDir := filepath.Join(sbxRoot, "var/run/instance/pyguard")
	binGuardDir := filepath.Join(sbxRoot, "var/run/instance/egress_block_bin")
	_ = os.MkdirAll(pyGuardDir, 0755)
	_ = os.MkdirAll(binGuardDir, 0755)

	siteCustomize := `import os, socket
if os.environ.get("SBX_ALLOW_EGRESS") == "false":
    _orig_connect = socket.socket.connect
    _orig_connect_ex = socket.socket.connect_ex
    def _is_loopback(addr):
        if not isinstance(addr, tuple) or len(addr) < 1:
            return True
        h = str(addr[0]).strip().lower()
        return h in ("127.0.0.1", "localhost", "::1", "") or h.startswith("127.")
    def _guarded_connect(self, address):
        if not _is_loopback(address):
            raise PermissionError(f"Network egress to {address} is blocked (sandbox allow_egress=false; use --allow-egress to enable)")
        return _orig_connect(self, address)
    def _guarded_connect_ex(self, address):
        if not _is_loopback(address):
            raise PermissionError(f"Network egress to {address} is blocked (sandbox allow_egress=false; use --allow-egress to enable)")
        return _orig_connect_ex(self, address)
    socket.socket.connect = _guarded_connect
    socket.socket.connect_ex = _guarded_connect_ex
`
	_ = os.WriteFile(filepath.Join(pyGuardDir, "sitecustomize.py"), []byte(siteCustomize), 0644)

	for _, tool := range []string{"curl", "wget"} {
		realBin := "/usr/bin/" + tool
		if _, err := os.Stat(realBin); err != nil {
			realBin = "/bin/" + tool
		}
		wrapper := fmt.Sprintf(`#!/bin/sh
if [ "$SBX_ALLOW_EGRESS" = "false" ]; then
  case "$*" in
    *127.0.0.1*|*localhost*) ;;
    *)
      echo "Network egress is blocked (sandbox allow_egress=false; use --allow-egress to enable)" >&2
      exit 7
      ;;
  esac
fi
exec %s "$@"
`, realBin)
		_ = os.WriteFile(filepath.Join(binGuardDir, tool), []byte(wrapper), 0755)
	}
}

// Exec runs a command inside the sandbox's isolated directory (or via `sandbox do [--allow-egress]` when available).
func (l *LocalProcessEngine) Exec(ctx context.Context, sandboxID string, cmd []string, workdir string, env map[string]string, background bool, timeout time.Duration) (string, string, int, int, error) {
	sbxRoot, err := l.sandboxRoot(sandboxID)
	if err != nil {
		return "", "", -1, 0, err
	}
	l.mu.RLock()
	isLive := l.alive[sandboxID]
	pgPort := l.pgPorts[sandboxID]
	allowEgress, hasEgress := l.allowEgress[sandboxID]
	l.mu.RUnlock()
	if !hasEgress {
		allowEgress = true
	}
	if v, ok := env["SBX_ALLOW_EGRESS"]; ok && strings.TrimSpace(v) != "" {
		allowEgress = strings.EqualFold(strings.TrimSpace(v), "true") || strings.TrimSpace(v) == "1"
	}
	if !isLive {
		return "", "sandbox is not running", 1, 0, fmt.Errorf("sandbox %s is not running", sandboxID)
	}
	if workdir == "" {
		workdir = "/workspace"
	}
	hostWorkdir, err := SafeResolvePath(sbxRoot, workdir)
	if err != nil {
		return "", "", -1, 0, err
	}
	_ = os.MkdirAll(hostWorkdir, 0755)
	ensureLocalEgressGuards(sbxRoot)

	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	// Build shell command string while preserving quoting for multi-arg commands (e.g. `python3 -c "..."`)
	// and rewriting `/workspace`, `/persistent`, `/pgdata`, `/var/run/instance`, and `/root` inside arguments.
	rewriteArg := func(s string) string {
		if strings.Contains(s, sbxRoot) {
			return s
		}
		out := s
		for _, prefix := range []string{"/workspace", "/persistent", "/pgdata", "/var/run/instance", "/root"} {
			out = strings.ReplaceAll(out, prefix, filepath.Join(sbxRoot, strings.TrimPrefix(prefix, "/")))
		}
		return out
	}

	var rewritten string
	cmdDisplay := strings.Join(cmd, " ")
	if len(cmd) >= 3 && (cmd[0] == "sh" || cmd[0] == "/bin/sh" || cmd[0] == "bash" || cmd[0] == "/bin/bash") && cmd[1] == "-c" {
		rewritten = rewriteArg(strings.Join(cmd[2:], " "))
	} else if len(cmd) == 1 {
		rewritten = rewriteArg(cmd[0])
	} else {
		quoted := make([]string, 0, len(cmd))
		for _, a := range cmd {
			quoted = append(quoted, shellQuote(rewriteArg(a)))
		}
		rewritten = strings.Join(quoted, " ")
	}

	shellBin := os.Getenv("SBX_LOCAL_SHELL")
	if shellBin == "" {
		shellBin = "/usr/bin/env"
	}
	_ = os.MkdirAll(filepath.Join(sbxRoot, "root"), 0700)
	_ = os.MkdirAll(filepath.Join(sbxRoot, "tmp"), 0777)
	procEnv := append(os.Environ(),
		"SANDBOX_ID="+sandboxID,
		"SBX_ALLOW_EGRESS="+strconv.FormatBool(allowEgress),
		"HOME="+filepath.Join(sbxRoot, "root"),
		"TMPDIR="+filepath.Join(sbxRoot, "tmp"),
		"WORKSPACE="+filepath.Join(sbxRoot, "workspace"),
		"PGDATA="+filepath.Join(sbxRoot, "pgdata/data"),
		"INSTANCE_STATE_FILE="+filepath.Join(sbxRoot, "var/run/instance/state"),
	)
	if !allowEgress {
		pyGuardDir := filepath.Join(sbxRoot, "var/run/instance/pyguard")
		binGuardDir := filepath.Join(sbxRoot, "var/run/instance/egress_block_bin")
		procEnv = append(procEnv,
			"PYTHONPATH="+pyGuardDir+":"+os.Getenv("PYTHONPATH"),
			"PATH="+binGuardDir+":"+os.Getenv("PATH"),
			"HTTP_PROXY=http://127.0.0.1:1",
			"HTTPS_PROXY=http://127.0.0.1:1",
			"NO_PROXY=localhost,127.0.0.1,::1",
		)
	}
	if pgPort > 0 {
		procEnv = append(procEnv,
			"PGHOST="+filepath.Join(sbxRoot, "pgdata/run"),
			"PGPORT="+strconv.Itoa(pgPort),
			"PGUSER=postgres",
			"PGDATABASE=postgres",
		)
	}
	for k, v := range env {
		if validEnvKey(k) {
			procEnv = append(procEnv, fmt.Sprintf("%s=%s", k, v))
		}
	}

	if background {
		// Unbound from HTTP request context so background daemon survives after HTTP handler returns!
		bgCmd := exec.Command(shellBin, "sh", "-c", rewritten)
		bgCmd.Dir = hostWorkdir
		bgCmd.Env = procEnv
		bgCmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		logFilePath := filepath.Join(sbxRoot, "var/log/sandbox.log")
		_ = os.MkdirAll(filepath.Dir(logFilePath), 0755)
		logFile, _ := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if logFile != nil {
			bgCmd.Stdout = logFile
			bgCmd.Stderr = logFile
		}
		if err := bgCmd.Start(); err != nil {
			if logFile != nil {
				_ = logFile.Close()
			}
			return "", err.Error(), 1, 0, err
		}
		if logFile != nil {
			_ = logFile.Close()
		}
		pid := 0
		if bgCmd.Process != nil {
			pid = bgCmd.Process.Pid
			l.mu.Lock()
			l.bgPIDs[sandboxID] = append(l.bgPIDs[sandboxID], pid)
			l.mu.Unlock()
			go func() { _ = bgCmd.Wait() }()
		}
		l.AppendLog(sandboxID, fmt.Sprintf("$ (bg) %s -> PID %d", cmdDisplay, pid))
		return fmt.Sprintf("%d\n", pid), "", 0, pid, nil
	}

	// If `/usr/local/gcp/bin/sandbox` is available on the host and `sandbox do` is enabled,
	// invoke `sandbox do [--allow-egress] --write --mount=type=bind,source=<sbxRoot>,destination=<sbxRoot> -w <hostWorkdir> ...`
	if _, statErr := os.Stat(cloudRunSandboxBin); statErr == nil && pgPort == 0 && os.Getenv("SBX_DISABLE_SANDBOX_DO") != "1" {
		doArgs := []string{
			"do",
			"--write",
			"--mount=type=bind,source=" + sbxRoot + ",destination=" + sbxRoot,
			"-w", hostWorkdir,
		}
		if allowEgress {
			doArgs = append(doArgs, "--allow-egress")
		}
		for _, kv := range []string{
			"SANDBOX_ID=" + sandboxID,
			"SBX_ALLOW_EGRESS=" + strconv.FormatBool(allowEgress),
			"HOME=" + filepath.Join(sbxRoot, "root"),
			"TMPDIR=" + filepath.Join(sbxRoot, "tmp"),
			"WORKSPACE=" + filepath.Join(sbxRoot, "workspace"),
			"INSTANCE_STATE_FILE=" + filepath.Join(sbxRoot, "var/run/instance/state"),
		} {
			doArgs = append(doArgs, "--env", kv)
		}
		for k, v := range env {
			if validEnvKey(k) {
				doArgs = append(doArgs, "--env", fmt.Sprintf("%s=%s", k, v))
			}
		}
		doArgs = append(doArgs, "--", shellBin, "sh", "-c", rewritten)

		doCtx, doCancel := context.WithTimeout(ctx, timeout)
		doCmd := exec.CommandContext(doCtx, cloudRunSandboxBin, doArgs...)
		doCmd.Dir = hostWorkdir
		doCmd.Env = procEnv
		var doOut, doErr bytes.Buffer
		doCmd.Stdout = &doOut
		doCmd.Stderr = &doErr
		errDo := doCmd.Run()
		doCancel()

		stderrStr := doErr.String()
		isSetupFailure := errDo != nil && (strings.Contains(stderrStr, "failed to create") ||
			strings.Contains(stderrStr, "failed to start container") ||
			strings.Contains(stderrStr, "runsc") ||
			strings.Contains(stderrStr, "Overlay") ||
			strings.Contains(stderrStr, "network namespace"))
		if !isSetupFailure {
			exitCode := 0
			if errors.Is(doCtx.Err(), context.DeadlineExceeded) {
				exitCode = 124
				errDo = context.DeadlineExceeded
			} else if errDo != nil {
				if exitErr, ok := errDo.(*exec.ExitError); ok {
					exitCode = exitErr.ExitCode()
				} else {
					exitCode = 1
				}
			}
			l.AppendLog(sandboxID, fmt.Sprintf("$ [sandbox do --allow-egress=%v] %s (exit=%d)", allowEgress, cmdDisplay, exitCode))
			if doOut.Len() > 0 {
				l.AppendLog(sandboxID, doOut.String())
			}
			return doOut.String(), stderrStr, exitCode, 0, errDo
		}
	}

	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	c := exec.CommandContext(execCtx, shellBin, "sh", "-c", rewritten)
	c.Dir = hostWorkdir
	c.Env = procEnv
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	c.Cancel = func() error {
		if c.Process != nil {
			_ = syscall.Kill(-c.Process.Pid, syscall.SIGKILL)
			return c.Process.Kill()
		}
		return nil
	}
	c.WaitDelay = 200 * time.Millisecond

	var outBuf, errBuf bytes.Buffer
	c.Stdout = &outBuf
	c.Stderr = &errBuf
	runErr := c.Run()
	exitCode := 0
	if errors.Is(execCtx.Err(), context.DeadlineExceeded) {
		exitCode = 124
		if errBuf.Len() == 0 {
			errBuf.WriteString(fmt.Sprintf("command timed out after %s\n", timeout))
		}
		runErr = context.DeadlineExceeded
	} else if runErr != nil {
		if exitErr, ok := runErr.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			exitCode = 1
		}
	}
	l.AppendLog(sandboxID, fmt.Sprintf("$ [allow-egress=%v] %s (exit=%d)", allowEgress, cmdDisplay, exitCode))
	if outBuf.Len() > 0 {
		l.AppendLog(sandboxID, outBuf.String())
	}
	return outBuf.String(), errBuf.String(), exitCode, 0, runErr
}

// ExecStream runs a command inside the local sandbox directory and streams stdout/stderr chunks to onChunk in real time.
func (l *LocalProcessEngine) ExecStream(ctx context.Context, sandboxID string, cmd []string, workdir string, env map[string]string, timeout time.Duration, onChunk func(stream, data string)) (string, string, int, error) {
	sbxRoot, err := l.sandboxRoot(sandboxID)
	if err != nil {
		return "", "", -1, err
	}
	l.mu.RLock()
	isLive := l.alive[sandboxID]
	pgPort := l.pgPorts[sandboxID]
	allowEgress, hasEgress := l.allowEgress[sandboxID]
	l.mu.RUnlock()
	if !hasEgress {
		allowEgress = true
	}
	if v, ok := env["SBX_ALLOW_EGRESS"]; ok && strings.TrimSpace(v) != "" {
		allowEgress = strings.EqualFold(strings.TrimSpace(v), "true") || strings.TrimSpace(v) == "1"
	}
	if !isLive {
		return "", "sandbox is not running", 1, fmt.Errorf("sandbox %s is not running", sandboxID)
	}
	if workdir == "" {
		workdir = "/workspace"
	}
	hostWorkdir, err := SafeResolvePath(sbxRoot, workdir)
	if err != nil {
		return "", "", -1, err
	}
	_ = os.MkdirAll(hostWorkdir, 0755)
	ensureLocalEgressGuards(sbxRoot)

	if timeout <= 0 {
		timeout = 60 * time.Second
	}

	rewriteArg := func(s string) string {
		if strings.Contains(s, sbxRoot) {
			return s
		}
		out := s
		for _, prefix := range []string{"/workspace", "/persistent", "/pgdata", "/var/run/instance", "/root"} {
			out = strings.ReplaceAll(out, prefix, filepath.Join(sbxRoot, strings.TrimPrefix(prefix, "/")))
		}
		return out
	}

	var rewritten string
	cmdDisplay := strings.Join(cmd, " ")
	if len(cmd) >= 3 && (cmd[0] == "sh" || cmd[0] == "/bin/sh" || cmd[0] == "bash" || cmd[0] == "/bin/bash") && cmd[1] == "-c" {
		rewritten = rewriteArg(strings.Join(cmd[2:], " "))
	} else if len(cmd) == 1 {
		rewritten = rewriteArg(cmd[0])
	} else {
		quoted := make([]string, 0, len(cmd))
		for _, a := range cmd {
			quoted = append(quoted, shellQuote(rewriteArg(a)))
		}
		rewritten = strings.Join(quoted, " ")
	}

	shellBin := os.Getenv("SBX_LOCAL_SHELL")
	if shellBin == "" {
		shellBin = "/usr/bin/env"
	}
	_ = os.MkdirAll(filepath.Join(sbxRoot, "root"), 0700)
	_ = os.MkdirAll(filepath.Join(sbxRoot, "tmp"), 0777)
	procEnv := append(os.Environ(),
		"SANDBOX_ID="+sandboxID,
		"SBX_ALLOW_EGRESS="+strconv.FormatBool(allowEgress),
		"HOME="+filepath.Join(sbxRoot, "root"),
		"TMPDIR="+filepath.Join(sbxRoot, "tmp"),
		"WORKSPACE="+filepath.Join(sbxRoot, "workspace"),
		"PGDATA="+filepath.Join(sbxRoot, "pgdata/data"),
		"INSTANCE_STATE_FILE="+filepath.Join(sbxRoot, "var/run/instance/state"),
	)
	if !allowEgress {
		pyGuardDir := filepath.Join(sbxRoot, "var/run/instance/pyguard")
		binGuardDir := filepath.Join(sbxRoot, "var/run/instance/egress_block_bin")
		procEnv = append(procEnv,
			"PYTHONPATH="+pyGuardDir+":"+os.Getenv("PYTHONPATH"),
			"PATH="+binGuardDir+":"+os.Getenv("PATH"),
			"HTTP_PROXY=http://127.0.0.1:1",
			"HTTPS_PROXY=http://127.0.0.1:1",
			"NO_PROXY=localhost,127.0.0.1,::1",
		)
	}
	if pgPort > 0 {
		procEnv = append(procEnv,
			"PGHOST="+filepath.Join(sbxRoot, "pgdata/run"),
			"PGPORT="+strconv.Itoa(pgPort),
			"PGUSER=postgres",
			"PGDATABASE=postgres",
		)
	}
	for k, v := range env {
		if validEnvKey(k) {
			procEnv = append(procEnv, fmt.Sprintf("%s=%s", k, v))
		}
	}

	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	c := exec.CommandContext(execCtx, shellBin, "sh", "-c", rewritten)
	c.Dir = hostWorkdir
	c.Env = procEnv
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	c.Cancel = func() error {
		if c.Process != nil {
			_ = syscall.Kill(-c.Process.Pid, syscall.SIGKILL)
			return c.Process.Kill()
		}
		return nil
	}
	c.WaitDelay = 200 * time.Millisecond

	stdout, stderr, runErr := streamCommandPipes(c, onChunk)
	exitCode := 0
	if errors.Is(execCtx.Err(), context.DeadlineExceeded) {
		exitCode = 124
		if stderr == "" {
			stderr = fmt.Sprintf("command timed out after %s\n", timeout)
			if onChunk != nil {
				onChunk("stderr", stderr)
			}
		}
		runErr = context.DeadlineExceeded
	} else if runErr != nil {
		if exitErr, ok := runErr.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			exitCode = 1
		}
	}
	l.AppendLog(sandboxID, fmt.Sprintf("$ [stream allow-egress=%v] %s (exit=%d)", allowEgress, cmdDisplay, exitCode))
	if len(stdout) > 0 {
		l.AppendLog(sandboxID, stdout)
	}
	return stdout, stderr, exitCode, runErr
}

// isLocalPortOwnedBySandbox inspects /proc/net/tcp{,6} and /proc/<pid>/{environ,fd} to verify
// whether targetPort has an active listener and whether that listener belongs to sandboxID.
func isLocalPortOwnedBySandbox(sandboxID string, targetPort int, bgPIDs []int) (listening bool, owned bool) {
	hexPort := fmt.Sprintf(":%04X", targetPort)
	listenSockets := make(map[string]bool)
	procReadable := false
	for _, netFile := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		data, err := os.ReadFile(netFile)
		if err != nil {
			continue
		}
		procReadable = true
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 10 || fields[3] != "0A" {
				continue
			}
			if strings.HasSuffix(strings.ToUpper(fields[1]), hexPort) && fields[9] != "0" && fields[9] != "" {
				listenSockets["socket:["+fields[9]+"]"] = true
			}
		}
	}
	if !procReadable {
		return true, true
	}
	if len(listenSockets) == 0 {
		return false, false
	}

	selfPID := os.Getpid()
	pidOwnsListeningSocket := func(pid int) bool {
		if pid <= 0 || pid == selfPID {
			return false
		}
		fdDir := fmt.Sprintf("/proc/%d/fd", pid)
		entries, err := os.ReadDir(fdDir)
		if err != nil {
			return false
		}
		for _, ent := range entries {
			if link, lErr := os.Readlink(filepath.Join(fdDir, ent.Name())); lErr == nil && listenSockets[link] {
				return true
			}
		}
		return false
	}

	for _, pid := range bgPIDs {
		if pidOwnsListeningSocket(pid) {
			return true, true
		}
	}

	needle := []byte("SANDBOX_ID=" + sandboxID + "\x00")
	procEntries, err := os.ReadDir("/proc")
	if err != nil {
		return true, false
	}
	for _, ent := range procEntries {
		if !ent.IsDir() {
			continue
		}
		pid, pErr := strconv.Atoi(ent.Name())
		if pErr != nil || pid <= 0 || pid == selfPID {
			continue
		}
		envBytes, eErr := os.ReadFile(fmt.Sprintf("/proc/%d/environ", pid))
		if eErr != nil || !bytes.Contains(envBytes, needle) {
			continue
		}
		if pidOwnsListeningSocket(pid) {
			return true, true
		}
	}
	return true, false
}

// DialSandboxPort connects to `127.0.0.1:<port>` for the local sandbox, validating port bounds,
// blocking privileged ports (< 1024) and non-sandbox localhost listeners (SSRF protection), and
// retrying for up to 5s so background HTTP/TCP servers restarted after auto-resume have time to bind.
func (l *LocalProcessEngine) DialSandboxPort(ctx context.Context, rec SandboxRecord, port int) (net.Conn, error) {
	if port < 1 || port > 65535 {
		return nil, fmt.Errorf("invalid port %d (must be between 1 and 65535)", port)
	}
	targetPort := port
	isDedicatedPgPort := false
	if port == 5432 {
		if rec.PostgresPort > 0 {
			targetPort = rec.PostgresPort
			isDedicatedPgPort = true
		} else {
			l.mu.RLock()
			p := l.pgPorts[rec.SandboxID]
			l.mu.RUnlock()
			if p > 0 {
				targetPort = p
				isDedicatedPgPort = true
			}
		}
	}
	if targetPort < 1024 {
		return nil, fmt.Errorf("refusing to proxy privileged port %d (< 1024)", targetPort)
	}
	addr := fmt.Sprintf("127.0.0.1:%d", targetPort)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if !isDedicatedPgPort {
			l.mu.RLock()
			bgPIDs := append([]int(nil), l.bgPIDs[rec.SandboxID]...)
			l.mu.RUnlock()
			listening, owned := isLocalPortOwnedBySandbox(rec.SandboxID, targetPort, bgPIDs)
			if listening && !owned {
				return nil, fmt.Errorf("SSRF protection: refusing to proxy port %d which is not owned by sandbox %s", targetPort, rec.SandboxID)
			}
		}
		d := net.Dialer{Timeout: 200 * time.Millisecond}
		conn, err := d.DialContext(ctx, "tcp", addr)
		if err == nil {
			if !isDedicatedPgPort {
				l.mu.RLock()
				bgPIDs := append([]int(nil), l.bgPIDs[rec.SandboxID]...)
				l.mu.RUnlock()
				if listening, owned := isLocalPortOwnedBySandbox(rec.SandboxID, targetPort, bgPIDs); listening && !owned {
					_ = conn.Close()
					return nil, fmt.Errorf("SSRF protection: refusing to proxy port %d which is not owned by sandbox %s", targetPort, rec.SandboxID)
				}
			}
			return conn, nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return nil, fmt.Errorf("dial sandbox %s port %d (%s): %w", rec.SandboxID, port, addr, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// ExportTar flushes any active Postgres WAL (`CHECKPOINT`) and archives the sandbox's root directory to destTarPath.
func (l *LocalProcessEngine) ExportTar(_ context.Context, rec SandboxRecord, destTarPath string) error {
	sbxRoot, err := l.sandboxRoot(rec.SandboxID)
	if err != nil {
		return err
	}
	l.mu.RLock()
	pgPort := l.pgPorts[rec.SandboxID]
	l.mu.RUnlock()
	if pgPort > 0 && os.Getuid() == 0 {
		pgRunDir := filepath.Join(sbxRoot, "pgdata/run")
		chkCmd := fmt.Sprintf("su -s /bin/sh postgres -c %s",
			shellQuote(fmt.Sprintf("psql -h %s -p %d -U postgres -c 'CHECKPOINT;' >/dev/null 2>&1 || true", shellQuote(pgRunDir), pgPort)))
		_ = exec.Command("/usr/bin/env", "sh", "-c", chkCmd).Run()
	}
	exportStart := time.Now()
	if err := createTarFromDir(sbxRoot, destTarPath); err != nil {
		return err
	}
	// Remember the archived entry manifest so a later ExportDeltaTar (Two-Phase Pre-Copy, Phase 2)
	// can compute modified/deleted entries relative to this export.
	if m, err := manifestFromTarFile(destTarPath); err == nil {
		l.manifests.record(rec.SandboxID, exportStart, m)
	}
	return nil
}

// ExportDeltaTar archives the entries created or modified since `since` (relative to the base
// manifests recorded by ExportTar at or after `since`) and returns the deleted entries.
func (l *LocalProcessEngine) ExportDeltaTar(_ context.Context, rec SandboxRecord, since time.Time, destTarPath string) ([]string, error) {
	sbxRoot, err := l.sandboxRoot(rec.SandboxID)
	if err != nil {
		return nil, err
	}
	bases := l.manifests.since(rec.SandboxID, since)
	if len(bases) == 0 {
		return nil, fmt.Errorf("no base export manifest for sandbox %s at or after %s", rec.SandboxID, since.UTC().Format(time.RFC3339Nano))
	}
	if _, err := os.Stat(sbxRoot); err != nil {
		return nil, fmt.Errorf("sandbox root %s missing before delta export: %w", sbxRoot, err)
	}
	var selected []string
	current := make(map[string]bool)
	_ = filepath.Walk(sbxRoot, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil || path == sbxRoot {
			return nil
		}
		rel, relErr := filepath.Rel(sbxRoot, path)
		if relErr != nil {
			return nil
		}
		relSlash := filepath.ToSlash(rel)
		if skipTarEntry(relSlash, info.Mode()) {
			return nil
		}
		current[relSlash] = true
		if entryNeedsDelta(relSlash, sigFromFileInfo(info), bases, since) {
			selected = append(selected, path)
		}
		return nil
	})
	sort.Strings(selected)
	if err := writeTarFromFileList(sbxRoot, selected, destTarPath); err != nil {
		return nil, err
	}
	return deletedSinceBases(current, bases), nil
}

// ApplyDeltaTar removes deletedPaths, extracts deltaTarPath over the prestaged sandbox root, and
// restarts PostgreSQL if it was quiesced during pre-staging.
func (l *LocalProcessEngine) ApplyDeltaTar(_ context.Context, rec SandboxRecord, deltaTarPath string, deletedPaths []string) error {
	sbxRoot, err := l.sandboxRoot(rec.SandboxID)
	if err != nil {
		return err
	}
	rels, err := validateDeletedPaths(deletedPaths)
	if err != nil {
		return err
	}
	for _, rel := range rels {
		target, err := SafeResolvePath(sbxRoot, rel)
		if err != nil {
			return err
		}
		if target == filepath.Clean(sbxRoot) {
			return fmt.Errorf("%w: refusing to delete sandbox root", ErrPathTraversal)
		}
		if err := os.RemoveAll(target); err != nil {
			return err
		}
	}
	if deltaTarPath != "" {
		if err := extractTarToDir(deltaTarPath, sbxRoot); err != nil {
			return fmt.Errorf("extract delta tar: %w", err)
		}
	}
	_ = os.WriteFile(filepath.Join(sbxRoot, "var/run/instance/state"), []byte("ACTIVE\n"), 0644)
	_ = os.WriteFile(filepath.Join(sbxRoot, "workspace/.instance_state"), []byte("ACTIVE\n"), 0644)
	if err := l.startLocalPostgresIfNeeded(rec, sbxRoot, true); err != nil {
		return err
	}
	l.AppendLog(rec.SandboxID, fmt.Sprintf("[system] Applied pre-copy delta (%d deleted paths)", len(rels)))
	return nil
}

// Quiesce cleanly stops PostgreSQL inside the local sandbox directory prior to suspend or migration.
func (l *LocalProcessEngine) Quiesce(_ context.Context, sandboxID string) error {
	sbxRoot, err := l.sandboxRoot(sandboxID)
	if err != nil {
		return err
	}
	l.mu.RLock()
	pgPort := l.pgPorts[sandboxID]
	l.mu.RUnlock()
	if pgPort > 0 && os.Getuid() == 0 {
		if pgBinDir := findLocalPostgresBinDir(); pgBinDir != "" {
			pgDataDir := filepath.Join(sbxRoot, "pgdata/data")
			stopCmd := fmt.Sprintf("su -s /bin/sh postgres -c %s",
				shellQuote(fmt.Sprintf("%s/pg_ctl -D %s -m fast -w stop >/dev/null 2>&1 || true", pgBinDir, shellQuote(pgDataDir))))
			_ = exec.Command("/usr/bin/env", "sh", "-c", stopCmd).Run()
		}
	}
	return nil
}

// Fork clones sourceSandboxID to targetRec.
func (l *LocalProcessEngine) Fork(ctx context.Context, sourceSandboxID string, targetRec SandboxRecord) (string, error) {
	tmpTar := filepath.Join(os.TempDir(), "local-fork-"+NewRandomID("")+".tar")
	defer os.Remove(tmpTar)
	if err := l.ExportTar(ctx, SandboxRecord{SandboxID: sourceSandboxID}, tmpTar); err != nil {
		return "", err
	}
	if err := l.Start(ctx, targetRec, tmpTar); err != nil {
		return "", err
	}
	return "local_overlay_fork", nil
}

// CopyFile uploads or downloads a file within the sandbox root with strict path traversal protection.
func (l *LocalProcessEngine) CopyFile(_ context.Context, sandboxID string, req FileCopyRequest) (FileCopyResponse, error) {
	sbxRoot, err := l.sandboxRoot(sandboxID)
	if err != nil {
		return FileCopyResponse{}, err
	}
	if strings.Contains(req.Path, "..") {
		return FileCopyResponse{}, fmt.Errorf("%w: %q contains '..'", ErrPathTraversal, req.Path)
	}
	resolved, err := SafeResolvePath(sbxRoot, req.Path)
	if err != nil {
		return FileCopyResponse{}, err
	}
	if strings.EqualFold(req.Direction, "download") {
		b, err := os.ReadFile(resolved)
		if err != nil {
			return FileCopyResponse{}, err
		}
		return FileCopyResponse{
			SandboxID: sandboxID,
			Path:      req.Path,
			Content:   b,
			SizeBytes: int64(len(b)),
		}, nil
	}
	if err := os.MkdirAll(filepath.Dir(resolved), 0755); err != nil {
		return FileCopyResponse{}, err
	}
	mode := os.FileMode(0644)
	if req.Mode != 0 {
		mode = os.FileMode(req.Mode)
	}
	if err := os.WriteFile(resolved, req.Content, mode); err != nil {
		return FileCopyResponse{}, err
	}
	l.AppendLog(sandboxID, fmt.Sprintf("[cp] Uploaded %d bytes to %s", len(req.Content), req.Path))
	return FileCopyResponse{
		SandboxID: sandboxID,
		Path:      req.Path,
		SizeBytes: int64(len(req.Content)),
	}, nil
}

// ReadLogs returns all logged lines and background daemon logs for sandboxID.
func (l *LocalProcessEngine) ReadLogs(_ context.Context, sandboxID string) (string, error) {
	sbxRoot, err := l.sandboxRoot(sandboxID)
	if err != nil {
		return "", err
	}
	l.mu.RLock()
	lines := append([]string(nil), l.logs[sandboxID]...)
	l.mu.RUnlock()
	if b, rErr := os.ReadFile(filepath.Join(sbxRoot, "var/log/sandbox.log")); rErr == nil && len(b) > 0 {
		lines = append(lines, strings.TrimRight(string(b), "\n"))
	}
	return strings.Join(lines, "\n"), nil
}

// AppendLog appends a timestamped log line in memory.
func (l *LocalProcessEngine) AppendLog(sandboxID, line string) {
	ts := fmt.Sprintf("[%s] %s", time.Now().UTC().Format("15:04:05.000"), strings.TrimRight(line, "\n"))
	l.mu.Lock()
	l.logs[sandboxID] = append(l.logs[sandboxID], ts)
	if len(l.logs[sandboxID]) > 200 {
		l.logs[sandboxID] = l.logs[sandboxID][len(l.logs[sandboxID])-200:]
	}
	l.mu.Unlock()
}

// Stop terminates background processes and Postgres (if running), marks the sandbox stopped, and removes its runtime directory.
func (l *LocalProcessEngine) Stop(_ context.Context, sandboxID string) error {
	sbxRoot, err := l.sandboxRoot(sandboxID)
	if err != nil {
		return err
	}
	l.mu.Lock()
	delete(l.alive, sandboxID)
	pids := l.bgPIDs[sandboxID]
	delete(l.bgPIDs, sandboxID)
	pgPort := l.pgPorts[sandboxID]
	delete(l.pgPorts, sandboxID)
	l.mu.Unlock()
	l.manifests.forget(sandboxID)

	for _, pid := range pids {
		if pid > 0 {
			_ = syscall.Kill(-pid, syscall.SIGKILL)
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	}
	if pgPort > 0 && os.Getuid() == 0 {
		if pgBinDir := findLocalPostgresBinDir(); pgBinDir != "" {
			pgDataDir := filepath.Join(sbxRoot, "pgdata/data")
			stopCmd := fmt.Sprintf("su -s /bin/sh postgres -c %s",
				shellQuote(fmt.Sprintf("%s/pg_ctl -D %s -m fast -w stop >/dev/null 2>&1 || true", pgBinDir, shellQuote(pgDataDir))))
			_ = exec.Command("/usr/bin/env", "sh", "-c", stopCmd).Run()
		}
	}
	_ = os.RemoveAll(sbxRoot)
	return nil
}

// IsAlive reports whether sandboxID is currently running.
func (l *LocalProcessEngine) IsAlive(_ context.Context, sandboxID string) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.alive[sandboxID]
}

// CheckInGuestState checks `var/run/instance/state` or `workspace/.instance_state` inside the sandbox.
func (l *LocalProcessEngine) CheckInGuestState(sandboxID string) string {
	sbxRoot, err := l.sandboxRoot(sandboxID)
	if err != nil {
		return ""
	}
	for _, rel := range []string{"var/run/instance/state", "workspace/.instance_state"} {
		if b, err := os.ReadFile(filepath.Join(sbxRoot, rel)); err == nil {
			val := strings.ToUpper(strings.TrimSpace(string(b)))
			if val != "" && val != "ACTIVE" {
				return val
			}
		}
	}
	return "ACTIVE"
}

func createTarFromDir(srcDir, destTarPath string) error {
	if _, err := os.Stat(srcDir); err != nil {
		return fmt.Errorf("source directory %s missing before tar export: %w", srcDir, err)
	}
	// Integrity guard: if pgdata/data/postgresql.conf exists without pgdata/data/PG_VERSION, refuse to write a corrupt tar!
	if _, confErr := os.Stat(filepath.Join(srcDir, "pgdata/data/postgresql.conf")); confErr == nil {
		if _, verErr := os.Stat(filepath.Join(srcDir, "pgdata/data/PG_VERSION")); os.IsNotExist(verErr) {
			return fmt.Errorf("corrupt pgdata directory in %s: postgresql.conf exists without PG_VERSION", srcDir)
		}
	}
	var files []string
	err := filepath.Walk(srcDir, func(path string, _ os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		if path == srcDir {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		return err
	}
	if _, err := os.Stat(srcDir); err != nil {
		return fmt.Errorf("source directory %s removed during tar export: %w", srcDir, err)
	}
	sort.Strings(files)
	return writeTarFromFileList(srcDir, files, destTarPath)
}

// skipTarEntry reports whether a sandbox-root-relative path must never be archived: transient state
// trigger files, Postgres lock files and sockets, and Unix sockets.
func skipTarEntry(relSlash string, mode os.FileMode) bool {
	return relSlash == "var/run/instance/state" ||
		relSlash == "workspace/.instance_state" ||
		strings.HasSuffix(relSlash, "/postmaster.pid") ||
		strings.HasSuffix(relSlash, "/postmaster.opts") ||
		strings.HasPrefix(relSlash, "pgdata/run/") ||
		mode&os.ModeSocket != 0
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

// writeTarFromFileList archives the given absolute paths under srcDir (names relative to srcDir)
// into destTarPath atomically (temp file + rename).
func writeTarFromFileList(srcDir string, files []string, destTarPath string) error {
	if err := os.MkdirAll(filepath.Dir(destTarPath), 0755); err != nil {
		return err
	}
	tmpTarPath := destTarPath + ".tmp." + NewRandomID("")
	f, err := os.Create(tmpTarPath)
	if err != nil {
		return err
	}
	tw := tar.NewWriter(f)
	fail := func(err error) error {
		_ = tw.Close()
		_ = f.Close()
		_ = os.Remove(tmpTarPath)
		return err
	}
	for _, path := range files {
		info, err := os.Lstat(path)
		if err != nil {
			continue
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			continue
		}
		relSlash := filepath.ToSlash(rel)
		if skipTarEntry(relSlash, info.Mode()) {
			continue
		}
		var linkTarget string
		if info.Mode()&os.ModeSymlink != 0 {
			linkTarget, err = os.Readlink(path)
			if err != nil {
				continue
			}
		}
		hdr, err := tar.FileInfoHeader(info, linkTarget)
		if err != nil {
			continue
		}
		hdr.Name = relSlash
		if info.IsDir() && !strings.HasSuffix(hdr.Name, "/") {
			hdr.Name += "/"
		}
		if !info.Mode().IsRegular() {
			if err := tw.WriteHeader(hdr); err != nil {
				return fail(err)
			}
			continue
		}
		rf, err := os.Open(path)
		if err != nil {
			continue
		}
		if err := tw.WriteHeader(hdr); err != nil {
			_ = rf.Close()
			return fail(err)
		}
		// The sandbox keeps running during pre-copy exports, so a file may shrink or grow while it
		// is read: write exactly hdr.Size bytes (zero-padding a short read) so the archive stays
		// valid. Such files have a fresh mtime and are re-sent by the Phase 2 delta.
		n, _ := io.Copy(tw, io.LimitReader(rf, hdr.Size))
		_ = rf.Close()
		if n < hdr.Size {
			if _, err := io.CopyN(tw, zeroReader{}, hdr.Size-n); err != nil {
				return fail(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpTarPath)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpTarPath)
		return err
	}
	if err := os.Rename(tmpTarPath, destTarPath); err != nil {
		_ = os.Remove(tmpTarPath)
		return err
	}
	return nil
}

func extractTarToDir(tarPath, destDir string) error {
	f, err := os.Open(tarPath)
	if err != nil {
		return err
	}
	defer f.Close()
	tr := tar.NewReader(f)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if strings.Contains(hdr.Name, "..") {
			return fmt.Errorf("%w: tar entry %q", ErrPathTraversal, hdr.Name)
		}
		relSlash := strings.TrimPrefix(filepath.ToSlash(hdr.Name), "/")
		if relSlash == "var/run/instance/state" || relSlash == "workspace/.instance_state" {
			continue
		}
		target, err := SafeResolvePath(destDir, hdr.Name)
		if err != nil {
			return err
		}
		mode := os.FileMode(hdr.Mode) & 0777
		existing, lstatErr := os.Lstat(target)
		switch hdr.Typeflag {
		case tar.TypeDir:
			if mode == 0 {
				mode = 0755
			}
			// Delta extraction over a live tree: replace a file/symlink that became a directory.
			if lstatErr == nil && !existing.IsDir() {
				_ = os.Remove(target)
			}
			if err := os.MkdirAll(target, mode); err != nil {
				return err
			}
			_ = os.Chmod(target, mode)
		case tar.TypeSymlink:
			if filepath.IsAbs(hdr.Linkname) {
				if _, lErr := SafeResolvePath(destDir, hdr.Linkname); lErr != nil {
					continue
				}
			} else {
				if _, lErr := SafeResolvePath(destDir, filepath.Join(filepath.Dir(hdr.Name), hdr.Linkname)); lErr != nil {
					continue
				}
			}
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return err
			}
			if lstatErr == nil && existing.IsDir() {
				_ = os.RemoveAll(target)
			} else {
				_ = os.Remove(target)
			}
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return err
			}
			// Never write through an existing symlink, and replace a directory that became a file.
			if lstatErr == nil && !existing.Mode().IsRegular() {
				if err := os.RemoveAll(target); err != nil {
					return err
				}
			}
			if mode == 0 {
				mode = 0644
			}
			out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
			if err != nil {
				return err
			}
			// Enforce max 256MB per file to prevent decompression DoS
			_, copyErr := io.Copy(out, io.LimitReader(tr, 256*1024*1024))
			_ = out.Close()
			if copyErr != nil {
				return copyErr
			}
			_ = os.Chmod(target, mode)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------------------------
// Two-Phase Pre-Copy delta support (shared by LocalProcessEngine and CloudRunSandboxEngine).
// ---------------------------------------------------------------------------------------------

// deltaMtimeSlack absorbs coarse filesystem timestamp granularity: anything with an mtime within
// this window before `since` is re-sent in the delta. Over-inclusion is always safe.
const deltaMtimeSlack = 2 * time.Second

// maxExportManifestsPerSandbox bounds how many recent ExportTar manifests are kept per sandbox
// (older ones are merged, never silently dropped, while younger than manifestRetention).
const maxExportManifestsPerSandbox = 4

// manifestRetention is how long export manifests stay relevant; it comfortably exceeds the
// longest possible pre-copy window (prestageTimeout + freeze).
const manifestRetention = 15 * time.Minute

// fileSig is the quick-check signature (rsync-style) of one archived filesystem entry.
type fileSig struct {
	Type    byte // '0' regular, '5' directory, '2' symlink, 'x' other
	Size    int64
	Mode    int64 // permission bits (& 0777)
	ModTime time.Time
	// Conflict marks an entry merged from several manifests with different signatures (or present
	// in only some of them); it always counts as changed.
	Conflict bool
}

func (a fileSig) differs(b fileSig) bool {
	if a.Conflict || b.Conflict || a.Type != b.Type || a.Mode != b.Mode {
		return true
	}
	if a.Type == '0' && a.Size != b.Size {
		return true
	}
	d := a.ModTime.Sub(b.ModTime)
	if d < 0 {
		d = -d
	}
	// Tar headers round mtimes to whole seconds.
	return d > time.Second
}

func sigFromFileInfo(info os.FileInfo) fileSig {
	sig := fileSig{Type: 'x', Mode: int64(info.Mode().Perm()), ModTime: info.ModTime()}
	switch {
	case info.Mode().IsRegular():
		sig.Type = '0'
		sig.Size = info.Size()
	case info.IsDir():
		sig.Type = '5'
	case info.Mode()&os.ModeSymlink != 0:
		sig.Type = '2'
	}
	return sig
}

func sigFromTarHeader(hdr *tar.Header) fileSig {
	sig := fileSig{Type: 'x', Mode: hdr.Mode & 0777, ModTime: hdr.ModTime}
	switch hdr.Typeflag {
	case tar.TypeReg, tar.TypeRegA:
		sig.Type = '0'
		sig.Size = hdr.Size
	case tar.TypeDir:
		sig.Type = '5'
	case tar.TypeSymlink:
		sig.Type = '2'
	}
	return sig
}

// normalizeDeltaRel canonicalizes a tar entry name or deleted path ("./workspace/a/" -> "workspace/a").
func normalizeDeltaRel(name string) string {
	rel := strings.TrimPrefix(filepath.ToSlash(name), "./")
	rel = strings.Trim(rel, "/")
	if rel == "." {
		return ""
	}
	return rel
}

func manifestFromTarReader(r io.Reader) (map[string]fileSig, error) {
	out := make(map[string]fileSig)
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		if rel := normalizeDeltaRel(hdr.Name); rel != "" {
			out[rel] = sigFromTarHeader(hdr)
		}
	}
}

// manifestFromTarFile reads only the headers of a tar archive (seeking past file contents).
func manifestFromTarFile(path string) (map[string]fileSig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return manifestFromTarReader(f)
}

type exportManifest struct {
	at      time.Time
	entries map[string]fileSig
}

// manifestTracker remembers the entry manifests of recent full ExportTar calls per sandbox so a
// later ExportDeltaTar can compute modified and deleted entries relative to a pre-copied base.
type manifestTracker struct {
	mu sync.Mutex
	m  map[string][]exportManifest
}

func (t *manifestTracker) record(sandboxID string, at time.Time, entries map[string]fileSig) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.m == nil {
		t.m = make(map[string][]exportManifest)
	}
	list := append(t.m[sandboxID], exportManifest{at: at, entries: entries})
	// Frequent background exports (post_exec / periodic) must never evict a pre-copy base that a
	// pending ExportDeltaTar still needs. Instead of dropping the oldest manifest, merge it into the
	// next one (keeping the newer start time): the union only over-includes, which is always safe.
	for len(list) > maxExportManifestsPerSandbox {
		oldest, next := list[0], list[1]
		if time.Since(oldest.at) > manifestRetention {
			list = list[1:]
			continue
		}
		merged := make(map[string]fileSig, len(next.entries))
		for rel, sig := range next.entries {
			merged[rel] = sig
		}
		for rel, sig := range oldest.entries {
			if cur, ok := merged[rel]; ok {
				if cur.differs(sig) {
					cur.Conflict = true
					merged[rel] = cur
				}
			} else {
				sig.Conflict = true
				merged[rel] = sig
			}
		}
		list = append([]exportManifest{{at: next.at, entries: merged}}, list[2:]...)
	}
	t.m[sandboxID] = list
}

// since returns every recorded manifest whose export started at or after `since`. The pre-copy
// base is one of them; comparing against all of them is safe because over-inclusion only adds
// entries to the delta (and deleting an already-absent path is a no-op).
func (t *manifestTracker) since(sandboxID string, since time.Time) []exportManifest {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []exportManifest
	for _, m := range t.m[sandboxID] {
		if !m.at.Before(since) {
			out = append(out, m)
		}
	}
	return out
}

func (t *manifestTracker) forget(sandboxID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.m, sandboxID)
}

// entryNeedsDelta reports whether the current entry `rel` must be re-sent in the delta.
func entryNeedsDelta(rel string, cur fileSig, bases []exportManifest, since time.Time) bool {
	if !cur.ModTime.Before(since.Add(-deltaMtimeSlack)) {
		return true
	}
	for _, b := range bases {
		prev, ok := b.entries[rel]
		if !ok || prev.differs(cur) {
			return true
		}
	}
	return false
}

// deletedSinceBases returns the entries present in any base manifest but absent now, pruned to
// top-most deleted directories and sorted.
func deletedSinceBases(current map[string]bool, bases []exportManifest) []string {
	set := make(map[string]bool)
	for _, b := range bases {
		for rel, sig := range b.entries {
			if !current[rel] && !skipTarEntry(rel, 0) && sig.Type != 'x' {
				set[rel] = true
			}
		}
	}
	all := make([]string, 0, len(set))
	for rel := range set {
		all = append(all, rel)
	}
	sort.Strings(all)
	out := make([]string, 0, len(all))
	for _, rel := range all {
		covered := false
		for parent := filepath.Dir(rel); parent != "." && parent != "/" && parent != ""; parent = filepath.Dir(parent) {
			if set[filepath.ToSlash(parent)] {
				covered = true
				break
			}
		}
		if !covered {
			out = append(out, rel)
		}
	}
	return out
}

// validateDeletedPaths normalizes deletedPaths and rejects any traversal or root-level deletion.
func validateDeletedPaths(deletedPaths []string) ([]string, error) {
	out := make([]string, 0, len(deletedPaths))
	for _, p := range deletedPaths {
		rel := normalizeDeltaRel(p)
		if rel == "" || strings.Contains(rel, "..") || strings.ContainsRune(rel, 0) {
			return nil, fmt.Errorf("%w: invalid deleted path %q", ErrPathTraversal, p)
		}
		out = append(out, rel)
	}
	return out, nil
}

func writeEmptyTar(destTarPath string) error {
	if err := os.MkdirAll(filepath.Dir(destTarPath), 0755); err != nil {
		return err
	}
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.Close(); err != nil {
		return err
	}
	return os.WriteFile(destTarPath, buf.Bytes(), 0644)
}
