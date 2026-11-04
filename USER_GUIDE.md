# Cloud Run Sandbox Dispenser — User Guide & CLI Reference

**Navigation:** [Overview & Quick Start (README.md)](README.md) ·
**User Guide & CLI Reference (USER_GUIDE.md)** ·
[Architecture & Maintainer Guide (ARCHITECTURE.md)](ARCHITECTURE.md) ·
[Overload Protection (OVERLOAD_PROTECTION.md)](OVERLOAD_PROTECTION.md)

This guide covers installing, initializing, authenticating, and operating the **Cloud Run Sandbox
Dispenser** CLI (`sbx` `v1.2.0`), followed by a complete flag-by-flag command reference and
end-to-end cookbook workflows.

## Contents

- [Command Cheat Sheet](#command-cheat-sheet)
- [1. Installation, Setup & Core Concepts](#1-installation-setup--core-concepts)
  - [1.1 Building & Installing the `sbx` CLI](#11-building--installing-the-sbx-cli)
  - [1.2 Authentication & Local Cache Files (`~/.cache/sbx/`)](#12-authentication--local-cache-files-cachesbx)
  - [1.3 Core Concepts: Hosts, Persistence & Execution Engines](#13-core-concepts-hosts-persistence--execution-engines)
  - [1.4 Sandbox IDs (`sbx-<host>-<8hex>`) vs. Sandbox Names](#14-sandbox-ids-sbx-host-8hex-vs-sandbox-names)
  - [1.5 Global Flags & Environment Variable Overrides](#15-global-flags--environment-variable-overrides)
- [2. Command-by-Command CLI Reference](#2-command-by-command-cli-reference)
  - [Setup & Version: `2.1 init` · `2.2 deploy` · `2.3 version`](#21-init--initialize-local-cli-configuration)
  - [Sandbox Lifecycle: `2.4 create` · `2.5 list` · `2.6 inspect` · `2.7 update` · `2.8 suspend` · `2.9 resume` · `2.10 delete`](#24-create--create-a-new-sandbox)
  - [Execution, Networking & I/O: `2.11 exec` · `2.12 preview-url` · `2.13 port-forward` · `2.14 ssh` · `2.15 cp` · `2.16 logs`](#211-exec--execute-commands-inside-a-sandbox-buffered-or-sse-stream)
  - [Cloning, Snapshots & Templates: `2.17 copy` · `2.18 snapshot` · `2.19 template`](#217-copy-alias-fork--clone-a-running-sandbox)
  - [Live Migration, Fleet Ops & Web Console: `2.20 migrate` · `2.21 hosts` · `2.22 config` · `2.23 stats & metrics` · `2.24 ui`](#220-migrate--two-phase-pre-copy-live-migration-or-host-drain)
- [3. End-to-End Cookbook Examples](#3-end-to-end-cookbook-examples)
  - [Example 1: Ephemeral Python Data Science Sandbox & File Transfer](#example-1-ephemeral-python-data-science-sandbox--file-transfer)
  - [Example 2: Custom Environment Template & Fast Copy-on-Write Cloning](#example-2-custom-environment-template--fast-copy-on-write-cloning)
  - [Example 3: Persistent PostgreSQL 16 Sandbox Surviving Live Migration & Crash](#example-3-persistent-postgresql-16-sandbox-surviving-live-migration--crash)
  - [Example 4: Cost Optimization with Idle Auto-Suspend, In-Guest `IDLE` Signaling & Bin-Packing](#example-4-cost-optimization-with-idle-auto-suspend-in-guest-idle-signaling--bin-packing)
  - [Example 5: High-Throughput Parallel Batch Creation](#example-5-high-throughput-parallel-batch-creation)

---

## Command Cheat Sheet

| Category | Command (Aliases) | One-Line Purpose | Section |
| :--- | :--- | :--- | :--- |
| **Setup** | `sbx init [URL]` | Discover `sandbox-control-plane` & `sandbox-pool` URLs and cache OIDC auth token. | [§2.1](#21-init--initialize-local-cli-configuration) |
| **Setup** | `sbx deploy` | Build and deploy the 3 Cloud Run services + GCS bucket and run `sbx init`. | [§2.2](#22-deploy--deploy-or-reconcile-the-dispenser-fleet) |
| **Setup** | `sbx version` (`-v`) | Check client version, server version, and `min_client_version`. | [§2.3](#23-version--check-client--server-protocol-versions) |
| **Lifecycle** | `sbx create [NAME]` | Create a sandbox on an open candidate host (or spawn a new `host-<8hex>` clone). | [§2.4](#24-create--create-a-new-sandbox) |
| **Lifecycle** | `sbx list` (`ls`) | List sandboxes across the fleet (`--name`, `--prefix`, `--status`). | [§2.5](#25-list-alias-ls--list-sandboxes-across-the-fleet) |
| **Lifecycle** | `sbx inspect ID` (`describe`) | Fetch the full `SandboxRecord` JSON (including resource and migration telemetry). | [§2.6](#26-inspect-alias-describe--inspect-sandbox-state--metrics) |
| **Lifecycle** | `sbx update ID` | Resize CPU/RAM or update idle timeout, egress policy, env, and metadata in place. | [§2.7](#27-update--update-cpu-memory-idle-timeout-egress-env--metadata-in-place) |
| **Lifecycle** | `sbx suspend ID` / `sbx stop ID` | Export writable overlay to GCS, stop container (`SUSPENDED` / `STOPPED`), and free host slot. | [§2.8](#28-suspend--stop--checkpoint-overlay-to-gcs-and-release-host-slot) |
| **Lifecycle** | `sbx resume ID` / `sbx start ID` | Hydrate a `SUSPENDED` or `STOPPED` sandbox onto a warm host and restart background jobs. | [§2.9](#29-resume--start--hydrate-a-suspended-or-stopped-sandbox) |
| **Lifecycle** | `sbx delete ID...` (`rm`) | Permanently delete one or more sandboxes and their persisted GCS overlays. | [§2.10](#210-delete-alias-rm--delete-one-or-more-sandboxes) |
| **Exec & I/O** | `sbx exec ID -- CMD` | Run foreground, real-time SSE streaming (`-s`), or background (`-d`) commands. | [§2.11](#211-exec--execute-commands-inside-a-sandbox-buffered-or-sse-stream) |
| **Networking** | `sbx preview-url ID` (`preview`) | Print the IAM-gated HTTP/WebSocket preview URL (`/proxy/<id>/<port>/<path>`). | [§2.12](#212-preview-url-alias-preview--iam-authenticated-httpwebsocket-preview-urls) |
| **Networking** | `sbx port-forward ID` (`forward`) | Tunnel a local TCP port to a remote port inside a sandbox (with Wake-on-Connect). | [§2.13](#213-port-forward-alias-forward--local-tcp-port-tunneling) |
| **Exec & I/O** | `sbx ssh ID` | Open a line-oriented interactive shell REPL inside `/workspace`. | [§2.14](#214-ssh--open-an-interactive-line-repl-shell) |
| **Exec & I/O** | `sbx cp SRC DST` | Upload or download files between the local machine and `<ID>:/remote/path`. | [§2.15](#215-cp--copy-files-between-local-machine-and-sandbox) |
| **Exec & I/O** | `sbx logs ID` | View sandbox command output and lifecycle events (`--tail=N`). | [§2.16](#216-logs--fetch-stdout-stderr-and-lifecycle-logs) |
| **State** | `sbx copy SRC NAME` (`fork`) | Clone a running sandbox via same-host CoW fork or cross-host snapshot. | [§2.17](#217-copy-alias-fork--clone-a-running-sandbox) |
| **State** | `sbx snapshot create\|list\|delete` | Capture, list, or delete (`rm`) point-in-time filesystem snapshots. | [§2.18](#218-snapshot-create-list-delete--rm--point-in-time-filesystem-snapshots) |
| **State** | `sbx template list\|create\|delete` | Manage built-in and custom snapshot-backed templates (alias: `templates`). | [§2.19](#219-template-list-create--add-delete--rm--built-in--custom-templates) |
| **Fleet Ops** | `sbx migrate ID` | Live-migrate a sandbox via Two-Phase Pre-Copy (`--to-host`) or drain `--host`. | [§2.20](#220-migrate--two-phase-pre-copy-live-migration-or-host-drain) |
| **Fleet Ops** | `sbx hosts list\|drain\|compact` | Inspect host sessions, drain a host, compact sparse hosts, or `simulate-crash`. | [§2.21](#221-hosts-list-drain-compact-simulate-crash--manage-host-sessions) |
| **Fleet Ops** | `sbx config get\|set` | View or dynamically tune fleet density, host CPU/RAM, and overload thresholds. | [§2.22](#222-config-get-set--view-and-tune-fleet-wide-settings) |
| **Fleet Ops** | `sbx stats` / `sbx metrics` | View fleet SLA summary, CPU/RAM overcommit ratios, `--history`, and sizing advice. | [§2.23](#223-stats--metrics--fleet-sla-and-overcommit-observability) |
| **Console** | `sbx ui` | Launch the Web Console BFF locally on `http://127.0.0.1:8080`. | [§2.24](#224-ui--web-console-ui-sandbox-ui--sbx-ui) |

---

## 1. Installation, Setup & Core Concepts

### 1.1 Building & Installing the `sbx` CLI

Build `sbx` from source using Go 1.23+ (or `make install`):

```shell
git clone https://github.com/oded996/cloud-run-sandbox-dispenser.git
cd cloud-run-sandbox-dispenser
make install   # builds ./sbx and installs into $GOBIN or $(go env GOPATH)/bin
export PATH="${GOBIN:-$(go env GOPATH | cut -d: -f1)/bin}:${PATH}"
```

Or install directly via `go install`:

```shell
go install github.com/oded996/cloud-run-sandbox-dispenser@latest
export PATH="${GOBIN:-$(go env GOPATH | cut -d: -f1)/bin}:${PATH}"
cloud-run-sandbox-dispenser version   # creates the 'sbx' symlink in GOBIN on first run
```

Once installed, either connect to an existing deployment with `sbx init`
([§2.1](#21-init--initialize-local-cli-configuration)) or deploy a new fleet in your GCP project
with `sbx deploy` ([§2.2](#22-deploy--deploy-or-reconcile-the-dispenser-fleet)).

### 1.2 Authentication & Local Cache Files (`~/.cache/sbx/`)

All backend services (`sandbox-pool` and `sandbox-control-plane`) enforce Cloud Run IAM
authentication (`roles/run.invoker`). You do not need to manually fetch or export tokens:

* **Automatic Token Acquisition**: When communicating with a Cloud Run `https://*.run.app`
  endpoint, `sbx` invokes `gcloud auth print-identity-token` (and `gcloud auth print-access-token`
  for GCS operations, or queries the GCE/Cloud Run metadata server when running inside GCP).
* **45-Minute Disk Cache**: Tokens are cached in `~/.cache/sbx/token_cache.json` (mode `0600`,
  45-minute TTL) and refreshed automatically before expiry, eliminating per-command `gcloud`
  subprocess latency.
* **Direct `curl` Inspection**: To call HTTP endpoints or preview URLs directly with `curl`, pass
  `Authorization: Bearer $(gcloud auth print-identity-token)`.

| Cache File (`~/.cache/sbx/`) | Purpose |
| :--- | :--- |
| `endpoints.json` | Persists `control_plane_url` and `pool_service_url` discovered by `sbx init` or `sbx deploy`. |
| `token_cache.json` | Caches OIDC identity tokens and OAuth2 access tokens (mode `0600`, 45m TTL). |
| `candidates.json` | Cached `GET /v1/candidates` response (`candidates[]`, `pool_service_url`, `max_density`). Refreshed when local free slots reach `0` or when `refresh_interval_sec` expires (`15s` normally, `5s` when fleet free slots `<= 5`, `2s` when `<= 2`). |
| `routes.json` | Maps `<sandbox_id> -> <host_id>` and `<name> -> <sandbox_id>`. Updated on `create`/`copy`/`migrate` and self-healed on `HTTP 409 Conflict`. |

### 1.3 Core Concepts: Hosts, Persistence & Execution Engines

* **Host Session (`host-<8hex>`)**: A single Cloud Run session-affinity instance in `sandbox-pool`
  (default `4` vCPU / `8Gi` RAM, packing up to `max_density=10` sandboxes). When a host has `0`
  active sandboxes, it releases its `/__internal/hold-busy` keepalive and marks
  `/sys/class/instance/state` as `idle` so Cloud Run scales it down to zero.
* **Persistent (`--persistent`) vs. Ephemeral Sandboxes**:
  * **Persistent (`--persistent`)**: Syncs the writable overlay (`/workspace`, `/persistent`, and
    PostgreSQL `/pgdata/data`) to `persistence/<sandbox_id>/overlay.tar` in GCS in the background
    (dirty sandboxes every `3s`, all persistent sandboxes every `10s`) and before every `suspend` or
    `migrate`. Persistent sandboxes survive suspend/resume cycles, live migrations, revision
    rollouts, and host crashes. Sandboxes created with `--template=postgres` or with an idle
    timeout `> 0` automatically enable `persistent=true`.
  * **Ephemeral (default when `--persistent` is omitted)**: Fast scratch environments for
    short-lived tasks. Ephemeral sandboxes support in-place `exec`, `cp`, `migrate`, and explicit
    `suspend`/`resume`, but skip periodic background GCS syncs.
* **Execution Engine (`cloudrun` vs. `local`)**:
  * **`cloudrun`**: When the Cloud Run Sandbox Runtime (`/usr/local/gcp/bin/sandbox` and
    `/run/gcp/sandbox/sandbox.sock`) is available on `sandbox-pool`, each sandbox runs in an
    isolated micro-container with private namespaces, cgroup-enforced `--cpu` and `--memory`
    limits, and runtime-enforced `--no-allow-egress`.
  * **`local`**: When the Cloud Run Sandbox Runtime socket is absent (or `SBX_FORCE_LOCAL_ENGINE=1`
    is set), `hostd` falls back to `LocalProcessEngine` (`engine_type: "local"`), giving each
    sandbox a private `chmod 0700` directory tree inside the shared `hostd` container.

> [!WARNING]
> In `engine_type: "local"` mode, sandboxes share the container kernel namespaces, network stack,
> and service-account identity with `hostd`, and `--no-allow-egress` is enforced only on a
> best-effort basis (via dead-end proxy env vars, `curl`/`wget`/`nc` PATH shims, and a Python
> `sitecustomize.py` socket guard). Always use `engine_type: "cloudrun"` for untrusted workloads.

### 1.4 Sandbox IDs (`sbx-<host>-<8hex>`) vs. Sandbox Names

* **Sandbox Name (`name`)**: An optional human-readable label passed via `sbx create <NAME>`
  (matching `^[a-zA-Z0-9][a-zA-Z0-9._-]{0,62}$`). Multiple sandboxes across the fleet can share the
  same `name`.
* **Sandbox ID (`sandbox_id`)**: A globally unique identifier minted by the creating host in the
  form `sbx-<host_id>-<8hex>` (for example, `sbx-host-742e5bf0-a1b2c3d4`).
  1. **Lock-Free Parallel Creation**: Concurrent workers can all run `sbx create worker -q` without
     naming collisions.
  2. **Zero-Lookup Direct Routing**: Because `<host_id>` is embedded in `sandbox_id`, `sbx` routes
     directly to the owning host without querying `sandbox-control-plane` (and self-heals
     `routes.json` via `HTTP 409` if the sandbox live-migrates).
  3. **Ambiguous Name Protection**: If exactly **one** active sandbox matches `<NAME>`, you can
     pass `<NAME>` to any command (`sbx exec my-box ...`). If **two or more** active sandboxes
     share `<NAME>`, `GET /v1/sandboxes/<NAME>` rejects the lookup with `HTTP 409 Conflict` and
     JSON body
     `{"error": "ambiguous sandbox name \"worker\" matches 2 sandboxes (sbx-..., sbx-...); please specify the exact sandbox_id"}`
     (`hostd` also includes `"error_code": "ambiguous_sandbox_name"`) so you never mutate or delete
     the wrong sandbox.

### 1.5 Global Flags & Environment Variable Overrides

Global flags may be placed before any subcommand:

| Global Flag | Default | Description |
| :--- | :--- | :--- |
| `--control-plane-url=<URL>` | `~/.cache/sbx/endpoints.json` | Override the `sandbox-control-plane` URL for a single command. |
| `--pool-url=<URL>` | `~/.cache/sbx/endpoints.json` | Override the `sandbox-pool` URL for a single command. |
| `--cache-dir=<DIR>` (alias `--config-dir`) | `~/.cache/sbx` | Directory for endpoint, candidate, route, and OIDC token caches. |
| `--json` | `false` | Emit machine-readable JSON output instead of tables/text. |
| `--help`, `-h` | `false` | Print global or subcommand usage help. |

Optional environment variables for stateless CI workers or multi-tenant test harnesses:

| Environment Variable | Purpose |
| :--- | :--- |
| `SBX_CONTROL_PLANE_URL` | Overrides `sandbox-control-plane` URL when `--control-plane-url` is not passed. |
| `SBX_POOL_URL` | Overrides `sandbox-pool` URL when `--pool-url` is not passed. |
| `SBX_CACHE_DIR` / `SBX_CONFIG_DIR` | Overrides the local cache directory (`~/.cache/sbx`). |
| `SBX_ID_TOKEN` | Explicit OIDC bearer token override for `https://` Cloud Run requests. |

---

## 2. Command-by-Command CLI Reference

### 2.1 `init` — Initialize Local CLI Configuration

Connects to an existing `sandbox-control-plane` service (by explicit URL or by auto-discovering it
via `gcloud` from `--project` and `--region`), caches an OIDC token in `token_cache.json`, fetches
`/v1/candidates`, and writes `endpoints.json` and `candidates.json`.

#### Synopsis & Flags

```shell
sbx init [CONTROL_PLANE_URL] [--url=<URL>] [--project=<PROJECT_ID>] [--region=<REGION>] [--json]
```

| Flag | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `--url` | `string` | positional `[CONTROL_PLANE_URL]` | HTTPS URL of the `sandbox-control-plane` Cloud Run service. |
| `--project` | `string` | `""` (active `gcloud` project) | GCP project ID used to discover `sandbox-control-plane` when no URL is given. |
| `--region` | `string` | `us-west1` | Cloud Run region used when discovering `sandbox-control-plane` by project. |

* When `--project` is omitted, `sbx init` checks `$GOOGLE_CLOUD_PROJECT`, `$GCLOUD_PROJECT`,
  `$DEVSHELL_PROJECT_ID`, `$CLOUDSDK_CORE_PROJECT`, and `gcloud config get-value project`.

#### Example

```shell
sbx init https://sandbox-control-plane-xxxxx-uw.a.run.app
# Or discover by project and region:
sbx init --project=my-gcp-project --region=us-west1
```

---

### 2.2 `deploy` — Deploy or Reconcile the Dispenser Fleet

Builds the container image via Cloud Build (when `deploy.sh` is present in the working directory),
deploys `sandbox-control-plane`, `sandbox-pool`, and `sandbox-ui` to Cloud Run, seeds Cloud Run IAM
and Cloud IAP bindings for the deploying user, and runs `sbx init` automatically. Once
`sandbox-control-plane` is reachable, `sbx deploy` can also reconcile fleet sizing from any
directory via `POST /v1/deploy`.

#### Synopsis & Flags

```shell
sbx deploy [--project=<PROJECT_ID>] [--region=<REGION>] [--bucket=<GCS_BUCKET>] \
    [--host-cpu=<VCPU>] [--host-memory=<MEMORY>] [--max-density=<N>]
```

| Flag | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `--project` | `string` | active `gcloud` project | GCP project ID (reads `$GOOGLE_CLOUD_PROJECT` / `gcloud config`, fallback `my-gcp-project`). |
| `--region` | `string` | `us-west1` | Cloud Run deployment region. |
| `--bucket` | `string` | `<PROJECT_ID>-sandbox-dispenser` | GCS bucket for state and overlays (created automatically if missing). |
| `--host-cpu` | `string` | `4` | vCPU allocation per `sandbox-pool` host clone (`2`, `4`, `8`). |
| `--host-memory` | `string` | `8Gi` | Memory allocation per `sandbox-pool` host clone (`4Gi`, `8Gi`, `16Gi`, `32Gi`). |
| `--max-density` | `int` | `10` | Maximum concurrent sandboxes packed onto a single host clone. |

#### Example

```shell
gcloud services enable run.googleapis.com cloudbuild.googleapis.com \
    artifactregistry.googleapis.com storage.googleapis.com iap.googleapis.com \
    --project=my-gcp-project

sbx deploy --project=my-gcp-project --region=us-west1 --host-cpu=4 --host-memory=8Gi
```

---

### 2.3 `version` — Check Client & Server Protocol Versions

*(Aliases: `--version`, `-v`)*

Prints the local `sbx` client version (`1.2.0`) and, when initialized, queries `GET /v1/config` to
report `server_version` and `min_client_version`.

```shell
sbx version
# Output: sbx client v1.2.0 (server v1.2.0, min_client_version=1.0.0)
```

---

### 2.4 `create` — Create a New Sandbox

Creates a sandbox directly on an open candidate `HostSession` (`POST /__session/create`), falling
back across candidates on `HTTP 429`/`503` and autonomously spawning a new `host-<8hex>` clone when
all cached candidates are full.

#### Synopsis & Flags

```shell
sbx create [NAME] [--name=<NAME>] [--template=ubuntu|python|node|postgres|<CUSTOM>] \
    [--persistent] [--cpu=<CPU>] [--memory=<MEMORY>] [--allow-egress=true|false | --no-allow-egress] \
    [--from-snapshot=<SNAPSHOT_ID>] [--host=<HOST_ID>] [--pinned] \
    [--idle-timeout=<DURATION> | --auto-suspend=<DURATION> | --no-auto-suspend] \
    [--env=<K=V>] [--metadata=<K=V>] [--force] [--quiet | -q | --id-only] [--json]
```

| Flag | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `--name` | `string` | positional `[NAME]` | Human-readable label; host always mints a unique `sbx-<host_id>-<8hex>` ID. |
| `--template` | `string` | `ubuntu` | Built-in (`ubuntu`, `python`, `node`, `postgres`) or custom template name. |
| `--persistent` | `bool` | `false` | Sync writable overlay to GCS (auto-enabled for `postgres` or when idle timeout `> 0`). |
| `--cpu` | `string` | `1` | CPU allocation (e.g., `1`, `2`, `0.5`, `500m`). |
| `--memory` | `string` | `1Gi` | Memory allocation (e.g., `512Mi`, `1Gi`, `2Gi`). |
| `--allow-egress` | `bool` | `true` | Allow outbound internet egress from the sandbox (`false` blocks internet egress). |
| `--no-allow-egress` | `bool` | `false` | Shorthand for `--allow-egress=false` (enforced in `cloudrun` mode; best-effort in `local`). |
| `--from-snapshot` | `string` | `""` | Hydrate initial filesystem state from an existing snapshot ID. |
| `--host` | `string` | `""` | Pin creation to a specific `HostSession` ID (sets `pinned_host=true`). |
| `--pinned` | `bool` | `false` | Pin sandbox to its host (`pinned_host=true`) so bin-pack compaction skips it. |
| `--idle-timeout` | `string` | `0` (disabled) | Auto-suspend after `<N>` seconds or duration (`300`, `5m`, `0s`) of inactivity. |
| `--auto-suspend` | `string` | `0` (disabled) | Alias for `--idle-timeout`. Automatically enables `persistent=true` when `> 0`. |
| `--no-auto-suspend` | `bool` | `false` | Shorthand for `--idle-timeout=0` (overrides a non-zero fleet `default_idle_timeout_sec`). |
| `--env` | `key=val` | none | Environment variables injected into the sandbox (repeatable or comma-separated). |
| `--metadata` | `key=val` | none | Arbitrary key-value labels attached to `SandboxRecord` (repeatable or comma-separated). |
| `--force` | `bool` | `false` | Overwrite existing ID and **bypass Tier 1 memory-pressure admission control**. |
| `--quiet`, `-q`, `--id-only` | `bool` | `false` | Print only the generated `sandbox_id` to stdout for shell capture. |

> [!NOTE]
> Passing `--force` bypasses host memory-pressure admission rejection (`!req.Force` in `hostd.go`);
> see [OVERLOAD_PROTECTION.md §3](OVERLOAD_PROTECTION.md#tier-1-memory-aware-admission-control-admission_blocked--http-429).

#### Examples

```shell
# Capture the unique sandbox_id in a variable (-q)
MY_SANDBOX=$(sbx create data-worker --template=python --persistent -q)

# Standard human-readable output (prints idle_timeout=off when disabled)
sbx create data-worker --template=python --persistent --cpu=2 --memory=2Gi
# Output:
# Created sandbox sbx-host-742e5bf0-a1b2c3d4 (name=data-worker) on host host-742e5bf0 (template=python, status=RUNNING, epoch=1, persistent=true, idle_timeout=off)
```

---

### 2.5 `list` (Alias: `ls`) — List Sandboxes Across the Fleet

Queries the control-plane in-memory fleet cache (`GET /v1/sandboxes`).

```shell
sbx list [--name=<NAME>] [--prefix=<PREFIX>] [--status=RUNNING|SUSPENDED|STOPPED|MIGRATING] [--json]
```

| Flag | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `--name` | `string` | `""` | Filter sandboxes by exact `name`. |
| `--prefix` | `string` | `""` | Filter sandboxes whose `sandbox_id` or `name` starts with `<PREFIX>`. |
| `--status` | `string` | `""` | Filter by lifecycle status (`RUNNING`, `SUSPENDED`, `STOPPED`, `MIGRATING`). |

```shell
sbx list --prefix=data-
```

```text
SANDBOX_ID                   NAME         STATUS    TEMPLATE  HOST_ID         EPOCH  PERSISTENT  IDLE_TIMEOUT  MIGRATIONS  RECOVERIES
sbx-host-742e5bf0-a1b2c3d4   data-worker  RUNNING   python    host-742e5bf0   1      true        off           0           0
sbx-host-91c4e012-f0e1d2c3   data-worker  RUNNING   python    host-91c4e012   1      true        300s          0           0
```

---

### 2.6 `inspect` (Alias: `describe`) — Inspect Sandbox State & Metrics

Fetches the full `SandboxRecord` JSON directly from the owning host (`GET /__session/inspect`),
falling back to `GET /v1/sandboxes/<id>` on the control plane when the sandbox is suspended.

```shell
sbx inspect "$MY_SANDBOX"
```

```json
{
  "sandbox_id": "sbx-host-742e5bf0-a1b2c3d4",
  "name": "data-worker",
  "host_id": "host-742e5bf0",
  "host_revision": "sandbox-pool-00012-xyz",
  "engine_type": "cloudrun",
  "epoch": 2,
  "status": "RUNNING",
  "template": "python",
  "persistent": true,
  "cpu": "2",
  "memory": "2Gi",
  "allocated_cpu_millicores": 2000,
  "allocated_memory_bytes": 2147483648,
  "used_memory_bytes": 48234496,
  "used_memory_pct": 2.2,
  "peak_memory_bytes": 67108864,
  "cpu_usage_pct": 2.0,
  "peak_cpu_usage_pct": 8.5,
  "allow_egress": true,
  "idle_timeout_sec": 0,
  "overlay_path": "persistence/sbx-host-742e5bf0-a1b2c3d4/overlay.tar",
  "migration_count": 1,
  "crash_recoveries": 0,
  "total_downtime_ms": 34,
  "last_migration_freeze_ms": 34,
  "last_migration_total_ms": 142,
  "last_migration_mode": "prestaged_delta",
  "created_at": "2026-10-18T07:30:00Z",
  "updated_at": "2026-10-18T07:31:10Z",
  "last_active_at": "2026-10-18T07:31:10Z"
}
```

---

### 2.7 `update` — Update CPU, Memory, Idle Timeout, Egress, Env & Metadata in Place

Dynamically updates a sandbox's configuration on the owning host (`POST /__session/update`).

```shell
sbx update <SANDBOX_ID_OR_NAME> [--cpu=<CPU>] [--memory=<MEMORY>] \
    [--idle-timeout=<DURATION> | --auto-suspend=<DURATION> | --no-auto-suspend] \
    [--allow-egress=true|false | --no-allow-egress] [--env=<K=V>] [--metadata=<K=V>] [--json]
```

| Flag | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `--cpu` | `string` | unchanged | Updated CPU allocation (e.g., `4`, `500m`). |
| `--memory` | `string` | unchanged | Updated memory allocation (e.g., `4Gi`). |
| `--idle-timeout` / `--auto-suspend` | `string` | unchanged | Updated auto-suspend timeout (`120`, `2m`, `0s`). |
| `--no-auto-suspend` | `bool` | `false` | Disable idle auto-suspend (`--idle-timeout=0`). |
| `--allow-egress` / `--no-allow-egress` | `bool` | unchanged | Enable or disable outbound internet egress in place. |
| `--env` / `--metadata` | `key=val` | unchanged | Environment variables or metadata labels to merge into `SandboxRecord`. |

```shell
sbx update "$MY_SANDBOX" --cpu=4 --memory=4Gi --idle-timeout=2m --no-allow-egress
# Output: Updated sandbox sbx-host-742e5bf0-a1b2c3d4 (cpu=4, memory=4Gi, idle_timeout=120s)
```

---

### 2.8 `suspend` / `stop` — Checkpoint Overlay to GCS and Release Host Slot

Quiesces the sandbox (running `CHECKPOINT` and `pg_ctl stop` for `postgres`), exports
`persistence/<sandbox_id>/overlay.tar` to GCS, stops the container, and releases the host slot
(`POST /__session/suspend` transitions to `SUSPENDED`; `POST /__session/stop` transitions to
`STOPPED`).

```shell
sbx suspend <SANDBOX_ID_OR_NAME> [--json]
sbx stop <SANDBOX_ID_OR_NAME> [--json]
```

---

### 2.9 `resume` / `start` — Hydrate a Suspended or Stopped Sandbox

Hydrates a `SUSPENDED` or `STOPPED` sandbox onto an open warm `HostSession` (or spawns a new host),
imports `overlay.tar`, increments `epoch`, restarts PostgreSQL, `background_commands`, and
`/workspace/.sbx_init.sh`, and marks the sandbox `RUNNING`.

> [!TIP]
> Running `sbx exec`, `sbx cp`, `sbx port-forward`, or sending an HTTP request to a suspended
> sandbox's preview URL automatically wakes and resumes the sandbox without requiring a manual
> `sbx resume`.

```shell
sbx resume <SANDBOX_ID_OR_NAME> [--json]
sbx start <SANDBOX_ID_OR_NAME> [--json]
```

---

### 2.10 `delete` (Alias: `rm`) — Delete One or More Sandboxes

Stops and deletes one or more sandboxes (`POST /__session/delete`), removing
`sandboxes/<sandbox_id>.json` and `persistence/<sandbox_id>/overlay.tar` from GCS.

```shell
sbx delete <SANDBOX_ID_OR_NAME> [SANDBOX_ID_2...] [--force] [--json]
```

---

### 2.11 `exec` — Execute Commands Inside a Sandbox (Buffered or SSE Stream)

Runs a foreground, real-time SSE streaming (`--stream` / `-s`), or background (`--background` /
`-d`) command inside the sandbox (`POST /__session/exec` or `POST /__session/exec-stream`).

* **Real-Time SSE Streaming (`--stream` / `-s`)**: Streams `stdout` and `stderr` chunks as
  Server-Sent Events (`start`, `stdout`, `stderr`, `exit`) and exits with the remote command's exit
  code.
* **Persistent Background Daemons (`--background` / `-d` & `/workspace/.sbx_init.sh`)**: Commands
  started with `-d` are saved in `background_commands` on `SandboxRecord` and are automatically
  restarted after live migrations, wake-from-suspend, or crash recoveries. If
  `/workspace/.sbx_init.sh` exists, `hostd` also executes it in the background on every
  hydration/resume.

#### Synopsis & Flags

```shell
sbx exec <SANDBOX_ID_OR_NAME> [--stream | -s] [--workdir=<DIR>] [--background | -d] \
    [--timeout=<DURATION>] [--allow-egress=true|false | --no-allow-egress] [--env=<K=V>] \
    -- <COMMAND> [ARGS...]
```

| Flag | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `--stream`, `-s` | `bool` | `false` | Stream `stdout`/`stderr` in real time over SSE (`/__session/exec-stream`). |
| `--workdir` | `string` | `/workspace` | Working directory inside the sandbox. |
| `--background`, `-d` | `bool` | `false` | Run asynchronously in the background, record in `background_commands`, and return PID. |
| `--timeout` | `string` | `60` (`60s`) | Execution timeout in integer seconds (`60`) or Go duration (`30s`, `2m`, `10m`). |
| `--allow-egress` / `--no-allow-egress` | `bool` | sandbox default | Override outbound internet egress for this command execution. |
| `--env` | `key=val` | none | Per-execution environment variables (repeatable or comma-separated). |

#### Examples

```shell
# Foreground command with custom environment
sbx exec "$MY_SANDBOX" --env=DEBUG=1 -- python3 -c "import os; print(os.getenv('DEBUG'))"

# Real-time SSE streaming (-s) with a 5-minute timeout
sbx exec "$MY_SANDBOX" -s --timeout=5m -- sh -c 'for i in 1 2 3; do echo "tick $i"; sleep 1; done'

# Start a persistent background HTTP server (-d)
sbx exec "$MY_SANDBOX" -d -- python3 -m http.server 8000
```

---

### 2.12 `preview-url` (Alias: `preview`) — IAM-Authenticated HTTP/WebSocket Preview URLs

Constructs and prints the reverse-proxy preview URL
(`<control_plane_url>/proxy/<id_or_name>/<port>/<path>`) for a port listening inside a sandbox.

* **Authentication & Header Isolation**: Preview URLs are protected by Cloud Run IAM
  (`roles/run.invoker` on `sandbox-control-plane`), not exposed anonymously. Pass
  `Authorization: Bearer $(gcloud auth print-identity-token)` when calling from `curl` or
  programmatic clients, or open via the IAP-authenticated `sandbox-ui` **"Open Preview URL"** button
  (`/proxy/<id_or_name>/<port>/...`):
  * IAP and platform credentials (`X-Serverless-*`, `X-Goog-Iap-*`, `Authorization`, and
    `GCP_IAAP_*` / `__Host-sbx_csrf` cookies) are stripped at every hop, while application cookies
    are preserved.
  * Send `X-Sandbox-Authorization` if you want your sandbox app to receive an `Authorization`
    header.
  * Root-relative `Location` redirects (e.g., `/login`) stay under `/proxy/<id_or_name>/<port>/...`.
* **WebSockets & Wake-on-HTTP**: Supports HTTP and WebSocket (`Upgrade: websocket`) traffic. If the
  target sandbox is `SUSPENDED`, the first incoming request automatically hydrates the sandbox from
  GCS, restarts `background_commands` and `/workspace/.sbx_init.sh`, retries dialing the target
  port for up to **5 seconds** while the server binds, and serves the response.

#### Synopsis & Flags

```shell
sbx preview-url <SANDBOX_ID_OR_NAME> [PORT] [--port=8000] [--path=/] [--json]
```

| Flag / Arg | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `[PORT]` or `--port` | `int` | `8000` | Target TCP port listening inside the sandbox. |
| `--path` | `string` | `/` | URL subpath appended after `/proxy/<id>/<port>`. |
| `--json` | `bool` | `false` | Emit JSON containing `sandbox_id`, `port`, `preview_url`, and `curl_example`. |

```shell
URL=$(sbx preview-url "$MY_SANDBOX" --port=8000 --path=/healthz)
curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" "$URL"
```

---

### 2.13 `port-forward` (Alias: `forward`) — Local TCP Port Tunneling

Listens on a local TCP port (`127.0.0.1:<LOCAL_PORT>`) and tunnels connections over
`/__session/port-forward` (`Upgrade: tcp`) into `<REMOTE_PORT>` inside the sandbox. Automatically
wakes `SUSPENDED` sandboxes on connect and keeps `activeConns` incremented while tunnels are open.

#### Synopsis & Flags

```shell
sbx port-forward <SANDBOX_ID_OR_NAME> [<LOCAL_PORT>:<REMOTE_PORT> | <PORT>] [--port=<PORT>]
```

| Argument / Flag | Description |
| :--- | :--- |
| `<LOCAL_PORT>:<REMOTE_PORT>` | Forward `127.0.0.1:<LOCAL_PORT>` to `<REMOTE_PORT>` inside the sandbox (e.g., `15432:5432`). |
| `<PORT>` or `--port=<PORT>` | Shorthand when local and remote ports are identical (e.g., `sbx port-forward "$SBX" 8000`). |

```shell
# Forward local port 15432 to PostgreSQL port 5432 inside the sandbox
sbx port-forward "$MY_PG" 15432:5432
```

---

### 2.14 `ssh` — Open an Interactive Line-REPL Shell

Opens an interactive line-oriented command loop connected to `/workspace` inside the sandbox. Type
`exit` or `quit` (or `Ctrl-D`) to disconnect.

> [!NOTE]
> `sbx ssh` is a lightweight line-oriented REPL built on `/__session/exec` without PTY allocation,
> so full-screen terminal programs (`vim`, `top`, `htop`, `less`) are not supported.

```shell
sbx ssh "$MY_SANDBOX"
```

---

### 2.15 `cp` — Copy Files Between Local Machine and Sandbox

Uploads or downloads files (`POST /__session/cp`). Remote paths are validated with
`SafeResolvePath` to prevent path traversal outside the sandbox root.

```shell
# Upload local file to sandbox
sbx cp ./train.py "$MY_SANDBOX":/workspace/train.py [--json]

# Download file from sandbox to local machine
sbx cp "$MY_SANDBOX":/workspace/metrics.json ./metrics.json [--json]
```

---

### 2.16 `logs` — Fetch Stdout, Stderr, and Lifecycle Logs

Fetches command output, background daemon logs, and lifecycle events
(`GET /__session/logs?tail=<N>`).

```shell
sbx logs <SANDBOX_ID_OR_NAME> [--tail=<LINES>] [--json]
```

| Flag | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `--tail` | `int` | `0` (all) | Slice and return only the last `<N>` lines on the server. |

---

### 2.17 `copy` (Alias: `fork`) — Clone a Running Sandbox

Clones a source sandbox's writable filesystem, environment variables, and template settings into a
new sandbox (`POST /__session/copy`) with a newly minted `sbx-<host_id>-<8hex>` ID:

* **Same-Host Copy-on-Write (`fork_method`)**: When `--target-host` is omitted (or matches the
  source host) and the source host has open capacity, `hostd` clones in place via
  `cloudrun_cow_fork` (Cloud Run Sandbox Runtime native CoW fork, with `tar_overlay_clone`
  fallback) or `local_overlay_fork` (`LocalProcessEngine`).
* **Cross-Host Clone (`cross_host_snapshot_clone`)**: When `--target-host` targets a different host
  (or the source host is full), `hostd` exports a snapshot tarball and hydrates the clone onto the
  target host.

#### Synopsis & Flags

```shell
sbx copy <SOURCE_ID_OR_NAME> <TARGET_NAME> [--target-host=<HOST_ID> | --to-host=<HOST_ID>] \
    [--persistent] [--force] [--quiet | -q | --id-only] [--json]
```

| Flag | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `--target-host` (alias `--to-host`) | `string` | same host (if free slot) | Target `HostSession` ID for cross-host cloning. |
| `--persistent` | `bool` | inherits source | Mark the cloned sandbox as persistent (`req.Persistent \|\| srcRec.Persistent`). |
| `--force` | `bool` | `false` | Overwrite target if a conflicting record exists. |
| `--quiet`, `-q`, `--id-only` | `bool` | `false` | Print only the newly minted `sandbox_id` to stdout. |

```shell
sbx copy "$MY_SANDBOX" my-dev-box-clone --persistent
# Output: Copied sandbox sbx-host-742e5bf0-a1b2c3d4 -> sbx-host-742e5bf0-e5f60718 (name=my-dev-box-clone) on host host-742e5bf0 (method=cloudrun_cow_fork)
```

---

### 2.18 `snapshot` (`create`, `list`, `delete` / `rm`) — Point-in-Time Filesystem Snapshots

Captures, lists, or deletes point-in-time filesystem snapshots (`snapshots/<snapshot_id>.json` and
`snapshots/<snapshot_id>.tar` in GCS).

```shell
sbx snapshot create <SANDBOX_ID_OR_NAME> [--name=<SNAPSHOT_ID>] [--force] [--json]
sbx snapshot list [--json]
sbx snapshot delete <SNAPSHOT_ID>   # alias: sbx snapshot rm <SNAPSHOT_ID>
```

| Flag (`snapshot create`) | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `--name` | `string` | `snap-<8hex>` | Custom identifier for the snapshot. |
| `--force` | `bool` | `false` | Overwrite an existing snapshot with the same `--name`. |

---

### 2.19 `template` (`list`, `create` / `add`, `delete` / `rm`) — Built-in & Custom Templates

*(Command alias: `sbx templates`)*

Manages built-in pre-warmed templates (`ubuntu`, `python`, `node`, `postgres` — protected from
deletion/overwrite) and custom snapshot-backed templates.

```shell
sbx template list [--json]
sbx template create <TEMPLATE_NAME> [--from-sandbox=<ID_OR_NAME> | --from-snapshot=<SNAP_ID>] \
    [--base-template=ubuntu|python|node|postgres] [--image=<IMAGE>] [--description=<TEXT>] [--force] [--json]
sbx template delete <TEMPLATE_NAME>   # alias: sbx template rm <TEMPLATE_NAME>
```

| Flag (`template create` / `add`) | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `--from-sandbox` | `string` | `""` | Server-side snapshot `<ID_OR_NAME>` as `snap-tpl-<NAME>` and register template. |
| `--from-snapshot` | `string` | `""` | Back the custom template with an existing `<SNAPSHOT_ID>`. |
| `--base-template` | `string` | `ubuntu` (or source's) | Underlying base runtime template (`ubuntu`, `python`, `node`, `postgres`). |
| `--image` | `string` | base template's image | Container image recorded in `TemplateSpec` (defaults to base template's image, fallback `ubuntu:24.04`). |
| `--description` | `string` | `""` | Human-readable description of the template. |
| `--force` | `bool` | `false` | Overwrite an existing custom template with the same name. |

* Deleting a custom template (`sbx template delete <NAME>`) also cleans up its auto-generated
  `snap-tpl-<NAME>` snapshot.

---

### 2.20 `migrate` — Two-Phase Pre-Copy Live Migration or Host Drain

Live-migrates a single sandbox to a target host (`POST /__session/migrate`) or drains all running
sandboxes from a host (`--host=<SOURCE_HOST_ID>`).

#### How Two-Phase Pre-Copy Live Migration Works

1. **Phase 1 — Warm Pre-Copy (`POST /__internal/prestage`, `0ms` Downtime)**: While the sandbox
   stays `RUNNING` on the source host (`sbxMu` unlocked, continuing to serve `exec` and `/proxy`
   traffic), the source host exports the base filesystem tar and streams it to the destination host
   (multipart up to `30 MiB`, or via a Store blob for larger bases), which unpacks it, warms the
   container, and stores `prestaged[sandboxID]` (`3m` TTL).
2. **Phase 2 — Brief Freeze & Direct Delta Cutover (`POST /__internal/hydrate` with
   `prestaged: true`)**: The source host acquires `sbxMu`, marks the sandbox `MIGRATING`, waits up
   to `2s` for in-flight `exec`/`cp` calls to complete, runs `/workspace/.sbx_pre_migrate.sh` (and
   `Quiesce` `CHECKPOINT` + clean `pg_ctl stop` for `postgres`), computes only modified/deleted
   files since Phase 1 (`ExportDeltaTar`), and streams the delta (`<= 20 MiB` inline) directly
   host-to-host over HTTP. The destination host applies `ApplyDeltaTar`, restarts PostgreSQL,
   `background_commands`, and `.sbx_init.sh`, and increments `epoch` via GCS CAS (`RPO=0`).
3. **Telemetry & Fallback**: `sbx inspect` reports the following migration telemetry fields:

   | `SandboxRecord` Field | Meaning |
   | :--- | :--- |
   | `last_migration_freeze_ms` | Phase 2 cutover freeze window, timed from the source host's `freezeStart`. |
   | `last_migration_total_ms` | End-to-end Phase 1 + Phase 2 migration duration (including any target cold start in Phase 1). |
   | `last_migration_mode` | `"prestaged_delta"` (Two-Phase Pre-Copy) or `"gcs_full"` (automatic full GCS hydration fallback). |
   | `total_downtime_ms` | Cumulative cutover freeze milliseconds across all migrations and crash recoveries. |

   Live Cloud Run (`us-west1`): **`~0.45 s` server freeze / `~0.78 s` client-observed HTTP freeze**
   vs **`~4.38 s` before (−82% / `5.6×` shorter)**; see
   [ARCHITECTURE.md §2.3](ARCHITECTURE.md#23-two-phase-pre-copy-live-migration).

#### Synopsis & Flags

```shell
sbx migrate <SANDBOX_ID_OR_NAME> [--to-host=<TARGET_HOST_ID> | --target-host=<TARGET_HOST_ID>] [--json]
sbx migrate --host=<SOURCE_HOST_ID> [--json]
```

| Flag | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `--to-host` (alias `--target-host`) | `string` | auto-selected | Destination `HostSession` ID (sets `pinned_host=true`). |
| `--host` | `string` | `""` | Evacuate and drain all running sandboxes from `<SOURCE_HOST_ID>`. |

```shell
sbx migrate "$MY_SANDBOX" --to-host=host-91c4e012
# Output: Migrated sandbox sbx-host-742e5bf0-a1b2c3d4 from host-742e5bf0 -> host-91c4e012 (epoch=2)

sbx migrate --host=host-742e5bf0
# Output: Drained host host-742e5bf0 (migrated: sbx-host-742e5bf0-a1b2c3d4->host-91c4e012)
```

---

### 2.21 `hosts` (`list`, `drain`, `compact`, `simulate-crash`) — Manage Host Sessions

Inspects active `sandbox-pool` host clones (`GET /v1/hosts`), drains a host
(`POST /v1/hosts/<id>/drain`), triggers bin-packing compaction across sparse hosts
(`POST /v1/compact`), or simulates an ungraceful `SIGKILL` crash
(`POST /v1/hosts/<id>/simulate-crash`) to test watchdog recovery.

```shell
sbx hosts list [--json]
sbx hosts drain <HOST_ID> [--json]
sbx hosts compact [--json]
sbx hosts simulate-crash <HOST_ID> [--json]
```

---

### 2.22 `config` (`get`, `set`) — View and Tune Fleet-Wide Settings

Reads (`GET /v1/config`) or dynamically updates (`POST /v1/config`) `fleet/config.json`. Changing
`--host-cpu` or `--host-memory` against a deployed Cloud Run fleet automatically rolls out a new
`sandbox-pool` revision with the updated sizing.

#### Synopsis & Flags (`config set`)

```shell
sbx config get
sbx config set [--max-density=<N>] [--host-cpu=<VCPU>] [--host-memory=<MEMORY>] \
    [--min-free-memory-mb=<MB>] [--min-free-memory-pct=<PCT>] [--max-memory-overcommit-ratio=<RATIO>] \
    [--hot-memory-threshold-pct=<PCT>] [--hot-cpu-threshold-pct=<PCT>] [--min-client-version=<SEMVER>] \
    [--project=<PROJECT_ID>] [--region=<REGION>] [--periodic-sync-sec=<SEC>] \
    [--default-idle-timeout=<SEC> | --auto-suspend=<SEC>] [--heartbeat-timeout-sec=<SEC>] \
    [--auto-bin-pack=true|false] [--target-revision=<REV>] [--pool-url=<URL>]
```

| Flag | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `--max-density` | `int` | `10` | Max concurrent sandboxes packed onto a single host clone. |
| `--host-cpu` / `--host-memory` | `string` | `4` / `8Gi` | vCPU and RAM allocation per `sandbox-pool` host clone. |
| `--min-free-memory-mb` | `int` | `512` | Minimum free host RAM (MiB) before `HTTP 429` (`host_memory_pressure`). |
| `--min-free-memory-pct` | `float` | `15.0` | Minimum free host RAM `%` before `HTTP 429` (`host_memory_pressure`). |
| `--max-memory-overcommit-ratio` | `float` | `2.0` | Max ratio of allocated sandbox memory to physical `HOST_MEMORY` (`active >= 1`). |
| `--hot-memory-threshold-pct` | `float` | `85.0` | Host memory `%` that triggers proactive live migration (`HOT_SHEDDING`). |
| `--hot-cpu-threshold-pct` | `float` | `90.0` | Host CPU `%` that triggers proactive live migration (`HOT_SHEDDING`). |
| `--min-client-version` | `string` | `1.0.0` | Minimum required client semver; older clients receive `HTTP 426 Upgrade Required`. |
| `--project` / `--region` | `string` | active / `us-west1` | GCP project and region used when rolling out `sandbox-pool` CPU/memory updates. |
| `--periodic-sync-sec` | `int` | `10` | Stored in `FleetConfig`; `hostd` currently uses a fixed 3s dirty / 10s full sweep (see [ARCHITECTURE.md §6](ARCHITECTURE.md#6-system-timers--defaults-reference)). |
| `--default-idle-timeout` | `int` | `0` (disabled) | Fleet-wide default auto-suspend timeout in seconds (alias: `--auto-suspend`). |
| `--heartbeat-timeout-sec` | `int` | `15` | Seconds without a heartbeat before the watchdog declares a host `DEAD`. |
| `--auto-bin-pack` | `bool` | `true` | Enable background bin-packing defragmentation by the control-plane leader. |
| `--target-revision` | `string` | `""` | Target Cloud Run revision; older hosts drain and migrate sandboxes onto it. |
| `--pool-url` | `string` | auto-configured | Override the `sandbox-pool` URL advertised via `/v1/candidates`. |

#### Example

```shell
sbx config get
```

```json
{
  "max_density": 10,
  "host_cpu": "4",
  "host_memory": "8Gi",
  "min_free_memory_mb": 512,
  "min_free_memory_pct": 15,
  "max_memory_overcommit_ratio": 2,
  "hot_memory_threshold_pct": 85,
  "hot_cpu_threshold_pct": 90,
  "min_client_version": "1.0.0",
  "server_version": "1.2.0",
  "target_revision": "",
  "pool_service_url": "https://sandbox-pool-xxxxx-uw.a.run.app",
  "control_plane_url": "https://sandbox-control-plane-xxxxx-uw.a.run.app",
  "periodic_sync_sec": 10,
  "default_idle_timeout_sec": 0,
  "heartbeat_timeout_sec": 15,
  "grace_period_sec": 600,
  "auto_bin_pack": true,
  "bin_pack_hysteresis": 1,
  "updated_at": "2026-10-18T07:42:00Z"
}
```

```shell
# Scale host size to 8 vCPU / 16Gi and raise density to 15
sbx config set --max-density=15 --host-cpu=8 --host-memory=16Gi
```

---

### 2.23 `stats` & `metrics` — Fleet SLA and Overcommit Observability

* **`sbx stats [--json]`**: Queries `GET /v1/stats` to display active/draining/dead hosts,
  running/suspended sandboxes, capacity slot utilization, bin-packing efficiency, live migrations,
  crash recoveries, and fleet uptime SLA.
* **`sbx metrics [--host=<ID>] [--sandbox=<ID>] [--history] [--json]`**: Queries `GET /v1/metrics`
  to display CPU/memory overcommit ratios, per-host `overload_state` (`NORMAL`, `ELEVATED`,
  `ADMISSION_BLOCKED`, `HOT_SHEDDING`), rolling 120-sample time-series history (`--history`), and
  automated sizing advice. See
  **[OVERLOAD_PROTECTION.md §2](OVERLOAD_PROTECTION.md#2-real-time--historical-resource-observability-sbx-metrics)**
  for full sample output and field definitions.

```shell
sbx stats
```

```text
Fleet Summary (Leader: cp-a1b2c3d4)
  Hosts:             2 active / 0 draining / 0 dead (total=2)
  Sandboxes:         18 running / 4 suspended (total=22)
  Capacity Slots:    18 / 20 used (90.0% bin-pack efficiency, max_density=10)
  Live Migrations:   14 (Hot-Host Shedding: 1, Memory Rejections: 2)
  Overcommit Ratio:  Memory 1.50x / CPU 1.25x
  Crash Recoveries:  2
  Fleet Uptime SLA:  99.994%
```

---

### 2.24 `ui` — Web Console UI (`sandbox-ui` / `sbx ui`)

Runs or accesses the interactive **Web Console BFF** (`ui.go`):

* **Hosted Cloud Run Service (`sandbox-ui`)**: Deployed at
  `https://sandbox-ui-<hash>-<region>.a.run.app` behind Cloud Identity-Aware Proxy (IAP). Discover
  its URL with:
  ```shell
  gcloud run services describe sandbox-ui --region=us-west1 --format='value(status.url)'
  ```
* **Local Workstation Server (`sbx ui`)**: Starts the BFF locally on `http://127.0.0.1:8080`,
  reusing `~/.cache/sbx/endpoints.json` and `token_cache.json` (or reading `CONTROL_PLANE_URL` and
  `POOL_SERVICE_URL` from the environment):
  ```shell
  sbx ui
  # Custom port or bind address:
  PORT=9090 BIND_HOST=127.0.0.1 sbx ui
  ```

#### Web Console Tabs & Security Hardening

1. **Overview & Metrics (`/api/overview`, `/api/metrics`)**: Live capacity gauges, CPU/memory
   overcommit ratios, rolling per-host resource charts, and sizing recommendations.
2. **Sandboxes, Live SSE Exec & "Open Preview URL" (`/api/sandboxes/*`, `/proxy/<id>/<port>/*`)**:
   Create, inspect, resize, suspend/resume, live-migrate, clone, snapshot, delete, and stream
   real-time interactive commands (`/api/sandboxes/exec-stream`) and logs. Clicking **Open Preview
   URL** opens `/proxy/<sandbox_id>/<port>/` on `sandbox-ui` to preview web servers inside the
   sandbox with automatic header isolation and Wake-on-HTTP; see
   [§2.12 `preview-url`](#212-preview-url-alias-preview--iam-authenticated-httpwebsocket-preview-urls)
   and [ARCHITECTURE.md §5.2](ARCHITECTURE.md#52-web-console-security-uigo--cloud-iap--iam-reconciliation).
3. **Host Instances (`/api/hosts/action`)**: Visual slot/memory bars with one-click host drain,
   fleet bin-pack compaction, and simulated crash-recovery testing.
4. **Config, Templates & Security (`/api/config`, `/api/templates`, `/api/security/policy`)**: Tune
   `FleetConfig` parameters, manage custom templates, and edit Cloud IAP (`ui_access_members`) and
   Cloud Run IAM (`backend_access_members`) policies (protected by the `self_revocation_forbidden`
   safeguard).
5. **Security Posture (`ui.go`)**: Enforces per-request cryptographic CSP nonces
   (`script-src 'self' 'nonce-...'`, `frame-ancestors 'none'`, `object-src 'none'`),
   `X-Frame-Options: DENY`, `X-Content-Type-Options: nosniff`, constant-time CSRF double-submit
   cookies (`__Host-sbx_csrf` over HTTPS / `sbx_csrf` over HTTP paired with `X-CSRF-Token`), and a
   `250 req/s` rate limiter. Local `sbx ui` binds exclusively to `127.0.0.1` unless `BIND_HOST` or
   `K_SERVICE` is set.

---

## 3. End-to-End Cookbook Examples

### Example 1: Ephemeral Python Data Science Sandbox & File Transfer

Provision a Python 3.12 sandbox, upload a CSV dataset, dynamically resize to 4 vCPU / 4Gi RAM
before aggregation, download the result, and clean up:

```shell
# 1. Create the sandbox (2 vCPU / 2Gi RAM) and capture its unique sandbox_id
MY_SANDBOX=$(sbx create py-etl --template=python --cpu=2 --memory=2Gi -q)

# 2. Upload local input data into /workspace/sales.csv
cat << 'EOF' > /tmp/sales.csv
region,revenue
na,1200.50
eu,980.25
apac,1540.75
EOF
sbx cp /tmp/sales.csv "$MY_SANDBOX":/workspace/sales.csv

# 3. Dynamically scale up the sandbox to 4 vCPU / 4Gi RAM in place
sbx update "$MY_SANDBOX" --cpu=4 --memory=4Gi

# 4. Run an aggregation script inside the sandbox
sbx exec "$MY_SANDBOX" -- python3 -c "
import csv, json
with open('/workspace/sales.csv') as f:
    total = sum(float(r['revenue']) for r in csv.DictReader(f))
with open('/workspace/summary.json', 'w') as out:
    json.dump({'total_revenue': total}, out)
print('Computed total_revenue:', total)
"

# 5. Download the output artifact and delete the sandbox
sbx cp "$MY_SANDBOX":/workspace/summary.json /tmp/summary.json
cat /tmp/summary.json
sbx delete "$MY_SANDBOX"
```

---

### Example 2: Custom Environment Template & Fast Copy-on-Write Cloning

Configure a base sandbox once, promote it to a reusable template, and fan out Copy-on-Write clones:

```shell
# 1. Create a base sandbox and populate /workspace/lib/config.py
MY_SANDBOX=$(sbx create base-env --template=python --cpu=2 --memory=2Gi -q)
sbx exec "$MY_SANDBOX" -- sh -c "mkdir -p /workspace/lib && echo 'VERSION = \"2.1.0\"' > /workspace/lib/config.py"

# 2. Register a reusable custom template directly from the live sandbox
sbx template create custom-agent-env \
    --from-sandbox="$MY_SANDBOX" \
    --description="Pre-configured agent workspace with /workspace/lib/config.py" \
    --force

# 3. Fork 3 Copy-on-Write clones from the running base sandbox
for i in 1 2 3; do
  CLONE_ID=$(sbx copy "$MY_SANDBOX" "agent-branch-${i}" -q)
  sbx exec "$CLONE_ID" -- python3 -c "import sys; sys.path.append('/workspace/lib'); import config; print('Clone ${i}:', config.VERSION)"
  sbx delete "$CLONE_ID"
done

# 4. Boot a brand-new sandbox anywhere in the fleet from the custom template
NEW_SANDBOX=$(sbx create fresh-agent --template=custom-agent-env -q)
sbx exec "$NEW_SANDBOX" -- cat /workspace/lib/config.py
sbx delete "$MY_SANDBOX" "$NEW_SANDBOX"
sbx template delete custom-agent-env
```

---

### Example 3: Persistent PostgreSQL 16 Sandbox Surviving Live Migration & Crash

The built-in `postgres` template initializes PostgreSQL 16 at `PGDATA=/pgdata/data` inside the
persisted overlay (automatically setting `persistent=true`). All committed rows survive live
migration and watchdog crash recovery (`RPO=0`):

```shell
# 1. Create a PostgreSQL 16 sandbox (persistent=true is automatic for postgres)
MY_PG=$(sbx create pg-orders --template=postgres --cpu=2 --memory=2Gi -q)

# 2. Create a table and insert committed rows
sbx exec "$MY_PG" -- psql -U postgres -d postgres -c "
  CREATE TABLE IF NOT EXISTS orders (id SERIAL PRIMARY KEY, item TEXT, amount INT);
  INSERT INTO orders (item, amount) VALUES ('gpu-node', 4), ('tpu-slice', 8);
"

# 3. Live-migrate the running PostgreSQL sandbox to another host session
sbx migrate "$MY_PG"
sbx exec "$MY_PG" -- psql -U postgres -d postgres -tAc "SELECT item, amount FROM orders ORDER BY id;"
# Output:
# gpu-node|4
# tpu-slice|8

# 4. Simulate an ungraceful SIGKILL crash on the current host and verify watchdog recovery
CUR_HOST=$(sbx inspect "$MY_PG" | python3 -c "import sys, json; print(json.load(sys.stdin)['host_id'])")
sbx hosts simulate-crash "$CUR_HOST"
sbx exec "$MY_PG" -- psql -U postgres -d postgres -tAc "SELECT COUNT(*) FROM orders;"
# Output: 2

sbx delete "$MY_PG"
```

---

### Example 4: Cost Optimization with Idle Auto-Suspend, In-Guest `IDLE` Signaling & Bin-Packing

You can minimize Cloud Run compute costs using three mechanisms:

1. **Per-Sandbox Idle Timeout (`--idle-timeout` / `--auto-suspend`)**: Suspends a sandbox after `N`
   seconds or duration (`5m`) without `exec`, `exec-stream`, `cp`, `proxy`, or `port-forward`
   activity (and automatically enables `persistent=true`).
2. **In-Guest Cooperative Self-Suspend (`$INSTANCE_STATE_FILE`)**: Any process inside a sandbox can
   write `IDLE`, `SUSPEND`, `STOP`, or `STOPPED` to `$INSTANCE_STATE_FILE`
   (`/var/run/instance/state`, also symlinked at `/workspace/.instance_state`). `hostd` polls this
   file every `400ms` and immediately checkpoints and transitions the sandbox to `SUSPENDED`
   without waiting for an idle timer.
3. **Fleet Bin-Packing (`sbx hosts compact`)**: Consolidates sandboxes from sparse hosts onto fewer
   warm clones so drained hosts scale to zero.

```shell
# 1. Create a sandbox with a 60-second idle timeout (automatically sets persistent=true)
MY_SANDBOX=$(sbx create batch-worker --template=ubuntu --idle-timeout=60 -q)

# 2. Run a job that writes output to /persistent and signals IDLE via $INSTANCE_STATE_FILE
sbx exec "$MY_SANDBOX" -- sh -c '
  echo "checkpoint-complete" > /persistent/status.txt
  echo "IDLE" > "${INSTANCE_STATE_FILE:-/var/run/instance/state}"
'

# 3. Once suspended, any subsequent sbx exec wakes and resumes the sandbox automatically
sbx exec "$MY_SANDBOX" -- cat /persistent/status.txt
# Output: checkpoint-complete

# 4. Trigger fleet-wide bin-packing compaction and clean up
sbx hosts compact
sbx delete "$MY_SANDBOX"
```

---

### Example 5: High-Throughput Parallel Batch Creation

Because `sbx create` routes directly to candidate hosts and mints collision-free
`sbx-<host_id>-<8hex>` IDs even when sandboxes share the same `--name`, you can fan out parallel
sandboxes from a script or CI job without central lock contention:

```shell
#!/usr/bin/env bash
set -euo pipefail

IDS_FILE="$(mktemp)"
trap 'rm -f "${IDS_FILE}"' EXIT

for shard in $(seq 1 20); do
  (
    MY_SANDBOX=$(sbx create ci-shard --template=python --metadata="shard=${shard}" -q)
    echo "$MY_SANDBOX" >> "${IDS_FILE}"
    sbx exec "$MY_SANDBOX" -- python3 -c "print('Completed shard ${shard} on', '$MY_SANDBOX')"
  ) &
done
wait

xargs -P 10 -n 1 sbx delete < "${IDS_FILE}"
```

---

**Navigation:** [Overview & Quick Start (README.md)](README.md) ·
**User Guide & CLI Reference (USER_GUIDE.md)** ·
[Architecture & Maintainer Guide (ARCHITECTURE.md)](ARCHITECTURE.md) ·
[Overload Protection (OVERLOAD_PROTECTION.md)](OVERLOAD_PROTECTION.md)
