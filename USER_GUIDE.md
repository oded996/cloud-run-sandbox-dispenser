# Cloud Run Sandbox Dispenser — User Guide & CLI Reference

This guide provides complete documentation for installing, initializing,
authenticating, and operating the **Cloud Run Sandbox Dispenser** CLI (`sbx`),
along with end-to-end cookbook recipes for stateful agent sandboxes, custom
templates, Copy-on-Write (CoW) cloning, and PostgreSQL 16 persistence.

> [!NOTE]
> For an overview of key capabilities, see
> **[README.md](README.md)**. For resource overcommit observability,
> memory-aware admission control, and proactive hot-host live migration, see
> **[OVERLOAD_PROTECTION.md](OVERLOAD_PROTECTION.md)**. For system
> architecture, Cloud Run session routing internals, and state consistency
> design, see **[ARCHITECTURE.md](ARCHITECTURE.md)**.

[TOC]

## TL;DR — 30-Second Basic Usage

```shell
# Prerequisite: run 'sbx init <CONTROL_PLANE_URL>' or 'sbx init --project=<PROJECT>' once (see Section 1 below)

# 1. Create a persistent Python sandbox (1 vCPU / 1Gi RAM) and capture its unique sandbox_id
MY_SANDBOX=$(sbx create my-dev-box \
    --template=python \
    --cpu=1 \
    --memory=1Gi \
    --persistent \
    --quiet)
echo "Created sandbox: $MY_SANDBOX"
# Output: Created sandbox: sbx-host-742e5bf0-a1b2c3d4
# (Or with JSON output: MY_SANDBOX=$(sbx --json create my-dev-box --template=python --cpu=1 --memory=1Gi --persistent | jq -r '.sandbox_id'))

# 2. Run a command and copy a local script into the sandbox using $MY_SANDBOX
sbx exec "$MY_SANDBOX" -- \
    python3 -c "print('Hello from Cloud Run!')"
echo "print('Hello from script.py!')" > ./script.py
sbx cp ./script.py "$MY_SANDBOX":/workspace/script.py
sbx exec "$MY_SANDBOX" -- python3 /workspace/script.py

# 3. List running sandboxes (multiple sandboxes can share the same name)
sbx list
# Output:
# SANDBOX_ID                   NAME         STATUS    TEMPLATE  HOST_ID         EPOCH  PERSISTENT  MIGRATIONS  RECOVERIES
# sbx-host-742e5bf0-a1b2c3d4   my-dev-box   RUNNING   python    host-742e5bf0   1      true        0           0

# 4. Dynamically resize CPU/RAM, or suspend and resume the sandbox (preserving /workspace and releasing the host slot)
sbx update "$MY_SANDBOX" --cpu=2 --memory=2Gi
sbx suspend "$MY_SANDBOX"
sbx resume "$MY_SANDBOX"

# 5. Delete the sandbox when done
sbx delete "$MY_SANDBOX"
```

-----

## 1\. Installation, Setup & Authentication

### Building the `sbx` CLI

Build `sbx` using standard Go tooling (or `make build`) and alias it in your
shell:

```shell
git clone https://github.com/oded996/cloud-run-sandbox-dispenser.git
cd cloud-run-sandbox-dispenser
go build -trimpath -ldflags="-s -w" -o sbx .
alias sbx="$(pwd)/sbx"

# Or install directly via Go (works in Google Cloud Shell & local shells, including 'sbx deploy'):
go install github.com/oded996/cloud-run-sandbox-dispenser@latest
ln -sf "$(go env GOPATH | cut -d: -f1)/bin/cloud-run-sandbox-dispenser" "$(go env GOPATH | cut -d: -f1)/bin/sbx"
export PATH="$(go env GOPATH | cut -d: -f1)/bin:${PATH}"
```

### Zero-Environment-Variable Setup (`sbx init` or `sbx deploy`)

You do **not** need to manually set environment variables or fetch
authentication tokens. Choose either **Option A** (connecting to an existing
deployment) or **Option B** (deploying a new fleet):

#### Option A — Connect to an Existing Dispenser (`sbx init`)

Run `sbx init` with the `sandbox-control-plane` URL (or pass `--project` and
`--region` so `sbx init` looks up the service URL via `gcloud`):

```shell
sbx init https://sandbox-control-plane-xxxxx-uw.a.run.app
# Or discover by project and region:
# sbx init --project=my-gcp-project --region=us-west1
```

`sbx init` automatically:

1.  Acquires and caches your Google Cloud OIDC identity token in
    `~/.cache/sbx/token_cache.json`.
2.  Queries `/v1/candidates` on the control plane to discover the `sandbox-pool`
    URL, `max_density` (default `10`), host size (`4` vCPU / `8Gi` RAM), and
    initial warm host candidates.
3.  Saves the discovered endpoints in `~/.cache/sbx/endpoints.json` and
    candidate list in `~/.cache/sbx/candidates.json`.

#### Option B — Deploy a New Dispenser from Scratch (`sbx deploy`)

Each `sandbox-pool` host clone defaults to **4 vCPU and 8Gi RAM** (`--host-cpu=4
--host-memory=8Gi`), which pairs with the default bin-packing density
(`max_density=10`) to comfortably host up to 10 sandboxes per warm clone:

```shell
sbx deploy \
    --project=my-gcp-project \
    --region=us-west1 \
    --host-cpu=4 \
    --host-memory=8Gi
```

`sbx deploy` builds and deploys the Cloud Run services (`sandbox-control-plane`
and `sandbox-pool`), configures IAM invoker bindings, and **automatically runs
`sbx init`** at the end so your local `~/.cache/sbx/` directory is immediately
configured with the control plane URL, pool URL, and cached auth token.

You can also modify the host vCPU/RAM allocation at any time after deployment
using `sbx config set`:

```shell
sbx config set --host-cpu=8 --host-memory=16Gi
```

### How Authentication & Token Caching Works

All backend services (`sandbox-pool` and `sandbox-control-plane`) enforce Cloud
Run IAM authentication. **You never need to manually fetch or export tokens**:

*   **Automatic Acquisition & Caching**: When `sbx init`, `sbx deploy`, or any
    `sbx` command communicates with a Cloud Run `https://*.run.app` endpoint,
    `sbx` automatically invokes the Google Cloud CLI (`gcloud auth
    print-identity-token` for Cloud Run IAM requests and `gcloud auth
    print-access-token` for GCS operations, or queries the GCE/Cloud Run
    metadata server when running inside GCP).

*   **Zero Per-Command Overhead**: The acquired token is cached on disk in
    `~/.cache/sbx/token_cache.json` (mode `0600`, 45-minute TTL) and refreshed
    automatically before expiration, so subsequent `sbx` commands run in
    milliseconds with zero `gcloud` subprocess overhead.

*   **Manual Inspection / Direct `curl` Access**: If you ever want to inspect
    your OIDC identity token or call the REST endpoints directly with `curl`,
    you can run:

    ```shell
    gcloud auth print-identity-token
    ```

### Local Cache Files in `~/.cache/sbx/`

| File               | Purpose                                                                                                                                                                               |
| :----------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `endpoints.json`   | Persists `control_plane_url` and `pool_service_url` configured by `sbx init` or `sbx deploy`.                                                                                         |
| `token_cache.json` | Caches OIDC identity tokens and OAuth2 access tokens across CLI runs (mode `0600`).                                                                                                   |
| `candidates.json`  | Cached response from `GET /v1/candidates` (`candidates[]`, `pool_service_url`, `max_density`, `fetched_at`). Refreshed adaptively when expired or when local free slots drop to zero. |
| `routes.json`      | Maps `<sandbox_id> -> <host_session_id>` and `<name> -> <sandbox_id>`. Automatically updated on `create`, `copy`, `migrate`, and self-healed if a sandbox migrates to a new host.     |

### Core Concepts: Hosts, Persistence & Execution Engines

*   **Host (`host-<id>`)**: A single Cloud Run session instance in the
    `sandbox-pool` service. Each host defaults to **4 vCPU and 8Gi RAM**
    (`--host-cpu=4 --host-memory=8Gi`) and bin-packs up to `max_density`
    (default `10`) isolated sandboxes concurrently. When a host has `0` active
    sandboxes, it marks itself `idle` so Cloud Run scales it down to zero.
*   **Persistent (`--persistent`) vs. Ephemeral Sandboxes**:
    *   **Persistent (`--persistent`)**: Continuously backs up the sandbox's
        writable filesystem (`/workspace`, `/persistent`, and PostgreSQL
        `/pgdata/data`) to Cloud Storage
        (`persistence/<sandbox_id>/overlay.tar`) every `periodic_sync_sec`
        (default `10s`) and before every `suspend` or `migrate`. Persistent
        sandboxes survive suspend/resume cycles, live migrations, revision
        rollouts, and unexpected host crashes.
    *   **Ephemeral (default when `--persistent` is omitted)**: Fast scratch
        environments for short-lived tasks. Ephemeral sandboxes still support
        in-place `exec`/`cp` and explicit `suspend`/`resume`, but do not run
        periodic background GCS syncs.
*   **Execution Engine (`cloudrun` vs. `local`)**:
    *   **`cloudrun`**: On Cloud Run environments with the Cloud Run Sandbox Runtime
        micro-container runtime socket (`/run/gcp/sandbox/sandbox.sock`), each sandbox
        runs inside a dedicated Cloud Run Sandbox Runtime container with full
        container/filesystem namespace isolation and hardware-enforced cgroup
        `--cpu` and `--memory` limits.
    *   **`local`**: On standard Cloud Run projects where the Cloud Run Sandbox Runtime socket is
        not present, `hostd` automatically operates in `engine_type: "local"`
        process-sandbox mode. Each sandbox is given its own private working
        directory tree (`chmod 0700` containing `/workspace`, `/persistent`,
        `/pgdata/data`) inside the shared host container without separate kernel
        namespaces, sharing the host image's pre-installed userland (`python3`,
        `node`, `psql`, `git`, `curl`) while `--cpu` and `--memory` are tracked
        as scheduling metadata on the `SandboxRecord`.

### Understanding Sandbox IDs vs. Sandbox Names

To allow fast, parallel sandbox creation across the fleet without central naming
collisions:

*   **Sandbox Name (`name`)**: An optional human-friendly label passed via `sbx
    create <NAME>` or `sbx create --name=<NAME>` (must match
    `^[a-zA-Z0-9][a-zA-Z0-9._-]{0,62}$`). If omitted, the sandbox's `name`
    defaults to its generated `sandbox_id`. **Multiple sandboxes across the
    fleet can share the same `name`**.
*   **Sandbox ID (`sandbox_id`)**: A globally unique identifier minted by the
    creating host session in the format: `sbx-<host_id>-<8_hex_chars>` (for
    example, `sbx-host-742e5bf0-a1b2c3d4`).
*   **Why `sandbox_id` Is Structured This Way**:
    1.  **Zero-Contention Parallel Creation**: Ten concurrent workers can all
        run `sbx create worker` simultaneously; each receives its own unique
        `sandbox_id` (`sbx-host-01-11223344`, `sbx-host-02-55667788`, etc.)
        without overwriting each other.
    2.  **Direct Host Routing**: Because the initial `<host_id>` is embedded in
        the `sandbox_id`, `sbx` can route commands directly to the host running
        your sandbox (and automatically update its local route cache if the
        sandbox live-migrates to another host; see
        **[ARCHITECTURE.md](ARCHITECTURE.md)** for protocol details).
    3.  **Capturing `$MY_SANDBOX`**: Because multiple sandboxes can share the
        same `name`, the recommended practice is to capture the unique
        `sandbox_id` on creation using `--quiet` (`-q` / `--id-only`) or
        `--json`: `MY_SANDBOX=$(sbx create my-dev-box --template=python
        --quiet)` and pass `"$MY_SANDBOX"` to subsequent commands.
    4.  **Ambiguous Name Protection**: If only **one** active sandbox has a
        given `name`, you can pass either its `name` or its `sandbox_id` to `sbx
        exec`, `inspect`, `suspend`, `delete`, etc. If **two or more** active
        sandboxes share the same `name`, passing the ambiguous `name` is safely
        rejected with an error listing all matching `sandbox_id`s (`ambiguous
        sandbox name "worker" matches 2 sandboxes: sbx-..., sbx-...; specify the
        exact sandbox_id`) so you never accidentally mutate or delete the wrong
        sandbox.

### Global Flags

Global flags may be passed before any subcommand:

| Flag                        | Description                                                            | Default                                           |
| :-------------------------- | :--------------------------------------------------------------------- | :------------------------------------------------ |
| `--control-plane-url=<URL>` | Override the `sandbox-control-plane` URL for a single invocation.      | Configured in `~/.cache/sbx/endpoints.json`       |
| `--pool-url=<URL>`          | Override the `sandbox-pool` URL for a single invocation.               | Auto-discovered via `sbx init` / `/v1/candidates` |
| `--cache-dir=<DIR>`         | Directory for local route, candidate, endpoint, and OIDC token caches. | `~/.cache/sbx`                                    |
| `--config-dir=<DIR>`        | Alias for `--cache-dir`.                                               | `~/.cache/sbx`                                    |
| `--json`                    | Emit structured JSON output instead of human-readable tables/messages. | `false`                                           |
| `--help`, `-h`              | Display global or subcommand usage information.                        | `false`                                           |

### Advanced: Optional Environment Variable Overrides (CI / Multi-Tenant Testing)

Normal users never need to set environment variables because `sbx init` and `sbx
deploy` configure `~/.cache/sbx/` automatically. For stateless CI runners or
isolated multi-tenant test workers, the following optional overrides are
supported:

| Environment Variable    | Purpose                                                                                                  |
| :---------------------- | :------------------------------------------------------------------------------------------------------- |
| `SBX_CONTROL_PLANE_URL` | Overrides the `sandbox-control-plane` service URL when `--control-plane-url` is not specified.           |
| `SBX_POOL_URL`          | Overrides the `sandbox-pool` service URL when `--pool-url` is not specified.                             |
| `SBX_CACHE_DIR`         | Overrides the cache directory (`~/.cache/sbx`) for isolated test workers.                                |
| `SBX_ID_TOKEN`          | Explicit OIDC Identity Token override for `Authorization: Bearer <token>` on `https://` Cloud Run calls. |

-----

## 2\. Complete Command-by-Command CLI Reference

### 2.1 `init` — Initialize Local CLI Configuration from Control Plane

Connects to an existing `sandbox-control-plane` service (either by explicit URL
or by discovering it via `--project` and `--region`), automatically fetches and
caches your Google Cloud OIDC identity token, discovers the `sandbox-pool` URL
and initial candidate host list from `/v1/candidates`, and persists the
configuration in `~/.cache/sbx/`.

#### Synopsis

```shell
sbx init [CONTROL_PLANE_URL] \
    [--url=<CONTROL_PLANE_URL>] \
    [--project=<PROJECT_ID>] \
    [--region=<REGION>] \
    [--json]
```

#### Flags

| Flag        | Type     | Default                          | Description                                                                          |
| :---------- | :------- | :------------------------------- | :----------------------------------------------------------------------------------- |
| `--url`     | `string` | positional `[CONTROL_PLANE_URL]` | HTTPS URL of the `sandbox-control-plane` Cloud Run service.                          |
| `--project` | `string` | `my-gcp-project`                 | GCP project ID used to auto-discover `sandbox-control-plane` when no URL is given.   |
| `--region`  | `string` | `us-west1`                       | Cloud Run region used to auto-discover `sandbox-control-plane` when no URL is given. |

#### Examples

```shell
# Initialize directly from a control plane URL
sbx init https://sandbox-control-plane-xxxxx-uw.a.run.app
# Output:
# Initialized sbx configuration in ~/.cache/sbx
#   Control Plane URL: https://sandbox-control-plane-xxxxx-uw.a.run.app
#   Pool Service URL:  https://sandbox-pool-xxxxx-uw.a.run.app
#   Host Size:         4 vCPU / 8Gi RAM
#   Max Density:       10 sandboxes/host
#   Auth Token:        Cached via gcloud (token_cache.json)

# Or auto-discover the control plane URL from your GCP project & region
sbx init --project=my-gcp-project --region=us-west1
```

-----

### 2.2 `deploy` — Deploy or Update the Dispenser Fleet on Cloud Run

Builds the container image via Cloud Build (when `deploy.sh` is present in the
working directory), deploys `sandbox-control-plane`, `sandbox-pool`, and
`sandbox-ui` to Cloud Run (configuring each `sandbox-pool` host clone with
`--host-cpu` and `--host-memory`), and **automatically runs `sbx init`** so your
local CLI is immediately ready to use.

Once `sandbox-control-plane` is reachable, `sbx deploy` delegates `sandbox-pool`
and `sandbox-ui` rollout, mutual URL wiring (`CONTROL_PLANE_URL`,
`POOL_SERVICE_URL`), and fleet configuration reconciliation to `POST /v1/deploy`
on the control plane—allowing you to reconcile or resize the fleet from any
directory once initialized.

#### Prerequisites for a Brand-New GCP Project

Before running `sbx deploy` in a new GCP project for the first time, ensure you
are authenticated with `gcloud` and enable the required Google Cloud APIs:

```shell
gcloud auth login
gcloud services enable \
    run.googleapis.com \
    cloudbuild.googleapis.com \
    artifactregistry.googleapis.com \
    storage.googleapis.com \
    --project=<PROJECT_ID>
```

#### Synopsis

```shell
sbx deploy \
    [--project=<PROJECT_ID>] \
    [--region=<REGION>] \
    [--bucket=<GCS_BUCKET>] \
    [--host-cpu=<VCPU>] \
    [--host-memory=<MEMORY>] \
    [--max-density=<N>]
```

#### Flags

| Flag            | Type     | Default                                                               | Description                                                                                                           |
| :-------------- | :------- | :-------------------------------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------- |
| `--project`     | `string` | `my-gcp-project`                                                      | Google Cloud project ID.                                                                                              |
| `--region`      | `string` | `us-west1`                                                            | Cloud Run deployment region.                                                                                          |
| `--bucket`      | `string` | `<PROJECT_ID>-sandbox-dispenser` (`my-gcp-project-sandbox-dispenser`) | GCS bucket name for state files and overlay persistence (created automatically by `sbx deploy` if it does not exist). |
| `--host-cpu`    | `string` | `4`                                                                   | vCPU allocation for each `sandbox-pool` host session clone (for example, `2`, `4`, `8`).                              |
| `--host-memory` | `string` | `8Gi`                                                                 | Memory allocation for each `sandbox-pool` host session clone (for example, `4Gi`, `8Gi`, `16Gi`).                     |
| `--max-density` | `int`    | `10`                                                                  | Maximum concurrent sandboxes packed onto a single `HostSession` clone.                                                |

#### Example

```shell
sbx deploy \
    --project=my-gcp-project \
    --region=us-west1 \
    --bucket=my-gcp-project-sandbox-dispenser \
    --host-cpu=4 \
    --host-memory=8Gi
```

-----

### 2.3 `create` — Create a New Sandbox

Creates a new sandbox directly on an available candidate `HostSession` (or
autonomously spawns a new `host-<random>` session clone if all known candidates
are full).

#### Synopsis

```shell
sbx create [NAME] \
    [--name=<NAME>] \
    [--template=ubuntu|python|node|postgres|<CUSTOM>] \
    [--persistent] \
    [--cpu=<CPU>] \
    [--memory=<MEMORY>] \
    [--allow-egress=true|false | --no-allow-egress] \
    [--from-snapshot=<SNAPSHOT_ID>] \
    [--host=<HOST_ID>] \
    [--pinned] \
    [--idle-timeout=<SECONDS> | --auto-suspend=<SECONDS> | --no-auto-suspend] \
    [--env=<KEY=VAL>] \
    [--metadata=<KEY=VAL>] \
    [--force] \
    [--quiet | -q | --id-only] \
    [--json]
```

#### Flags

| Flag                         | Type      | Default             | Description                                                                                                                                                                                                               |
| :--------------------------- | :-------- | :------------------ | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `--name`                     | `string`  | positional `[NAME]` | Human-readable sandbox name. Host generates a unique `sandbox_id` (`sbx-<host_id>-<8hex>`).                                                                                                                               |
| `--template`                 | `string`  | `ubuntu`            | Template to boot: built-in (`ubuntu`, `python`, `node`, `postgres`) or custom registered template name.                                                                                                                   |
| `--persistent`               | `bool`    | `false`             | Persist writable `rootfs-upper` overlay (`/workspace`, `/persistent`, `/pgdata/data`) to GCS across migrations, suspends, and crashes.                                                                                    |
| `--cpu`                      | `string`  | `1`                 | CPU allocation (positive number like `1`, `2`, `0.5`, or millicores like `500m`).                                                                                                                                         |
| `--memory`                   | `string`  | `1Gi`               | Memory allocation (for example, `512Mi`, `1Gi`, `2Gi`).                                                                                                                                                                   |
| `--allow-egress`             | `bool`    | `true`              | Allow outbound internet egress from the sandbox (enforced via Cloud Run `sandbox do --allow-egress` / Cloud Run Sandbox Runtime `--allow-egress` and local socket/proxy guards).                                                           |
| `--no-allow-egress`          | `bool`    | `false`             | Shorthand for `--allow-egress=false` to block outbound internet access from the sandbox while permitting loopback (`127.0.0.1`/`localhost`).                                                                              |
| `--from-snapshot`            | `string`  | `""`                | Hydrate initial filesystem state from an existing snapshot ID.                                                                                                                                                            |
| `--host`                     | `string`  | `""`                | Pin sandbox creation to a specific `HostSession` ID (`pinned_host=true`).                                                                                                                                                 |
| `--pinned`                   | `bool`    | `false`             | Pin sandbox to its assigned host (`pinned_host=true`) so automatic bin-packing compaction does not migrate it.                                                                                                            |
| `--idle-timeout`             | `string`  | `0` (disabled)      | Auto-suspend sandbox after `<N>` seconds or duration (`300`, `5m`, `300s`, `0s`) of no traffic (`0` disables auto-suspend even if fleet `default_idle_timeout_sec > 0`). Automatically sets `persistent=true` when `> 0`. |
| `--auto-suspend`             | `string`  | `0` (disabled)      | Alias for `--idle-timeout` (accepts integer seconds like `300` or Go duration strings like `5m`, `300s`, `0s`).                                                                                                           |
| `--no-auto-suspend`          | `bool`    | `false`             | Shorthand for `--idle-timeout=0` to explicitly disable idle auto-suspend for this sandbox even when fleet `default_idle_timeout_sec > 0`.                                                                                 |
| `--env`                      | `key=val` | none                | Environment variables injected into the sandbox (repeatable or comma-separated `K1=V1,K2=V2`).                                                                                                                            |
| `--metadata`                 | `key=val` | none                | Arbitrary key-value labels attached to the `SandboxRecord` (repeatable or comma-separated).                                                                                                                               |
| `--force`                    | `bool`    | `false`             | Force creation/overwrite if targeting an existing sandbox ID.                                                                                                                                                             |
| `--quiet`, `-q`, `--id-only` | `bool`    | `false`             | Print only the generated `sandbox_id` (`sbx-<host_id>-<8hex>`) to stdout for easy variable capture (`MY_SANDBOX=$(sbx create ... -q)`).                                                                                   |

#### Examples

**Quiet ID Capture (`--quiet` / `-q` / `--id-only`):**

```shell
MY_SANDBOX=$(sbx create data-worker \
    --template=python \
    --persistent \
    --quiet)
echo "$MY_SANDBOX"
```

```text
sbx-host-742e5bf0-a1b2c3d4
```

**Standard Text Output:**

```shell
sbx create data-worker \
    --template=python \
    --persistent \
    --cpu=2 \
    --memory=2Gi \
    --env=PYTHONUNBUFFERED=1,STAGE=prod \
    --metadata=team=ml,owner=alice
```

```text
Created sandbox sbx-host-742e5bf0-a1b2c3d4 (name=data-worker) on host host-742e5bf0 (template=python, status=RUNNING, epoch=1, persistent=true, idle_timeout=0s)
```

**JSON Output (`--json`):**

```shell
sbx --json create data-worker --template=python --persistent
```

```json
{
  "sandbox_id": "sbx-host-742e5bf0-a1b2c3d4",
  "name": "data-worker",
  "host_id": "host-742e5bf0",
  "host_revision": "sandbox-pool-00012-xyz",
  "engine_type": "cloudrun",
  "epoch": 1,
  "status": "RUNNING",
  "template": "python",
  "persistent": true,
  "cpu": "1",
  "memory": "1Gi",
  "allow_egress": true,
  "overlay_path": "persistence/sbx-host-742e5bf0-a1b2c3d4/overlay.tar",
  "created_at": "2026-10-18T07:30:00Z",
  "updated_at": "2026-10-18T07:30:00Z",
  "last_active_at": "2026-10-18T07:30:00Z"
}
```

-----

### 2.4 `list` (Alias: `ls`) — List Sandboxes Across the Fleet

Queries the horizontally scaled `sandbox-control-plane` in-memory fleet cache
(`GET /v1/sandboxes`) and prints both `SANDBOX_ID` and `NAME`.

#### Synopsis

```shell
sbx list \
    [--name=<NAME>] \
    [--prefix=<PREFIX>] \
    [--status=RUNNING|SUSPENDED|STOPPED|MIGRATING] \
    [--json]
```

#### Flags

| Flag       | Type     | Default | Description                                                                  |
| :--------- | :------- | :------ | :--------------------------------------------------------------------------- |
| `--name`   | `string` | `""`    | Filter sandboxes by exact `name`.                                            |
| `--prefix` | `string` | `""`    | Filter sandboxes whose `sandbox_id` or `name` starts with `<PREFIX>`.        |
| `--status` | `string` | `""`    | Filter by lifecycle status (for example, `RUNNING`, `SUSPENDED`, `STOPPED`). |

#### Example

```shell
sbx list --prefix=data-
```

```text
SANDBOX_ID                   NAME         STATUS    TEMPLATE  HOST_ID         EPOCH  PERSISTENT  MIGRATIONS  RECOVERIES
sbx-host-742e5bf0-a1b2c3d4   data-worker  RUNNING   python    host-742e5bf0   1      true        0           0
sbx-host-91c4e012-f0e1d2c3   data-worker  RUNNING   python    host-91c4e012   1      true        0           0
```

-----

### 2.5 `inspect` (Alias: `describe`) — Inspect Detailed Sandbox State & Metrics

Fetches the full `SandboxRecord` JSON directly from the owning host (`GET
/__session/inspect`), falling back to `GET /v1/sandboxes/<id>` on the control
plane if the sandbox is currently suspended without an active host. Always
outputs indented JSON.

#### Synopsis

```shell
sbx inspect <SANDBOX_ID_OR_NAME>
```

#### Example

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
  "epoch": 1,
  "status": "RUNNING",
  "template": "python",
  "persistent": true,
  "cpu": "2",
  "memory": "2Gi",
  "allow_egress": true,
  "env": {
    "PYTHONUNBUFFERED": "1",
    "STAGE": "prod"
  },
  "metadata": {
    "owner": "alice",
    "team": "ml"
  },
  "overlay_path": "persistence/sbx-host-742e5bf0-a1b2c3d4/overlay.tar",
  "migration_count": 0,
  "crash_recoveries": 0,
  "total_downtime_ms": 0,
  "created_at": "2026-10-18T07:30:00Z",
  "updated_at": "2026-10-18T07:31:10Z",
  "last_active_at": "2026-10-18T07:31:10Z"
}
```

-----

### 2.6 `update` — Update CPU, Memory, Idle Timeout, Egress, Environment, and Metadata in Place

Dynamically updates a sandbox's resource limits, auto-suspend idle timeout,
outbound network egress policy, environment variables, or metadata labels (`POST
/__session/update`).

#### Synopsis

```shell
sbx update <SANDBOX_ID_OR_NAME> \
    [--cpu=<CPU>] \
    [--memory=<MEMORY>] \
    [--idle-timeout=<SECONDS> | --auto-suspend=<SECONDS> | --no-auto-suspend] \
    [--allow-egress=true|false | --no-allow-egress] \
    [--env=<KEY=VAL>] \
    [--metadata=<KEY=VAL>] \
    [--json]
```

#### Flags

| Flag                | Type      | Default   | Description                                                                                                           |
| :------------------ | :-------- | :-------- | :-------------------------------------------------------------------------------------------------------------------- |
| `--cpu`             | `string`  | unchanged | Updated CPU limit (for example, `2` or `500m`).                                                                       |
| `--memory`          | `string`  | unchanged | Updated memory limit (for example, `4Gi`).                                                                            |
| `--idle-timeout`    | `string`  | unchanged | Updated auto-suspend idle timeout in seconds or duration (`120`, `2m`, `120s`, `0s`; `0` disables idle auto-suspend). |
| `--auto-suspend`    | `string`  | unchanged | Alias for `--idle-timeout` (accepts integer seconds like `120` or Go duration strings like `2m`, `120s`, `0s`).       |
| `--no-auto-suspend` | `bool`    | `false`   | Shorthand for `--idle-timeout=0` to disable idle auto-suspend for this sandbox.                                       |
| `--allow-egress`    | `bool`    | unchanged | Enable or disable outbound internet egress (`true` or `false`) in place.                                              |
| `--no-allow-egress` | `bool`    | unchanged | Shorthand for `--allow-egress=false` to disable outbound internet egress in place.                                    |
| `--env`             | `key=val` | unchanged | Environment variables to merge into the sandbox configuration.                                                        |
| `--metadata`        | `key=val` | unchanged | Metadata key-value labels to merge into the sandbox configuration.                                                    |

#### Example

```shell
sbx update "$MY_SANDBOX" \
    --cpu=4 \
    --memory=4Gi \
    --idle-timeout=120 \
    --no-allow-egress \
    --metadata=tier=high-mem
```

```text
Updated sandbox sbx-host-742e5bf0-a1b2c3d4 (cpu=4, memory=4Gi, idle_timeout=120s)
```

-----

### 2.7 `exec` — Execute Commands Inside a Sandbox (Buffered or Real-Time SSE Stream)

Runs a foreground, real-time SSE streaming (`--stream` / `-s`), or background
command directly inside the target sandbox (`POST /__session/exec` or `POST
/__session/exec-stream`).

*   **Real-Time SSE Streaming (`--stream` / `-s`)**: Streams `stdout` and
    `stderr` chunks in real time over Server-Sent Events (`event: start`,
    `event: stdout`, `event: stderr`, `event: exit`) while the command runs, and
    returns the command's exact exit code. Active SSE streams keep the sandbox's
    activity counter incremented so auto-suspend never fires mid-stream.
*   **Automatic Wake-on-Exec**: If the sandbox is `SUSPENDED` or `STOPPED`,
    `hostd` automatically hydrates its saved overlay tarball from GCS, restarts
    PostgreSQL (for `postgres` sandboxes) and any `background_commands`,
    transitions it back to `RUNNING`, and executes the command transparently.
*   **Background Daemons (`--background` & `/workspace/.sbx_init.sh`)**:
    Commands started with `--background` (or `-d`) are recorded in
    `background_commands` on the `SandboxRecord` and are **automatically
    restarted** after live migrations, auto-resume from suspend, or crash
    recoveries. Additionally, if `/workspace/.sbx_init.sh` exists inside the
    sandbox, `hostd` automatically executes it in the background on every
    hydration/resume. (Processes spawned via a foreground `sbx exec` that
    background themselves with `&` without `-d` or `.sbx_init.sh` are not
    tracked in `background_commands`.)
*   **Per-Command Network Egress Override (`--allow-egress` /
    `--no-allow-egress`)**: Override the sandbox's default `allow_egress`
    setting for a single command invocation (enforced via Cloud Run
    `/usr/local/gcp/bin/sandbox do --allow-egress` and socket/proxy guards).

#### Synopsis

```shell
sbx exec <SANDBOX_ID_OR_NAME> \
    [--stream | -s] \
    [--workdir=<DIR>] \
    [--background | -d] \
    [--timeout=<SECONDS>] \
    [--allow-egress=true|false | --no-allow-egress] \
    [--env=<KEY=VAL>] \
    -- <COMMAND> [ARGS...]
```

#### Flags

| Flag                 | Type      | Default         | Description                                                                           |
| :------------------- | :-------- | :-------------- | :------------------------------------------------------------------------------------ |
| `--stream`, `-s`     | `bool`    | `false`         | Stream `stdout` and `stderr` chunks in real time over SSE (`/__session/exec-stream`). |
| `--workdir`          | `string`  | `/workspace`    | Working directory inside the sandbox.                                                 |
| `--background`, `-d` | `bool`    | `false`         | Run the command asynchronously in the background and return its PID immediately.      |
| `--timeout`          | `int`     | `60`            | Command execution timeout in seconds.                                                 |
| `--allow-egress`     | `bool`    | sandbox default | Override outbound internet egress (`true` or `false`) for this command execution.     |
| `--no-allow-egress`  | `bool`    | sandbox default | Block outbound internet egress for this command execution (`--allow-egress=false`).   |
| `--env`              | `key=val` | none            | Per-execution environment variables (repeatable or comma-separated).                  |

#### Examples

**Foreground Command:**

```shell
sbx exec "$MY_SANDBOX" \
    --workdir=/workspace \
    --env=DEBUG=1 \
    -- python3 -c "import os; print('DEBUG =', os.getenv('DEBUG'))"
```

```text
DEBUG = 1
```

**Real-Time SSE Streaming Command (`--stream`):**

```shell
sbx exec "$MY_SANDBOX" --stream -- python3 -u -c "
import time
for i in range(3):
    print(f'tick {i+1}', flush=True)
    time.sleep(0.5)
"
```

**Background Command with JSON Output:**

```shell
sbx --json exec "$MY_SANDBOX" \
    --background \
    -- python3 -m http.server 9000
```

```json
{
  "sandbox_id": "sbx-host-742e5bf0-a1b2c3d4",
  "host_id": "host-742e5bf0",
  "epoch": 1,
  "exit_code": 0,
  "stdout": "1842\n",
  "stderr": "",
  "pid": 1842,
  "duration_ms": 14
}
```

-----

### 2.7b `preview-url` & `port-forward` — HTTP/WebSocket Preview URLs and TCP Port Forwarding

Every sandbox supports live HTTP/WebSocket reverse proxying
(`/proxy/<id_or_name>/<port>/<path>`) and raw TCP port tunneling
(`/__session/port-forward`).

*   **Wake-on-HTTP / Wake-on-Connect**: If a sandbox is `SUSPENDED` (manual or
    auto-suspended after idle timeout), an incoming HTTP request to
    `/proxy/<sandbox_id>/<port>/...` or a `sbx port-forward` connection
    automatically hydrates the sandbox from GCS, restarts PostgreSQL and any
    `background_commands`, waits up to 5 seconds for the listening port to bind,
    and serves the request transparently!

#### Synopsis

```shell
# Print the HTTP/WebSocket preview URL for a port inside a sandbox
sbx preview-url <SANDBOX_ID_OR_NAME> [--port=8080] [--path=/] [--json]

# Forward a local TCP port to a remote port inside a sandbox (e.g. web server or PostgreSQL 5432)
sbx port-forward <SANDBOX_ID_OR_NAME> <LOCAL_PORT>:<REMOTE_PORT>
```

#### Examples

```shell
# Get the preview URL for port 8080 and query it with curl
URL=$(sbx preview-url "$MY_SANDBOX" --port=8080 --path=/healthz)
curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" "$URL"

# Forward local TCP port 15432 to PostgreSQL port 5432 inside a Postgres sandbox
sbx port-forward "$MY_PG" 15432:5432
```

-----

### 2.8 `ssh` — Open an Interactive Shell Session Inside a Sandbox

Opens an interactive line-oriented REPL shell session connected to `/workspace`
inside the sandbox. Type `exit` or `quit` (or press `Ctrl-D`) to disconnect.

#### Synopsis

```shell
sbx ssh <SANDBOX_ID_OR_NAME>
```

#### Example

```shell
sbx ssh "$MY_SANDBOX"
```

```text
Connected to interactive session on sandbox sbx-host-742e5bf0-a1b2c3d4 (type 'exit' to quit)
sbx-host-742e5bf0-a1b2c3d4:/workspace# uname -a
Linux localhost 6.6.x #1 SMP x86_64 GNU/Linux
sbx-host-742e5bf0-a1b2c3d4:/workspace# exit
```

-----

### 2.9 `cp` — Copy Files Between Local Machine and Sandbox

Uploads a local file into a sandbox or downloads a file from a sandbox to the
local machine (`POST /__session/cp`). All remote paths are validated with
`SafeResolvePath` to prevent directory traversal outside the sandbox root.

#### Synopsis

```shell
# Upload local file to sandbox
sbx cp <LOCAL_PATH> <SANDBOX_ID_OR_NAME>:<REMOTE_PATH> [--json]

# Download file from sandbox to local machine
sbx cp <SANDBOX_ID_OR_NAME>:<REMOTE_PATH> <LOCAL_PATH> [--json]
```

#### Examples

```shell
# Upload a script into /workspace/train.py
sbx cp ./train.py "$MY_SANDBOX":/workspace/train.py
# Output: Uploaded 4096 bytes to sbx-host-742e5bf0-a1b2c3d4:/workspace/train.py

# Download results from /workspace/metrics.json
sbx cp "$MY_SANDBOX":/workspace/metrics.json ./metrics.json
# Output: Downloaded 512 bytes from sbx-host-742e5bf0-a1b2c3d4:/workspace/metrics.json to ./metrics.json
```

-----

### 2.10 `logs` — Fetch Stdout, Stderr, and System Lifecycle Logs

Retrieves recent execution output, background process logs, and lifecycle events
(`CREATE`, `MIGRATE`, `SUSPEND`, `RESUME`) for a sandbox (`GET
/__session/logs`).

#### Synopsis

```shell
sbx logs <SANDBOX_ID_OR_NAME> \
    [--tail=<LINES>] \
    [--json]
```

#### Flags

| Flag     | Type  | Default   | Description                              |
| :------- | :---- | :-------- | :--------------------------------------- |
| `--tail` | `int` | `0` (all) | Number of trailing log lines to display. |

#### Example

```shell
sbx logs "$MY_SANDBOX" --tail=50
```

```text
[2026-10-18T07:30:00Z] Sandbox sbx-host-742e5bf0-a1b2c3d4 started on host host-742e5bf0 (template=python, engine=cloudrun)
[2026-10-18T07:30:12Z] EXEC [python3 /workspace/train.py] -> exit=0 (42ms)
Epoch 1: loss=0.142
```

-----

### 2.11 `suspend` / `stop` — Snapshot Overlay to GCS and Release Host Capacity

Quiesces the sandbox (for example, running `CHECKPOINT` and `pg_ctl stop` for
PostgreSQL sandboxes), exports the full writable `rootfs-upper` overlay tarball
to `persistence/<sandbox_id>/overlay.tar` in GCS, stops the container, and
releases the slot on the host session (`POST /__session/suspend` or `POST
/__session/stop`). When the last running sandbox on a host is suspended, the
host marks `/sys/class/instance/state` as `idle` so Cloud Run scales the
instance to zero.

#### Synopsis

```shell
sbx suspend <SANDBOX_ID_OR_NAME> [--json]
sbx stop <SANDBOX_ID_OR_NAME> [--json]
```

#### Example

```shell
sbx suspend "$MY_SANDBOX"
```

```text
Sandbox sbx-host-742e5bf0-a1b2c3d4 is now SUSPENDED (overlay saved to persistence/sbx-host-742e5bf0-a1b2c3d4/overlay.tar)
```

-----

### 2.12 `resume` / `start` — Hydrate a Suspended or Stopped Sandbox Back to `RUNNING`

Hydrates a `SUSPENDED` or `STOPPED` sandbox onto an available warm `HostSession`
(or spawns a new host session if needed), imports its saved overlay tarball
(`persistence/<sandbox_id>/overlay.tar`), increments `epoch`, restarts any
registered background commands or PostgreSQL services, and transitions the
sandbox back to `RUNNING`.

> [!TIP]
> You do not even need to run `sbx resume` manually before `sbx exec`
> or `sbx cp`—any `exec` or `cp` call against a suspended sandbox automatically
> triggers wake-on-demand hydration!

#### Synopsis

```shell
sbx resume <SANDBOX_ID_OR_NAME> [--json]
sbx start <SANDBOX_ID_OR_NAME> [--json]
```

#### Example

```shell
sbx resume "$MY_SANDBOX"
```

```text
Sandbox sbx-host-742e5bf0-a1b2c3d4 resumed on host host-742e5bf0 (status=RUNNING, epoch=2)
```

-----

### 2.13 `migrate` — Live-Migrate a Running Sandbox or Drain an Entire Host

Performs a coordinated live migration (`POST /__session/migrate`) of a single
sandbox to a specific target host (or an automatically selected warm host), or
drains all sandboxes off a specified host session (`--host=<HOST_ID>`).

During live migration:

1.  `hostd` quiesces in-guest databases (such as PostgreSQL) and exports the
    writable overlay to GCS.
2.  The target host imports the overlay via `POST /__internal/hydrate`,
    increments `epoch` in `sandboxes/<sandbox_id>.json` with CAS fencing, and
    restarts background processes.
3.  The source host removes its local container and redirects any stale clients
    via `HTTP 409 Conflict` (`X-Sandbox-Host-Session: <new_host_id>`).

#### Synopsis

```shell
# Migrate a single sandbox (auto-selecting target host or pinning to --to-host)
sbx migrate <SANDBOX_ID_OR_NAME> \
    [--to-host=<TARGET_HOST_ID>] \
    [--target-host=<TARGET_HOST_ID>] \
    [--json]

# Drain all sandboxes from an entire host session
sbx migrate --host=<SOURCE_HOST_ID> [--json]
```

#### Flags

| Flag            | Type     | Default       | Description                                                                         |
| :-------------- | :------- | :------------ | :---------------------------------------------------------------------------------- |
| `--to-host`     | `string` | auto-selected | Destination `HostSession` ID to migrate the sandbox onto (sets `pinned_host=true`). |
| `--target-host` | `string` | auto-selected | Alias for `--to-host`.                                                              |
| `--host`        | `string` | `""`          | Evacuate and drain all sandboxes currently running on `<SOURCE_HOST_ID>`.           |

#### Examples

```shell
# Migrate a single sandbox to a specific host session
sbx migrate "$MY_SANDBOX" --to-host=host-91c4e012
# Output: Migrated sandbox sbx-host-742e5bf0-a1b2c3d4 from host-742e5bf0 -> host-91c4e012 (epoch=2)

# Drain all sandboxes from host-742e5bf0 onto other fleet hosts
sbx migrate --host=host-742e5bf0
# Output: Drained host host-742e5bf0: [sbx-host-742e5bf0-a1b2c3d4->host-91c4e012]
```

-----

### 2.14 `copy` (Alias: `fork`) — Clone a Running Sandbox via CoW Fork or Cross-Host Snapshot

Clones a source sandbox's entire writable filesystem, environment variables, and
template settings into a new sandbox (`POST /__session/copy`) with
`name=<TARGET_NAME>` and a newly generated globally unique `sandbox_id`
(`sbx-<host_id>-<8hex>`):

*   **Same-Host Copy-on-Write (`cloudrun-cow-fork` / `local-cow-copy`)**: When
    `--target-host` is omitted (or matches the source host) and the source host
    has a free slot, `hostd` forks the container overlay directly on the same
    host for sub-second cloning.
*   **Cross-Host Clone (`cross-host-tar-clone`)**: When `--target-host`
    specifies a different host (or the source host is at `max_density`), `hostd`
    exports a snapshot tarball and hydrates the new clone onto the target host
    session.

#### Synopsis

```shell
sbx copy <SOURCE_SANDBOX_ID_OR_NAME> <TARGET_NAME> \
    [--target-host=<HOST_ID>] \
    [--to-host=<HOST_ID>] \
    [--persistent] \
    [--force] \
    [--quiet | -q | --id-only] \
    [--json]
```

#### Flags

| Flag                         | Type     | Default                        | Description                                                                                                                     |
| :--------------------------- | :------- | :----------------------------- | :------------------------------------------------------------------------------------------------------------------------------ |
| `--target-host`              | `string` | same host (if capacity allows) | Target `HostSession` ID for cross-host cloning.                                                                                 |
| `--to-host`                  | `string` | `""`                           | Alias for `--target-host`.                                                                                                      |
| `--persistent`               | `bool`   | inherits source                | Mark the cloned sandbox as persistent.                                                                                          |
| `--force`                    | `bool`   | `false`                        | Overwrite target if a conflicting record exists.                                                                                |
| `--quiet`, `-q`, `--id-only` | `bool`   | `false`                        | Print only the newly cloned `sandbox_id` (`sbx-<host_id>-<8hex>`) to stdout (`MY_CLONE=$(sbx copy "$MY_SANDBOX" my-clone -q)`). |

#### Examples

```shell
# Standard output (mints a new unique sbx-<host_id>-<8hex> ID with name=my-dev-box-clone)
sbx copy "$MY_SANDBOX" my-dev-box-clone --persistent
# Output: Copied sandbox sbx-host-742e5bf0-a1b2c3d4 -> sbx-host-742e5bf0-e5f60718 (name=my-dev-box-clone) on host host-742e5bf0 (method=local_overlay_fork)

# Capture cloned sandbox_id directly with --quiet (-q)
MY_CLONE=$(sbx copy "$MY_SANDBOX" branch-experiment-2 --persistent --quiet)
echo "Cloned sandbox: $MY_CLONE"
# Output: Cloned sandbox: sbx-host-742e5bf0-e1f2a3b4
```

-----

### 2.15 `snapshot` (`create`, `list`, `delete`) — Manage Point-in-Time Filesystem Snapshots

Captures, lists, or deletes immutable point-in-time filesystem snapshots
(`snapshots/<snapshot_id>.json` and `snapshots/<snapshot_id>.tar` in GCS). Saved
snapshots can be used to seed new sandboxes (`sbx create
--from-snapshot=<SNAPSHOT_ID>`) or registered as reusable templates (`sbx
template create --from-snapshot=<SNAPSHOT_ID>`).

#### Synopsis

```shell
# Create a point-in-time snapshot from a sandbox
sbx snapshot create <SANDBOX_ID_OR_NAME> \
    [--name=<SNAPSHOT_ID>] \
    [--force] \
    [--json]

# List all snapshots in the fleet (default action if subcommand omitted)
sbx snapshot list [--json]

# Delete a snapshot
sbx snapshot delete <SNAPSHOT_ID>
```

#### Flags (`snapshot create`)

| Flag      | Type     | Default         | Description                                            |
| :-------- | :------- | :-------------- | :----------------------------------------------------- |
| `--name`  | `string` | `snap-<random>` | Custom identifier for the snapshot.                    |
| `--force` | `bool`   | `false`         | Overwrite an existing snapshot with the same `--name`. |

#### Examples

```shell
# Capture a named snapshot
sbx snapshot create "$MY_SANDBOX" --name=snap-py-deps-v1 --force
# Output: Created snapshot snap-py-deps-v1 from sbx-host-742e5bf0-a1b2c3d4 (184320 bytes)

# List available snapshots
sbx snapshot list
```

```text
SNAPSHOT_ID      SOURCE_SANDBOX              TEMPLATE  SIZE_BYTES  CREATED_AT
snap-py-deps-v1  sbx-host-742e5bf0-a1b2c3d4  python    184320      2026-10-18T07:35:00Z
```

```shell
# Boot a new sandbox from the snapshot
MY_RESTORED=$(sbx create worker-from-snap \
    --template=python \
    --from-snapshot=snap-py-deps-v1 \
    --quiet)

# Delete a snapshot when no longer needed
sbx snapshot delete snap-py-deps-v1
```

-----

### 2.16 `template` (`list`, `create` / `add`, `delete`) — Manage Built-in & Custom Templates

Lists built-in pre-warmed templates (`ubuntu`, `python`, `node`, `postgres`) and
custom snapshot-backed templates, registers new custom templates from either an
existing snapshot (`--from-snapshot`) or a live sandbox (`--from-sandbox`), or
deletes custom templates.

> [!IMPORTANT]
> Built-in templates (`ubuntu`, `python`, `node`, `postgres`)
> are protected and cannot be overwritten or deleted.

#### Synopsis

```shell
# List all built-in and custom templates
sbx template list [--json]

# Register a new custom template (alias: `sbx template add`)
sbx template create <TEMPLATE_NAME> \
    [--from-sandbox=<SANDBOX_ID_OR_NAME> | --from-snapshot=<SNAPSHOT_ID>] \
    [--base-template=ubuntu|python|node|postgres] \
    [--image=<CONTAINER_IMAGE>] \
    [--description=<TEXT>] \
    [--force] \
    [--json]

# Delete a custom template
sbx template delete <TEMPLATE_NAME>
```

#### Flags (`template create` / `template add`)

| Flag              | Type     | Default                                 | Description                                                                                       |
| :---------------- | :------- | :-------------------------------------- | :------------------------------------------------------------------------------------------------ |
| `--from-sandbox`  | `string` | `""`                                    | Automatically snapshot `<SANDBOX_ID_OR_NAME>` as `snap-tpl-<NAME>` and back the template with it. |
| `--from-snapshot` | `string` | `""`                                    | Back the custom template with an existing `<SNAPSHOT_ID>`.                                        |
| `--base-template` | `string` | `ubuntu` (or source sandbox's template) | Underlying base runtime template (`ubuntu`, `python`, `node`, `postgres`).                        |
| `--image`         | `string` | `ubuntu:24.04`                          | Container image reference recorded in `TemplateSpec`.                                             |
| `--description`   | `string` | `""`                                    | Human-readable description of the template.                                                       |
| `--force`         | `bool`   | `false`                                 | Overwrite an existing custom template with the same `<TEMPLATE_NAME>`.                            |

#### Examples

```shell
# Register a custom template directly from a configured sandbox
sbx template create ml-pandas \
    --from-sandbox="$MY_SANDBOX" \
    --description="Python 3.12 with pandas, numpy, and scikit-learn pre-installed" \
    --force
# Output: Registered template ml-pandas (type=snapshot, snapshot=snap-tpl-ml-pandas)

# List all templates
sbx template list
```

```text
NAME       TYPE       IMAGE                             SNAPSHOT_ID         DESCRIPTION
ml-pandas  snapshot   ubuntu:24.04                      snap-tpl-ml-pandas  Python 3.12 with pandas, numpy, and scikit-learn pre-installed
node       prewarmed  node:22-slim                                          Node.js 22 LTS runtime sandbox for JS/TS agents and toolchains
postgres   prewarmed  mirror.gcr.io/library/postgres:16                     PostgreSQL 16 stateful database sandbox with libfakeuid.so & PGDATA=/pgdata/data overlay persistence
python     prewarmed  python:3.12-slim                                      Python 3.12 runtime sandbox pre-warmed for AI code execution
ubuntu     prewarmed  ubuntu:24.04                                          Ubuntu 24.04 LTS general-purpose sandbox with curl, git, python3, tar
```

-----

### 2.17 `delete` (Alias: `rm`) — Delete One or More Sandboxes

Stops and deletes one or more sandboxes (`POST /__session/delete`), removing
their ownership records (`sandboxes/<sandbox_id>.json`), persisted overlay
tarballs, and freeing their host capacity slots.

#### Synopsis

```shell
sbx delete <SANDBOX_ID_OR_NAME> [SANDBOX_ID_2...] \
    [--force] \
    [--json]
```

#### Example

```shell
sbx delete "$MY_SANDBOX" "$MY_CLONE"
```

```text
Deleted sandbox sbx-host-742e5bf0-a1b2c3d4
Deleted sandbox sbx-host-742e5bf0-c9d8e7f6
```

-----

### 2.18 `hosts` (`list`, `drain`, `compact`, `simulate-crash`) — Manage Host Sessions & Bin-Packing

Inspects active `sandbox-pool` session clones (`GET /v1/hosts`), drains a
specific host session, triggers immediate fleet-wide bin-packing defragmentation
(`POST /v1/compact`), or simulates an ungraceful `SIGKILL` host crash (`POST
/v1/hosts/<id>/simulate-crash`) to test watchdog resurrection.

#### Synopsis

```shell
# List all host sessions in the fleet
sbx hosts list [--json]

# Gracefully evacuate all sandboxes from a host session
sbx hosts drain <HOST_ID> [--json]

# Trigger immediate bin-packing defragmentation across sparse hosts
sbx hosts compact [--json]

# Simulate a sudden SIGKILL crash on a host and trigger watchdog resurrection
sbx hosts simulate-crash <HOST_ID> [--json]
```

#### Examples

```shell
# List active host sessions
sbx hosts list
```

```text
HOST_ID        STATUS  ACTIVE  FREE  MAX  CPU/RAM  REVISION                INSTANCE_STATE  LAST_HEARTBEAT
host-742e5bf0  ACTIVE  8       2     10   4/8Gi    sandbox-pool-00012-xyz  busy            07:40:15
host-91c4e012  ACTIVE  2       8     10   4/8Gi    sandbox-pool-00012-xyz  busy            07:40:14
```

```shell
# Compact sparse hosts onto minimal session clones
sbx hosts compact
# Output: Compaction result: drained=[host-91c4e012] migrated=[sbx-host-91c4e012-11223344->host-742e5bf0 sbx-host-91c4e012-55667788->host-742e5bf0] (compacted 1 sparse host(s))
```

-----

### 2.19 `config` (`get`, `set`) — View and Tune Fleet-Wide Dispenser Settings

Reads (`GET /v1/config`) or dynamically updates (`POST /v1/config`) fleet-wide
dispenser configuration stored at `fleet/config.json`. Updating configuration
automatically refreshes the local CLI candidate cache.

> [!NOTE]
> When `--host-cpu` or `--host-memory` is changed to a new size
> against a live Cloud Run deployment, `sandbox-control-plane` automatically
> reconciles `sandbox-pool` on Cloud Run to roll out a new revision with the
> updated vCPU/RAM allocation (existing hosts gracefully drain and live-migrate
> their sandboxes onto the new revision). Passing unchanged values is a fast
> metadata no-op.

#### Synopsis

```shell
# View current fleet configuration (always outputs indented JSON)
sbx config get

# Update fleet configuration parameters
sbx config set \
    [--max-density=<N>] \
    [--host-cpu=<VCPU>] \
    [--host-memory=<MEMORY>] \
    [--min-free-memory-mb=<MB>] \
    [--min-free-memory-pct=<PCT>] \
    [--max-memory-overcommit-ratio=<RATIO>] \
    [--hot-memory-threshold-pct=<PCT>] \
    [--hot-cpu-threshold-pct=<PCT>] \
    [--min-client-version=<SEMVER>] \
    [--project=<PROJECT_ID>] \
    [--region=<REGION>] \
    [--periodic-sync-sec=<SECONDS>] \
    [--default-idle-timeout=<SECONDS> | --auto-suspend=<SECONDS>] \
    [--heartbeat-timeout-sec=<SECONDS>] \
    [--auto-bin-pack=true|false] \
    [--target-revision=<REVISION_NAME>] \
    [--pool-url=<URL>]
```

#### Flags (`config set`)

| Flag                            | Type     | Default                 | Description                                                                                                                           |
| :------------------------------ | :------- | :---------------------- | :------------------------------------------------------------------------------------------------------------------------------------ |
| `--max-density`                 | `int`    | `10`                    | Maximum concurrent sandboxes packed onto a single `HostSession` clone.                                                                |
| `--host-cpu`                    | `string` | `4`                     | vCPU allocation for each `sandbox-pool` host session clone (for example, `4`, `8`).                                                   |
| `--host-memory`                 | `string` | `8Gi`                   | Memory allocation for each `sandbox-pool` host session clone (for example, `8Gi`, `16Gi`).                                            |
| `--min-free-memory-mb`          | `int`    | `512`                   | Minimum free host RAM in MiB before rejecting new creations with `HTTP 429` (`host_memory_pressure`).                                 |
| `--min-free-memory-pct`         | `float`  | `15.0`                  | Minimum free host RAM percentage (`0–100`) before rejecting new creations with `HTTP 429` (`host_memory_pressure`).                   |
| `--max-memory-overcommit-ratio` | `float`  | `2.0`                   | Maximum ratio of allocated sandbox memory to physical `HOST_MEMORY` per host (`active >= 1`).                                         |
| `--hot-memory-threshold-pct`    | `float`  | `85.0`                  | Host actual memory usage `%` that triggers proactive live migration (`HOT_SHEDDING`) before OOM.                                      |
| `--hot-cpu-threshold-pct`       | `float`  | `90.0`                  | Host actual CPU usage `%` that triggers proactive live migration (`HOT_SHEDDING`).                                                    |
| `--min-client-version`          | `string` | `1.0.0`                 | Minimum required `sbx` client/SDK semver (for example, `1.0.0`, `1.2.0`); older clients receive `HTTP 426 Upgrade Required`.          |
| `--project`                     | `string` | active `gcloud` project | GCP project ID used when updating `sandbox-pool` Cloud Run host CPU/memory.                                                           |
| `--region`                      | `string` | `us-west1`              | Cloud Run region used when updating `sandbox-pool` host CPU/memory.                                                                   |
| `--periodic-sync-sec`           | `int`    | `10`                    | Interval in seconds between background overlay tarball syncs for `--persistent` sandboxes.                                            |
| `--default-idle-timeout`        | `int`    | `0` (disabled)          | Fleet-wide default auto-suspend idle timeout in seconds (`0` disables fleet-wide auto-suspend by default).                            |
| `--auto-suspend`                | `int`    | `0` (disabled)          | Alias for `--default-idle-timeout`.                                                                                                   |
| `--heartbeat-timeout-sec`       | `int`    | `15`                    | Seconds without a heartbeat before the watchdog declares a host `DEAD` and resurrects its sandboxes.                                  |
| `--auto-bin-pack`               | `bool`   | `true`                  | Enable or disable continuous background bin-packing defragmentation by the control-plane leader.                                      |
| `--target-revision`             | `string` | `""`                    | Target Cloud Run revision name; hosts on older revisions automatically drain and live-migrate their sandboxes to the target revision. |
| `--pool-url`                    | `string` | auto-configured         | Override the `sandbox-pool` service URL advertised to CLIs via `/v1/candidates`.                                                      |

#### Examples

**Inspect Current Configuration (`sbx config get`):**

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

**Update Host Size, Overload Protection Thresholds & Fleet Density (`sbx config
set`):**

```shell
sbx config set \
    --max-density=15 \
    --host-cpu=8 \
    --host-memory=16Gi \
    --min-free-memory-mb=512 \
    --min-free-memory-pct=15 \
    --max-memory-overcommit-ratio=2.0 \
    --hot-memory-threshold-pct=85 \
    --hot-cpu-threshold-pct=90 \
    --min-client-version=1.2.0 \
    --periodic-sync-sec=10 \
    --heartbeat-timeout-sec=15 \
    --auto-bin-pack=true
```

```json
{
  "max_density": 15,
  "host_cpu": "8",
  "host_memory": "16Gi",
  "min_free_memory_mb": 512,
  "min_free_memory_pct": 15,
  "max_memory_overcommit_ratio": 2,
  "hot_memory_threshold_pct": 85,
  "hot_cpu_threshold_pct": 90,
  "min_client_version": "1.2.0",
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
  "updated_at": "2026-10-18T07:45:00Z"
}
```

-----

### 2.20 `stats` — View Real-Time Fleet Observability, Bin-Packing Efficiency & Uptime SLA

Displays fleet-wide capacity utilization, active/draining/dead host counts,
running/suspended sandbox counts, bin-packing efficiency percentage, cumulative
live migrations (including hot-host shedding and memory admission rejections),
overcommit ratios, crash recoveries, and the fleet uptime SLA percentage (`GET
/v1/stats`).

#### Synopsis

```shell
sbx stats [--json]
```

#### Example

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

-----

### 2.21 `ui` — Web Console UI (`sandbox-ui` / `sbx ui`)

The dispenser includes an interactive **Web Console UI** (`sandbox-ui`,
implemented in `ui.go` and `service-ui.yaml`) that serves as a
Backend-for-Frontend (BFF) so the browser never handles raw IAM tokens directly.

*   **Hosted Cloud Run Service (`sandbox-ui`)**: Deployed as its own Cloud Run
    service at `https://sandbox-ui-qd4pckfcha-uw.a.run.app`, designed for
    browser authentication via Identity-Aware Proxy (IAP) while communicating
    with `sandbox-control-plane` and `sandbox-pool` using Cloud Run IAM.
*   **Local Workstation Server (`sbx ui`)**: Runs the Web Console locally on
    `http://127.0.0.1:8080`, automatically reusing the endpoints
    (`endpoints.json`), route cache (`routes.json`), and `gcloud` OIDC token
    cache (`token_cache.json`) in `~/.cache/sbx/`.

#### Synopsis

```shell
# Launch the Web Console locally on http://127.0.0.1:8080
sbx ui

# Or specify a custom port / bind address via environment variables
PORT=9090 BIND_HOST=127.0.0.1 sbx ui
```

#### Web Console Features

1.  **Fleet Overview & SLA Observability**: Real-time dashboard showing
    active/draining/dead host clones, running/suspended sandboxes, capacity slot
    utilization, bin-packing efficiency percentage, live migrations, crash
    recoveries, and the fleet uptime SLA.
2.  **Sandboxes Manager & Live Command Runner (`exec` & `logs`)**: Create new
    sandboxes (choosing template, CPU/RAM, idle timeout, and persistence),
    inspect full JSON records, dynamically resize resources in place,
    suspend/resume, live-migrate, clone (`copy`), snapshot, delete, run
    interactive commands (`exec`), and stream sandbox logs directly from the
    browser.
3.  **Host Clones & Bin-Packing Visualizer**: Visualizes each `4 vCPU / 8Gi`
    host clone's occupied vs. free capacity slots with one-click buttons to
    gracefully drain a host (`drain`), trigger fleet-wide bin-packing compaction
    (`compact`), or test crash recovery (`simulate-crash`).
4.  **Dispenser Configuration & Templates Manager**: View and edit fleet-wide
    settings (`max_density`, `host_cpu`, `host_memory`, `periodic_sync_sec`,
    `heartbeat_timeout_sec`, `auto_bin_pack`, `target_revision`) and register or
    delete custom snapshot-backed templates.

-----

### 2.22 `version` (Aliases: `--version`, `-v`) — Check Client & Server Protocol Versions

Prints the local `sbx` client version (`1.2.0`) and, when a control plane URL is
configured in `~/.cache/sbx/endpoints.json`, queries `GET /v1/config` to display
the running server version and the fleet's minimum required client version
(`min_client_version`).

#### Synopsis

```shell
sbx version [--json]
```

#### Examples

```shell
sbx version
# Output:
# sbx client v1.2.0 (server v1.2.0, min_client_version=1.0.0)

sbx --json version
```

```json
{
  "client_type": "sbx-cli/go",
  "client_version": "1.2.0",
  "min_client_version": "1.0.0",
  "server_version": "1.2.0"
}
```

-----

### 2.23 `metrics` — View Real-Time & Historical CPU, Memory & Overcommit Observability

Queries `GET /v1/metrics` on `sandbox-control-plane` to display fleet-wide CPU
and memory overcommit ratios, per-host memory/CPU utilization and overload
protection states (`NORMAL`, `ELEVATED`, `ADMISSION_BLOCKED`, `HOT_SHEDDING`),
per-sandbox RSS/peak memory and CPU usage, rolling historical resource samples
(`--history`), and automated sizing/overcommit tuning recommendations. For a
detailed guide to overcommit and overload protection, see
**[OVERLOAD_PROTECTION.md](OVERLOAD_PROTECTION.md)**.

#### Synopsis

```shell
sbx metrics \
    [--host=<HOST_ID>] \
    [--sandbox=<SANDBOX_ID_OR_NAME>] \
    [--history] \
    [--json]
```

#### Flags

| Flag        | Type     | Default              | Description                                                                                              |
| :---------- | :------- | :------------------- | :------------------------------------------------------------------------------------------------------- |
| `--host`    | `string` | `""` (all hosts)     | Filter resource metrics and optional time-series history to a single `HostSession` ID.                   |
| `--sandbox` | `string` | `""` (all sandboxes) | Filter resource metrics to a specific sandbox ID or name.                                                |
| `--history` | `bool`   | `false`              | Include rolling point-in-time `HostResourceSample` time-series history (`metrics/hosts/<host_id>.json`). |

#### Examples

```shell
# View fleet-wide overcommit ratios, per-host overload states, and sizing recommendations
sbx metrics
```

```text
Fleet Resource & Overcommit Metrics
  Fleet Memory Overcommit Ratio: 1.5
  Fleet CPU Overcommit Ratio:    1.25
  Hot-Host Migrations Triggered: 1
  Memory Admission Rejections:   2

Host Resource Breakdown:
  - host-742e5bf0: state=ELEVATED used_mem_pct=68.4% mem_overcommit=1.5x cpu_overcommit=1.25x cpu_usage=24.5%
  - host-91c4e012: state=NORMAL used_mem_pct=21.2% mem_overcommit=0.5x cpu_overcommit=0.5x cpu_usage=8.0%

Sizing & Overcommit Recommendations:
  - Host host-742e5bf0 is overcommitted on memory (1.50x allocated vs physical 8Gi) with 68.4% actual usage; monitor peak_memory_pct or increase --host-memory=16Gi if workloads spike concurrently.
```

```shell
# Fetch full JSON metrics and time-series history for a specific host
sbx --json metrics --host=host-742e5bf0 --history
```

-----

## 3\. End-to-End Cookbook Examples

### Example 1: Ephemeral Python Data Science Sandbox & File Transfer

Provision an isolated Python 3.12 sandbox with 2 vCPU and 2Gi RAM, upload a
dataset and script, dynamically resize the sandbox to 4 vCPU and 4Gi RAM before
heavy aggregation, download the resulting artifact, and clean up:

```shell
# 1. Create the sandbox (2 vCPU / 2Gi RAM) and capture its unique sandbox_id with --quiet
MY_SANDBOX=$(sbx create py-etl \
    --template=python \
    --cpu=2 \
    --memory=2Gi \
    --quiet)
echo "Provisioned sandbox: $MY_SANDBOX"

# 2. Upload local input data into /workspace/sales.csv
cat << 'EOF' > /tmp/sales.csv
region,revenue
na,1200.50
eu,980.25
apac,1540.75
EOF
sbx cp /tmp/sales.csv "$MY_SANDBOX":/workspace/sales.csv

# 3. Dynamically scale up the sandbox to 4 vCPU / 4Gi RAM in place before running analysis
sbx update "$MY_SANDBOX" --cpu=4 --memory=4Gi

# 4. Run an inline Python aggregation script inside the sandbox
sbx exec "$MY_SANDBOX" -- python3 -c "
import csv, json
with open('/workspace/sales.csv') as f:
    total = sum(float(r['revenue']) for r in csv.DictReader(f))
with open('/workspace/summary.json', 'w') as out:
    json.dump({'total_revenue': total}, out)
print('Computed total_revenue:', total)
"

# 5. Download the output artifact back to the workstation and delete the sandbox
sbx cp "$MY_SANDBOX":/workspace/summary.json /tmp/summary.json
cat /tmp/summary.json
sbx delete "$MY_SANDBOX"
```

-----

### Example 2: Custom Environment Template & Fast Copy-on-Write Cloning

Install custom packages once in a base sandbox, promote it to a reusable
template, and fan out multiple Copy-on-Write clones in milliseconds:

```shell
# 1. Create a base sandbox (2 vCPU / 2Gi RAM) and install project dependencies
MY_SANDBOX=$(sbx create base-env \
    --template=python \
    --cpu=2 \
    --memory=2Gi \
    --quiet)
sbx exec "$MY_SANDBOX" -- sh -c "mkdir -p /workspace/lib && echo 'VERSION = \"2.1.0\"' > /workspace/lib/config.py"

# 2. Register a reusable custom template directly from the live sandbox
sbx template create custom-agent-env \
    --from-sandbox="$MY_SANDBOX" \
    --description="Pre-configured agent workspace with /workspace/lib/config.py" \
    --force

# 3. Instantaneously fork 3 Copy-on-Write clones from the running base sandbox
for i in 1 2 3; do
  CLONE_ID=$(sbx copy "$MY_SANDBOX" "agent-branch-${i}" --quiet)
  sbx exec "$CLONE_ID" -- python3 -c "import sys; sys.path.append('/workspace/lib'); import config; print('Clone ${i} loaded version:', config.VERSION)"
done

# 4. Or boot brand-new sandboxes anywhere in the fleet using `--template=custom-agent-env`
NEW_SANDBOX=$(sbx create fresh-agent \
    --template=custom-agent-env \
    --cpu=1 \
    --memory=1Gi \
    --quiet)
sbx exec "$NEW_SANDBOX" -- cat /workspace/lib/config.py
```

-----

### Example 3: Persistent PostgreSQL 16 Sandbox Surviving Live Migration & Host Crash (`RPO=0`)

The built-in `postgres` template initializes a PostgreSQL 16 cluster at
`PGDATA=/pgdata/data` using `libfakeuid.so` (`LD_PRELOAD` mapping `UID 0 ->
999`). Because `PGDATA` resides inside the persisted overlay, all tables and
committed transactions survive both graceful live migrations and unexpected host
crashes:

```shell
# 1. Create a persistent PostgreSQL 16 sandbox (2 vCPU / 2Gi RAM) and capture its sandbox_id
MY_SANDBOX=$(sbx create pg-orders \
    --template=postgres \
    --cpu=2 \
    --memory=2Gi \
    --persistent \
    --quiet)
echo "Postgres sandbox: $MY_SANDBOX"

# 2. Create a table and insert committed rows
sbx exec "$MY_SANDBOX" -- psql -U postgres -d postgres -c "
  CREATE TABLE IF NOT EXISTS orders (id SERIAL PRIMARY KEY, item TEXT, amount INT);
  INSERT INTO orders (item, amount) VALUES ('gpu-node', 4), ('tpu-slice', 8);
"

# 3. Live-migrate the running PostgreSQL sandbox to a new host session
sbx migrate "$MY_SANDBOX"
sbx inspect "$MY_SANDBOX"

# 4. Verify all rows are intact on the new host session (RPO=0)
sbx exec "$MY_SANDBOX" -- psql -U postgres -d postgres -tAc "SELECT item, amount FROM orders ORDER BY id;"
# Output:
# gpu-node|4
# tpu-slice|8

# 5. Simulate an unexpected SIGKILL crash on the current host session
CUR_HOST=$(sbx inspect "$MY_SANDBOX" | python3 -c "import sys, json; print(json.load(sys.stdin)['host_id'])")
sbx hosts simulate-crash "$CUR_HOST"

# 6. Verify automatic watchdog resurrection and database integrity
sbx exec "$MY_SANDBOX" -- psql -U postgres -d postgres -tAc "SELECT COUNT(*) FROM orders;"
# Output: 2
```

-----

### Example 4: Cost Optimization with Idle Auto-Suspend, In-Guest `IDLE` Signaling & Bin-Packing

You can minimize Cloud Run compute costs using three complementary mechanisms:

1.  **Per-Sandbox Idle Timeout (`--idle-timeout` / `--auto-suspend`)**:
    Automatically snapshots and suspends a sandbox after `N` seconds (or Go
    duration string like `5m`) without `exec`, `exec-stream`, `cp`, `proxy`, or
    `port-forward` activity.
2.  **In-Guest Cooperative Self-Suspend (`/var/run/instance/state`)**: A process
    inside the sandbox can cooperatively write `IDLE` or `SUSPEND` (to suspend)
    or `STOP` (to stop) to `/var/run/instance/state` (or
    `$SBX_GUEST_STATE_FILE`) as soon as a batch job finishes to trigger
    immediate suspend/stop without waiting for a timeout. Background servers
    started with `sbx exec -d` (`--background`) or placed in
    `/workspace/.sbx_init.sh` are automatically restarted when the sandbox
    resumes.
3.  **Fleet Bin-Packing (`sbx hosts compact`)**: Consolidates sandboxes from
    under-utilized hosts onto fewer warm clones so drained clones transition
    `/sys/class/instance/state` to `idle` and scale to zero.

<!-- end list -->

```shell
# 1. Create a persistent sandbox with a 60-second idle auto-suspend timeout
MY_SANDBOX=$(sbx create batch-worker \
    --template=ubuntu \
    --persistent \
    --idle-timeout=60 \
    --quiet)

# 2. Run a job that writes artifacts to /persistent and cooperatively signals IDLE when finished
sbx exec "$MY_SANDBOX" -- sh -c '
  echo "checkpoint-complete" > /persistent/status.txt
  mkdir -p /var/run/instance
  echo "IDLE" > /var/run/instance/state
'

# 3. Once suspended, any subsequent `sbx exec` automatically wakes and resumes the sandbox
sbx exec "$MY_SANDBOX" -- cat /persistent/status.txt
# Output: checkpoint-complete

# 4. Trigger fleet-wide bin-packing compaction to consolidate sparse hosts
sbx hosts compact
sbx stats
```

-----

### Example 5: High-Throughput Parallel Batch Creation from Scripting

Because `sbx create` routes directly to candidate hosts and generates
collision-free `sbx-<host_id>-<8hex>` IDs even when sandboxes share the same
`--name`, you can launch dozens of parallel sandboxes from a shell script or CI
pipeline with zero lock contention:

```shell
#!/usr/bin/env bash
set -euo pipefail

IDS_FILE="$(mktemp)"
trap 'rm -f "${IDS_FILE}"' EXIT

# Launch 20 sandboxes concurrently sharing the logical name "ci-shard"
for shard in $(seq 1 20); do
  (
    MY_SANDBOX=$(sbx create ci-shard \
        --template=python \
        --metadata="shard=${shard}" \
        --quiet)
    echo "$MY_SANDBOX" >> "${IDS_FILE}"
    sbx exec "$MY_SANDBOX" -- python3 -c "print('Completed shard ${shard} on', '$MY_SANDBOX')"
  ) &
done
wait

echo "All 20 parallel shards completed. Cleaning up..."
# Delete all created sandboxes in parallel
xargs -P 10 -n 1 sbx delete < "${IDS_FILE}"
```
