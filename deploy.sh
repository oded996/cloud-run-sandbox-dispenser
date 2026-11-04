#!/usr/bin/env bash
# Deploy sandbox-pool and sandbox-control-plane to Cloud Run.
set -euo pipefail

PROJECT="${1:-${GOOGLE_CLOUD_PROJECT:-${DEVSHELL_PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || true)}}}"
if [ -z "${PROJECT}" ] || [ "${PROJECT}" = "(unset)" ]; then
  echo "Error: GCP project ID required (pass as arg 1 or set GOOGLE_CLOUD_PROJECT)" >&2
  exit 1
fi
REGION="${2:-us-west1}"
BUCKET="${3:-${PROJECT}-sandbox-dispenser}"
HOST_CPU="${HOST_CPU:-4}"
HOST_MEMORY="${HOST_MEMORY:-8Gi}"
DEPLOYER_EMAIL="${DEPLOYER_EMAIL:-$(gcloud config get-value account 2>/dev/null || echo admin@example.com)}"
if [ -z "${DEPLOYER_EMAIL}" ]; then
  DEPLOYER_EMAIL="admin@example.com"
fi
AR_REPO="sandbox-dispenser"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT}/${AR_REPO}/dispenser:latest"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

echo "[0/5] Enabling required Google Cloud APIs (Run, Cloud Build, Artifact Registry, Storage, IAP)..."
gcloud services enable \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  storage.googleapis.com \
  iap.googleapis.com \
  --project="${PROJECT}" --quiet >/dev/null 2>&1 || true

echo "[1/5] Ensuring GCS bucket gs://${BUCKET} exists in ${PROJECT} (${REGION})..."
if ! gcloud storage buckets describe "gs://${BUCKET}" --project="${PROJECT}" >/dev/null 2>&1; then
  gcloud storage buckets create "gs://${BUCKET}" --project="${PROJECT}" --location="${REGION}" --uniform-bucket-level-access
fi

echo "[2/5] Ensuring Artifact Registry repository ${AR_REPO} exists..."
if ! gcloud artifacts repositories describe "${AR_REPO}" --project="${PROJECT}" --location="${REGION}" >/dev/null 2>&1; then
  gcloud artifacts repositories create "${AR_REPO}" --project="${PROJECT}" --location="${REGION}" --repository-format=docker
fi

echo "[3/5] Building and pushing container image ${IMAGE} via Cloud Build..."
STAGE_DIR="$(mktemp -d)"
trap 'rm -rf "${STAGE_DIR}"' EXIT
if [ "${SKIP_BUILD:-0}" != "1" ]; then
  cp go.mod dispenser.go runtime.go hostd.go controld.go cli.go ui.go main.go Dockerfile deploy.sh service-control-plane.yaml service-pool.yaml service-ui.yaml "${STAGE_DIR}/"
  gcloud builds submit "${STAGE_DIR}" --project="${PROJECT}" --tag="${IMAGE}" --gcs-log-dir="gs://${BUCKET}/cloudbuild-logs" --quiet || true
fi

DIGEST_IMAGE="$(gcloud artifacts docker images describe "${IMAGE}" --project="${PROJECT}" --format='value(image_summary.fully_qualified_digest)')"
if [ -z "${DIGEST_IMAGE}" ]; then
  DIGEST_IMAGE="${IMAGE}"
fi
echo "Built image: ${DIGEST_IMAGE}"

echo "[4/5] Deploying sandbox-control-plane and sandbox-pool (deployer: ${DEPLOYER_EMAIL})..."
gcloud services enable iap.googleapis.com --project="${PROJECT}" --quiet >/dev/null 2>&1 || true
gcloud beta services identity create --service=iap.googleapis.com --project="${PROJECT}" --quiet >/dev/null 2>&1 || true

CP_YAML="${STAGE_DIR}/service-control-plane.rendered.yaml"
POOL_YAML="${STAGE_DIR}/service-pool.rendered.yaml"

sed \
  -e "s|IMAGE_URI_PLACEHOLDER|${DIGEST_IMAGE}|g" \
  -e "s|PROJECT_ID_PLACEHOLDER|${PROJECT}|g" \
  -e "s|BUCKET_NAME_PLACEHOLDER|${BUCKET}|g" \
  -e "s|POOL_SERVICE_URL_PLACEHOLDER||g" \
  -e "s|CONTROL_PLANE_URL_PLACEHOLDER||g" \
  -e "s|DEPLOYER_EMAIL_PLACEHOLDER|${DEPLOYER_EMAIL}|g" \
  service-control-plane.yaml > "${CP_YAML}"

gcloud beta run services replace "${CP_YAML}" --project="${PROJECT}" --region="${REGION}" --quiet
CP_URL="$(gcloud run services describe sandbox-control-plane --project="${PROJECT}" --region="${REGION}" --format='value(status.url)')"
echo "sandbox-control-plane URL: ${CP_URL}"

sed \
  -e "s|IMAGE_URI_PLACEHOLDER|${DIGEST_IMAGE}|g" \
  -e "s|PROJECT_ID_PLACEHOLDER|${PROJECT}|g" \
  -e "s|BUCKET_NAME_PLACEHOLDER|${BUCKET}|g" \
  -e "s|CONTROL_PLANE_URL_PLACEHOLDER|${CP_URL}|g" \
  -e "s|POOL_SERVICE_URL_PLACEHOLDER||g" \
  -e "s|HOST_CPU_PLACEHOLDER|${HOST_CPU}|g" \
  -e "s|HOST_MEMORY_PLACEHOLDER|${HOST_MEMORY}|g" \
  service-pool.yaml > "${POOL_YAML}"

gcloud alpha run services replace "${POOL_YAML}" --project="${PROJECT}" --region="${REGION}" --quiet
POOL_URL="$(gcloud run services describe sandbox-pool --project="${PROJECT}" --region="${REGION}" --format='value(status.url)')"
echo "sandbox-pool URL: ${POOL_URL}"

echo "[5/5] Wiring mutual service URLs, Cloud IAP on sandbox-ui, and IAM invoker roles..."
sed \
  -e "s|IMAGE_URI_PLACEHOLDER|${DIGEST_IMAGE}|g" \
  -e "s|PROJECT_ID_PLACEHOLDER|${PROJECT}|g" \
  -e "s|BUCKET_NAME_PLACEHOLDER|${BUCKET}|g" \
  -e "s|POOL_SERVICE_URL_PLACEHOLDER|${POOL_URL}|g" \
  -e "s|CONTROL_PLANE_URL_PLACEHOLDER|${CP_URL}|g" \
  -e "s|DEPLOYER_EMAIL_PLACEHOLDER|${DEPLOYER_EMAIL}|g" \
  service-control-plane.yaml > "${CP_YAML}"
gcloud beta run services replace "${CP_YAML}" --project="${PROJECT}" --region="${REGION}" --quiet

sed \
  -e "s|IMAGE_URI_PLACEHOLDER|${DIGEST_IMAGE}|g" \
  -e "s|PROJECT_ID_PLACEHOLDER|${PROJECT}|g" \
  -e "s|BUCKET_NAME_PLACEHOLDER|${BUCKET}|g" \
  -e "s|CONTROL_PLANE_URL_PLACEHOLDER|${CP_URL}|g" \
  -e "s|POOL_SERVICE_URL_PLACEHOLDER|${POOL_URL}|g" \
  -e "s|HOST_CPU_PLACEHOLDER|${HOST_CPU}|g" \
  -e "s|HOST_MEMORY_PLACEHOLDER|${HOST_MEMORY}|g" \
  service-pool.yaml > "${POOL_YAML}"
gcloud alpha run services replace "${POOL_YAML}" --project="${PROJECT}" --region="${REGION}" --quiet

UI_YAML="${STAGE_DIR}/service-ui.rendered.yaml"
sed \
  -e "s|IMAGE_URI_PLACEHOLDER|${DIGEST_IMAGE}|g" \
  -e "s|PROJECT_ID_PLACEHOLDER|${PROJECT}|g" \
  -e "s|BUCKET_NAME_PLACEHOLDER|${BUCKET}|g" \
  -e "s|POOL_SERVICE_URL_PLACEHOLDER|${POOL_URL}|g" \
  -e "s|CONTROL_PLANE_URL_PLACEHOLDER|${CP_URL}|g" \
  -e "s|DEPLOYER_EMAIL_PLACEHOLDER|${DEPLOYER_EMAIL}|g" \
  service-ui.yaml > "${UI_YAML}"
gcloud beta run services replace "${UI_YAML}" --project="${PROJECT}" --region="${REGION}" --quiet
gcloud beta run services update sandbox-ui --project="${PROJECT}" --region="${REGION}" --iap --quiet >/dev/null 2>&1 || true
UI_URL="$(gcloud run services describe sandbox-ui --project="${PROJECT}" --region="${REGION}" --format='value(status.url)')"
echo "sandbox-ui URL (IAP Enabled): ${UI_URL}"

PROJECT_NUMBER="$(gcloud projects describe "${PROJECT}" --format='value(projectNumber)')"
if [ -n "${PROJECT_NUMBER}" ]; then
  COMPUTE_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
  IAP_SA="service-${PROJECT_NUMBER}@gcp-sa-iap.iam.gserviceaccount.com"
  gcloud run services add-iam-policy-binding sandbox-control-plane --project="${PROJECT}" --region="${REGION}" --member="serviceAccount:${COMPUTE_SA}" --role="roles/run.invoker" --quiet >/dev/null
  gcloud run services add-iam-policy-binding sandbox-pool --project="${PROJECT}" --region="${REGION}" --member="serviceAccount:${COMPUTE_SA}" --role="roles/run.invoker" --quiet >/dev/null
  gcloud run services add-iam-policy-binding sandbox-ui --project="${PROJECT}" --region="${REGION}" --member="serviceAccount:${COMPUTE_SA}" --role="roles/run.invoker" --quiet >/dev/null
  gcloud run services add-iam-policy-binding sandbox-ui --project="${PROJECT}" --region="${REGION}" --member="serviceAccount:${IAP_SA}" --role="roles/run.invoker" --quiet >/dev/null 2>&1 || true
fi

if [ -n "${DEPLOYER_EMAIL}" ]; then
  gcloud run services add-iam-policy-binding sandbox-control-plane --project="${PROJECT}" --region="${REGION}" --member="user:${DEPLOYER_EMAIL}" --role="roles/run.invoker" --quiet >/dev/null 2>&1 || true
  gcloud run services add-iam-policy-binding sandbox-pool --project="${PROJECT}" --region="${REGION}" --member="user:${DEPLOYER_EMAIL}" --role="roles/run.invoker" --quiet >/dev/null 2>&1 || true
  gcloud run services add-iam-policy-binding sandbox-ui --project="${PROJECT}" --region="${REGION}" --member="user:${DEPLOYER_EMAIL}" --role="roles/run.invoker" --quiet >/dev/null 2>&1 || true
  gcloud iap web add-iam-policy-binding --resource-type=cloud-run --service=sandbox-ui --region="${REGION}" --project="${PROJECT}" --member="user:${DEPLOYER_EMAIL}" --role="roles/iap.httpsResourceAccessor" --quiet >/dev/null 2>&1 || \
    gcloud beta iap web add-iam-policy-binding --resource-type=cloud-run --service=sandbox-ui --region="${REGION}" --project="${PROJECT}" --member="user:${DEPLOYER_EMAIL}" --role="roles/iap.httpsResourceAccessor" --quiet >/dev/null 2>&1 || true
fi

ID_TOKEN="$(gcloud auth print-identity-token --audiences="${CP_URL}" 2>/dev/null || gcloud auth print-identity-token 2>/dev/null || true)"
if [ -n "${ID_TOKEN}" ]; then
  curl -fsSL -X POST "${CP_URL}/v1/deploy" \
    -H "Authorization: Bearer ${ID_TOKEN}" \
    -H "Content-Type: application/json" \
    -H "X-Sbx-Client-Version: 1.2.0" \
    -H "X-Sbx-User-Email: ${DEPLOYER_EMAIL}" \
    -d "{\"project\":\"${PROJECT}\",\"region\":\"${REGION}\",\"bucket\":\"${BUCKET}\",\"image\":\"${DIGEST_IMAGE}\",\"host_cpu\":\"${HOST_CPU}\",\"host_memory\":\"${HOST_MEMORY}\"}" >/dev/null || true
  curl -fsSL -X POST "${CP_URL}/v1/security/policy" \
    -H "Authorization: Bearer ${ID_TOKEN}" \
    -H "Content-Type: application/json" \
    -H "X-Sbx-Client-Version: 1.2.0" \
    -H "X-Sbx-User-Email: ${DEPLOYER_EMAIL}" \
    -d "{\"action\":\"add_ui_member\",\"member\":\"user:${DEPLOYER_EMAIL}\"}" >/dev/null || true
  curl -fsSL -X POST "${CP_URL}/v1/security/policy" \
    -H "Authorization: Bearer ${ID_TOKEN}" \
    -H "Content-Type: application/json" \
    -H "X-Sbx-Client-Version: 1.2.0" \
    -H "X-Sbx-User-Email: ${DEPLOYER_EMAIL}" \
    -d "{\"action\":\"add_backend_member\",\"member\":\"user:${DEPLOYER_EMAIL}\"}" >/dev/null || true
  if [ -n "${COMPUTE_SA:-}" ]; then
    curl -fsSL -X POST "${CP_URL}/v1/security/policy" \
      -H "Authorization: Bearer ${ID_TOKEN}" \
      -H "Content-Type: application/json" \
      -H "X-Sbx-Client-Version: 1.2.0" \
      -H "X-Sbx-User-Email: ${DEPLOYER_EMAIL}" \
      -d "{\"action\":\"add_backend_member\",\"member\":\"serviceAccount:${COMPUTE_SA}\"}" >/dev/null || true
  fi
fi

echo "================================================================="
echo "Deployment complete!"
echo "  CONTROL_PLANE_URL=${CP_URL}"
echo "  POOL_SERVICE_URL=${POOL_URL}"
echo "  UI_SERVICE_URL=${UI_URL}"
echo "================================================================="
