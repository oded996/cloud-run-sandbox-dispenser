package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// UIServer implements the Cloud Run Sandbox Dispenser Web Console (`sandbox-ui`).
// It acts as a Backend-for-Frontend (BFF) so the browser never handles IAM bearer tokens
// directly, and enforces strict Content-Security-Policy (per-request cryptographic nonces),
// CSRF double-submit cookies on all state-changing requests, method allow-listing, and
// rate limiting.
// TODO(security): Integrate Cloud Identity-Aware Proxy (IAP) or OAuth 2.0 OIDC user session
// validation for multi-tenant RBAC when deployed outside IAM-protected Cloud Run ingress.
type UIServer struct {
	client          *CLIClient
	controlPlaneURL string
	poolServiceURL  string
	projectID       string
	region          string
	csrfCookieName  string

	rateMu     sync.Mutex
	rateWindow time.Time
	rateCount  int
}

// NewUIServer creates a new Web Console BFF server backed by `controlPlaneURL` and `poolServiceURL`.
func NewUIServer(controlPlaneURL, poolServiceURL, cacheDir string) *UIServer {
	if cacheDir == "" {
		cacheDir = filepath.Join(os.TempDir(), "sbx-ui-cache")
	}
	cpURL := strings.TrimRight(strings.TrimSpace(controlPlaneURL), "/")
	poolURL := strings.TrimRight(strings.TrimSpace(poolServiceURL), "/")
	cli := NewCLIClient(cpURL, poolURL, cacheDir)
	proj := DefaultGCPProject()
	reg := strings.TrimSpace(os.Getenv("GOOGLE_CLOUD_REGION"))
	if reg == "" {
		reg = "us-west1"
	}
	return &UIServer{
		client:          cli,
		controlPlaneURL: cpURL,
		poolServiceURL:  poolURL,
		projectID:       proj,
		region:          reg,
		csrfCookieName:  "sbx_csrf",
		rateWindow:      time.Now(),
	}
}

func generateSecureHex(numBytes int) string {
	b := make([]byte, numBytes)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

func isHTTPSRequest(r *http.Request) bool {
	if r.TLS != nil {
		return true
	}
	return strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")
}

func (u *UIServer) allowRequestRate() bool {
	u.rateMu.Lock()
	defer u.rateMu.Unlock()
	now := time.Now()
	if now.Sub(u.rateWindow) >= time.Second {
		u.rateWindow = now
		u.rateCount = 0
	}
	u.rateCount++
	return u.rateCount <= 250
}

func setSecurityHeaders(w http.ResponseWriter, nonce string) {
	if nonce != "" {
		csp := fmt.Sprintf(
			"default-src 'self'; script-src 'self' 'nonce-%s'; style-src 'self' 'nonce-%s' https://fonts.googleapis.com; style-src-elem 'self' 'nonce-%s' https://fonts.googleapis.com; style-src-attr 'unsafe-inline'; font-src 'self' https://fonts.gstatic.com; img-src 'self' data:; connect-src 'self'; object-src 'none'; frame-ancestors 'none'; base-uri 'self'",
			nonce, nonce, nonce,
		)
		w.Header().Set("Content-Security-Policy", csp)
	} else {
		w.Header().Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
	}
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
	w.Header().Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
	w.Header().Set("Cache-Control", "no-store")
}

func (u *UIServer) ensureCSRFCookie(w http.ResponseWriter, r *http.Request) string {
	secure := isHTTPSRequest(r)
	cookieName := u.csrfCookieName
	if secure {
		cookieName = "__Host-" + u.csrfCookieName
	}
	if c, err := r.Cookie(cookieName); err == nil && len(c.Value) >= 32 {
		return c.Value
	}
	if c, err := r.Cookie(u.csrfCookieName); err == nil && len(c.Value) >= 32 {
		return c.Value
	}
	token := generateSecureHex(32)
	http.SetCookie(w, &http.Cookie{
		Name:     cookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		Secure:   secure,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   86400,
	})
	return token
}

func (u *UIServer) verifyCSRF(r *http.Request) bool {
	hdr := strings.TrimSpace(r.Header.Get("X-CSRF-Token"))
	if len(hdr) < 32 {
		return false
	}
	for _, name := range []string{"__Host-" + u.csrfCookieName, u.csrfCookieName} {
		if c, err := r.Cookie(name); err == nil && len(c.Value) >= 32 {
			if subtle.ConstantTimeCompare([]byte(hdr), []byte(c.Value)) == 1 {
				return true
			}
		}
	}
	return false
}

// ServeHTTP routes Web Console requests with strict method allow-listing and CSRF verification.
func (u *UIServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !u.allowRequestRate() {
		setSecurityHeaders(w, "")
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "rate limit exceeded"})
		return
	}

	if strings.HasPrefix(r.URL.Path, "/proxy/") || r.URL.Path == "/__session/port-forward" {
		u.handleUIProxy(w, r)
		return
	}

	// Strict HTTP method allow-list: only GET, POST, DELETE are permitted for UI/API routes.
	if r.Method != http.MethodGet && r.Method != http.MethodPost && r.Method != http.MethodDelete {
		setSecurityHeaders(w, "")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	if r.URL.Path == "/healthz" {
		setSecurityHeaders(w, "")
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "mode": "ui"})
		return
	}

	if r.URL.Path == "/" || r.URL.Path == "/index.html" {
		if r.Method != http.MethodGet {
			setSecurityHeaders(w, "")
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		nonce := generateSecureHex(16)
		csrfToken := u.ensureCSRFCookie(w, r)
		setSecurityHeaders(w, nonce)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, renderConsoleHTML(nonce, csrfToken, u.projectID, u.region, u.controlPlaneURL, u.poolServiceURL))
		return
	}

	// All /api/... endpoints return JSON with strict security headers.
	setSecurityHeaders(w, "")
	if r.Method == http.MethodPost || r.Method == http.MethodDelete {
		if !u.verifyCSRF(r) {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "CSRF token validation failed"})
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	}

	switch r.URL.Path {
	case "/api/overview":
		u.handleAPIOverview(w, r)
	case "/api/metrics":
		u.handleAPIMetrics(w, r)
	case "/api/security/policy":
		u.handleAPISecurityPolicy(w, r)
	case "/api/sandboxes/create":
		u.handleAPISandboxCreate(w, r)
	case "/api/sandboxes/exec":
		u.handleAPISandboxExec(w, r)
	case "/api/sandboxes/exec-stream":
		u.handleAPISandboxExecStream(w, r)
	case "/api/sandboxes/logs":
		u.handleAPISandboxLogs(w, r)
	case "/api/sandboxes/update":
		u.handleAPISandboxUpdate(w, r)
	case "/api/sandboxes/lifecycle":
		u.handleAPISandboxLifecycle(w, r)
	case "/api/hosts/action":
		u.handleAPIHostAction(w, r)
	case "/api/config":
		u.handleAPIConfig(w, r)
	case "/api/templates":
		u.handleAPITemplates(w, r)
	case "/api/snapshots":
		u.handleAPISnapshots(w, r)
	default:
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown endpoint"})
	}
}

func (u *UIServer) forwardCallerHeaders(r *http.Request) map[string]string {
	hdrs := make(map[string]string)
	if v := strings.TrimSpace(r.Header.Get("X-Goog-Authenticated-User-Email")); v != "" {
		hdrs["X-Goog-Authenticated-User-Email"] = v
	}
	if v := strings.TrimSpace(r.Header.Get("X-Sbx-User-Email")); v != "" {
		hdrs["X-Sbx-User-Email"] = v
	}
	if len(hdrs) == 0 {
		caller := ExtractCallerEmail(r, "")
		if caller != "" {
			hdrs["X-Sbx-User-Email"] = caller
		}
	}
	return hdrs
}

func (u *UIServer) ensurePoolURL(ctx context.Context) {
	if u.client.PoolServiceURL == "" && u.client.ControlPlaneURL != "" {
		_, _ = u.client.RefreshCandidates(ctx)
		u.poolServiceURL = u.client.PoolServiceURL
	}
}

func (u *UIServer) handleAPIOverview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "GET required"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	u.ensurePoolURL(ctx)
	callerHdrs := u.forwardCallerHeaders(r)

	type overviewResponse struct {
		ProjectID       string           `json:"project_id"`
		Region          string           `json:"region"`
		ControlPlaneURL string           `json:"control_plane_url"`
		PoolServiceURL  string           `json:"pool_service_url"`
		Stats           FleetSummary     `json:"stats"`
		Config          FleetConfig      `json:"config"`
		Metrics         map[string]any   `json:"metrics"`
		Security        SecurityPolicy   `json:"security"`
		Sandboxes       []SandboxRecord  `json:"sandboxes"`
		Hosts           []HostRecord     `json:"hosts"`
		Events          []ActivityEvent  `json:"events"`
		Templates       []TemplateSpec   `json:"templates"`
		Snapshots       []SnapshotRecord `json:"snapshots"`
		FetchedAt       time.Time        `json:"fetched_at"`
	}

	out := overviewResponse{
		ProjectID:       u.projectID,
		Region:          u.region,
		ControlPlaneURL: u.client.ControlPlaneURL,
		PoolServiceURL:  u.client.PoolServiceURL,
		Metrics:         map[string]any{},
		Sandboxes:       []SandboxRecord{},
		Hosts:           []HostRecord{},
		Events:          []ActivityEvent{},
		Templates:       []TemplateSpec{},
		Snapshots:       []SnapshotRecord{},
		FetchedAt:       time.Now().UTC(),
	}

	var wg sync.WaitGroup
	var mu sync.Mutex

	wg.Add(8)
	go func() {
		defer wg.Done()
		var st FleetSummary
		if _, err := u.client.doJSON(ctx, http.MethodGet, u.client.ControlPlaneURL+"/v1/stats", callerHdrs, nil, &st); err == nil {
			mu.Lock()
			out.Stats = st
			out.Config = st.Config
			if out.Config.HostCPU == "" {
				out.Config.HostCPU = "4"
			}
			if out.Config.HostMemory == "" {
				out.Config.HostMemory = "8Gi"
			}
			if out.PoolServiceURL == "" && st.Config.PoolServiceURL != "" {
				out.PoolServiceURL = st.Config.PoolServiceURL
			}
			mu.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		var m map[string]any
		if _, err := u.client.doJSON(ctx, http.MethodGet, u.client.ControlPlaneURL+"/v1/metrics?history=true", callerHdrs, nil, &m); err == nil && m != nil {
			mu.Lock()
			out.Metrics = m
			mu.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		var sec SecurityPolicy
		if _, err := u.client.doJSON(ctx, http.MethodGet, u.client.ControlPlaneURL+"/v1/security/policy", callerHdrs, nil, &sec); err == nil {
			mu.Lock()
			out.Security = sec
			mu.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		var res struct {
			Sandboxes []SandboxRecord `json:"sandboxes"`
		}
		if _, err := u.client.doJSON(ctx, http.MethodGet, u.client.ControlPlaneURL+"/v1/sandboxes", callerHdrs, nil, &res); err == nil && res.Sandboxes != nil {
			mu.Lock()
			out.Sandboxes = res.Sandboxes
			mu.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		var res struct {
			Hosts []HostRecord `json:"hosts"`
		}
		if _, err := u.client.doJSON(ctx, http.MethodGet, u.client.ControlPlaneURL+"/v1/hosts", callerHdrs, nil, &res); err == nil && res.Hosts != nil {
			mu.Lock()
			out.Hosts = res.Hosts
			mu.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		var res struct {
			Events []ActivityEvent `json:"events"`
		}
		if _, err := u.client.doJSON(ctx, http.MethodGet, u.client.ControlPlaneURL+"/v1/events", callerHdrs, nil, &res); err == nil && res.Events != nil {
			mu.Lock()
			out.Events = res.Events
			mu.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		var res struct {
			Templates []TemplateSpec `json:"templates"`
		}
		if _, err := u.client.doJSON(ctx, http.MethodGet, u.client.ControlPlaneURL+"/v1/templates", callerHdrs, nil, &res); err == nil && res.Templates != nil {
			mu.Lock()
			out.Templates = res.Templates
			mu.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		var res struct {
			Snapshots []SnapshotRecord `json:"snapshots"`
		}
		if _, err := u.client.doJSON(ctx, http.MethodGet, u.client.ControlPlaneURL+"/v1/snapshots", callerHdrs, nil, &res); err == nil && res.Snapshots != nil {
			mu.Lock()
			out.Snapshots = res.Snapshots
			mu.Unlock()
		}
	}()
	wg.Wait()

	writeJSON(w, http.StatusOK, out)
}

func (u *UIServer) handleAPIMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "GET required"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	var m map[string]any
	if _, err := u.client.doJSON(ctx, http.MethodGet, u.client.ControlPlaneURL+"/v1/metrics?history=true", u.forwardCallerHeaders(r), nil, &m); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, m)
}

func (u *UIServer) handleAPISecurityPolicy(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
	defer cancel()
	callerHdrs := u.forwardCallerHeaders(r)

	if r.Method == http.MethodGet {
		var pol SecurityPolicy
		if _, err := u.client.doJSON(ctx, http.MethodGet, u.client.ControlPlaneURL+"/v1/security/policy", callerHdrs, nil, &pol); err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, pol)
		return
	}
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "GET or POST required"})
		return
	}
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	var updated SecurityPolicy
	status, err := u.client.doJSON(ctx, http.MethodPost, u.client.ControlPlaneURL+"/v1/security/policy", callerHdrs, body, &updated)
	if err != nil {
		if strings.Contains(err.Error(), "self_revocation_forbidden") {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error":   "self_revocation_forbidden",
				"message": err.Error(),
			})
			return
		}
		if status >= 400 && status < 500 {
			writeJSON(w, status, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, updated)
}

func (u *UIServer) handleAPISandboxCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	var req CreateSandboxRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	if req.Name != "" {
		if err := ValidateID(req.Name); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
	}
	if req.TargetHostID != "" {
		if err := ValidateID(req.TargetHostID); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
	}
	if err := ValidateCPUMemory(req.CPU, req.Memory); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
	defer cancel()
	u.ensurePoolURL(ctx)

	rec, err := u.client.CreateSandbox(ctx, req)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

func (u *UIServer) handleAPISandboxExec(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	var req ExecRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	if err := ValidateID(req.SandboxID); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if len(req.Command) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "command is required"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 65*time.Second)
	defer cancel()
	u.ensurePoolURL(ctx)

	sbxID, err := u.client.ResolveUniqueSandboxID(ctx, req.SandboxID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	req.SandboxID = sbxID
	resp, err := u.client.Exec(ctx, req)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (u *UIServer) handleAPISandboxExecStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	var req ExecRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	if err := ValidateID(req.SandboxID); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if len(req.Command) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "command is required"})
		return
	}
	streamTimeout := 3600 * time.Second
	if req.TimeoutSec > 0 {
		streamTimeout = time.Duration(req.TimeoutSec)*time.Second + 10*time.Second
	}
	ctx, cancel := context.WithTimeout(r.Context(), streamTimeout)
	defer cancel()
	u.ensurePoolURL(ctx)

	sbxID, err := u.client.ResolveUniqueSandboxID(ctx, req.SandboxID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	req.SandboxID = sbxID
	req.Stream = true

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)

	_, streamErr := u.client.ExecStream(ctx, req, func(evt ExecStreamEvent) {
		b, mErr := json.Marshal(evt)
		if mErr != nil {
			return
		}
		_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", evt.Type, string(b))
		if flusher != nil {
			flusher.Flush()
		}
	})
	if streamErr != nil {
		b, _ := json.Marshal(ExecStreamEvent{Type: "error", Error: streamErr.Error(), ExitCode: -1})
		_, _ = fmt.Fprintf(w, "event: error\ndata: %s\n\n", string(b))
		if flusher != nil {
			flusher.Flush()
		}
	}
}

func (u *UIServer) handleUIProxy(w http.ResponseWriter, r *http.Request) {
	if u.client.ControlPlaneURL == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "control_plane_url is not configured"})
		return
	}
	cpBase := strings.TrimRight(u.client.ControlPlaneURL, "/")
	parsedCP, parseErr := url.Parse(cpBase)
	if parseErr != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": parseErr.Error()})
		return
	}

	if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") || strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade") || r.Header.Get("Upgrade") != "" {
		proxy := &httputil.ReverseProxy{
			Rewrite: func(pr *httputil.ProxyRequest) {
				pr.SetURL(parsedCP)
				pr.Out.URL.Path = r.URL.Path
				pr.Out.URL.RawPath = ""
				pr.Out.URL.RawQuery = r.URL.RawQuery
				pr.Out.Host = parsedCP.Host
				// Never forward the browser's IAP / X-Serverless-* credentials: GFE on
				// sandbox-control-plane would evaluate them before our ID token and return 401.
				sanitizeProxyHeadersInPlace(pr.Out.Header, true)
				if strings.HasPrefix(cpBase, "https://") {
					if tok, tokErr := GetIDToken(r.Context(), cpBase); tokErr == nil && tok != "" {
						pr.Out.Header.Set("Authorization", "Bearer "+tok)
					}
				}
			},
			FlushInterval: -1,
		}
		proxy.ServeHTTP(w, r)
		return
	}

	targetURL := cpBase + r.URL.RequestURI()
	var bodyBytes []byte
	if r.Body != nil {
		const maxProxyBody = 16 << 20
		bodyBytes, _ = io.ReadAll(io.LimitReader(r.Body, maxProxyBody+1))
		if len(bodyBytes) > maxProxyBody {
			http.Error(w, "proxy request payload exceeds 16 MiB limit; use direct pool URL for larger uploads", http.StatusRequestEntityTooLarge)
			return
		}
	}
	outReq, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL, bytes.NewReader(bodyBytes))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	// Strip the browser's IAP / X-Serverless-* credentials and IAP/UI cookies (see
	// sanitizeProxyForwardHeaders), then attach this service's own ID token for the next hop.
	sanitizeProxyForwardHeaders(outReq.Header, r.Header, true)
	if strings.HasPrefix(targetURL, "https://") {
		if tok, tokErr := GetIDToken(r.Context(), u.client.ControlPlaneURL); tokErr == nil && tok != "" {
			outReq.Header.Set("Authorization", "Bearer "+tok)
		}
	}
	// The UI client must surface upstream redirects to the browser instead of following them.
	httpClient := *u.client.HTTPClient
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := httpClient.Do(outReq)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	defer resp.Body.Close()
	for k, vals := range resp.Header {
		for _, v := range vals {
			w.Header().Add(k, v)
		}
	}
	if idOrName, port, _, pErr := parseProxyPathAndPort(r); pErr == nil && strings.HasPrefix(r.URL.Path, "/proxy/") {
		rewriteProxyLocation(w.Header(), idOrName, port)
	}
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
}

func (u *UIServer) handleAPISandboxLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "GET required"})
		return
	}
	rawID := strings.TrimSpace(r.URL.Query().Get("sandbox_id"))
	if err := ValidateID(rawID); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	tailN, _ := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("tail")))
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	u.ensurePoolURL(ctx)

	sbxID, err := u.client.ResolveUniqueSandboxID(ctx, rawID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	ep := "/__session/logs?sandbox_id=" + url.QueryEscape(sbxID)
	if tailN > 0 {
		ep += "&tail=" + strconv.Itoa(tailN)
	}
	var res struct {
		SandboxID string `json:"sandbox_id"`
		HostID    string `json:"host_id"`
		Logs      string `json:"logs"`
	}
	if err := u.client.CallSandboxEndpoint(ctx, http.MethodGet, ep, sbxID, nil, &res); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	if tailN > 0 {
		res.Logs = tailLogLines(res.Logs, tailN)
	}
	writeJSON(w, http.StatusOK, res)
}

func (u *UIServer) handleAPISandboxUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	var req struct {
		SandboxID      string            `json:"sandbox_id"`
		CPU            string            `json:"cpu,omitempty"`
		Memory         string            `json:"memory,omitempty"`
		IdleTimeoutSec *int              `json:"idle_timeout_sec,omitempty"`
		AllowEgress    *bool             `json:"allow_egress,omitempty"`
		Env            map[string]string `json:"env,omitempty"`
		Metadata       map[string]string `json:"metadata,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	if err := ValidateID(req.SandboxID); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := ValidateCPUMemory(req.CPU, req.Memory); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
	defer cancel()
	u.ensurePoolURL(ctx)

	sbxID, err := u.client.ResolveUniqueSandboxID(ctx, req.SandboxID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	req.SandboxID = sbxID
	var rec SandboxRecord
	if err := u.client.CallSandboxEndpoint(ctx, http.MethodPost, "/__session/update", sbxID, req, &rec); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

func (u *UIServer) handleAPISandboxLifecycle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	var body struct {
		Action       string `json:"action"`
		SandboxID    string `json:"sandbox_id"`
		TargetHostID string `json:"target_host_id,omitempty"`
		TargetName   string `json:"target_name,omitempty"`
		SnapshotID   string `json:"snapshot_id,omitempty"`
		Persistent   bool   `json:"persistent,omitempty"`
		Force        bool   `json:"force,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	if err := ValidateID(body.SandboxID); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
	defer cancel()
	u.ensurePoolURL(ctx)

	sbxID, err := u.client.ResolveUniqueSandboxID(ctx, body.SandboxID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	action := strings.ToLower(strings.TrimSpace(body.Action))
	switch action {
	case "suspend", "stop", "resume", "start":
		var rec SandboxRecord
		if err := u.client.CallSandboxEndpoint(ctx, http.MethodPost, "/__session/"+action, sbxID, map[string]string{"sandbox_id": sbxID}, &rec); err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, rec)
	case "migrate":
		if body.TargetHostID != "" {
			if err := ValidateID(body.TargetHostID); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
		}
		var rec SandboxRecord
		if err := u.client.CallSandboxEndpoint(ctx, http.MethodPost, "/__session/migrate", sbxID, MigrateSandboxRequest{
			SandboxID:    sbxID,
			TargetHostID: body.TargetHostID,
			PinnedHost:   body.TargetHostID != "",
		}, &rec); err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
			return
		}
		u.client.SetRouteAndName(rec.SandboxID, rec.Name, rec.HostID)
		writeJSON(w, http.StatusOK, rec)
	case "copy":
		if body.TargetName != "" {
			if err := ValidateID(body.TargetName); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
		}
		if body.TargetHostID != "" {
			if err := ValidateID(body.TargetHostID); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
		}
		var rec SandboxRecord
		if err := u.client.CallSandboxEndpoint(ctx, http.MethodPost, "/__session/copy", sbxID, CopySandboxRequest{
			SourceSandboxID: sbxID,
			TargetSandboxID: body.TargetName,
			Name:            body.TargetName,
			TargetHostID:    body.TargetHostID,
			Persistent:      body.Persistent,
			Force:           body.Force,
		}, &rec); err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
			return
		}
		u.client.SetRouteAndName(rec.SandboxID, rec.Name, rec.HostID)
		writeJSON(w, http.StatusOK, rec)
	case "snapshot":
		if body.SnapshotID != "" {
			if err := ValidateID(body.SnapshotID); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
		}
		var snap SnapshotRecord
		if err := u.client.CallSandboxEndpoint(ctx, http.MethodPost, "/__session/snapshot", sbxID, map[string]any{
			"sandbox_id":  sbxID,
			"snapshot_id": body.SnapshotID,
			"force":       body.Force,
		}, &snap); err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, snap)
	case "delete":
		var res map[string]any
		err := u.client.CallSandboxEndpoint(ctx, http.MethodPost, "/__session/delete", sbxID, map[string]string{"sandbox_id": sbxID}, &res)
		if u.client.ControlPlaneURL != "" {
			_, _ = u.client.doJSON(ctx, http.MethodDelete, u.client.ControlPlaneURL+"/v1/sandboxes/"+url.PathEscape(sbxID), nil, nil, &res)
		}
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
			return
		}
		u.client.SetRoute(sbxID, "")
		writeJSON(w, http.StatusOK, map[string]any{"deleted": sbxID})
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unsupported lifecycle action"})
	}
}

func (u *UIServer) handleAPIHostAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	var body struct {
		Action string `json:"action"`
		HostID string `json:"host_id,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
	defer cancel()

	action := strings.ToLower(strings.TrimSpace(body.Action))
	switch action {
	case "compact":
		var res map[string]any
		if _, err := u.client.doJSON(ctx, http.MethodPost, u.client.ControlPlaneURL+"/v1/compact", nil, nil, &res); err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, res)
	case "drain", "simulate-crash":
		if err := ValidateID(body.HostID); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		var res map[string]any
		if _, err := u.client.doJSON(ctx, http.MethodPost, u.client.ControlPlaneURL+"/v1/hosts/"+url.PathEscape(body.HostID)+"/"+action, nil, nil, &res); err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, res)
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unsupported host action"})
	}
}

func (u *UIServer) handleAPIConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST required"})
		return
	}
	var patch map[string]any
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	hostCPU, _ := patch["host_cpu"].(string)
	hostMem, _ := patch["host_memory"].(string)
	if err := ValidateCPUMemory(hostCPU, hostMem); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	var updated FleetConfig
	if _, err := u.client.doJSON(ctx, http.MethodPost, u.client.ControlPlaneURL+"/v1/config", nil, patch, &updated); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, updated)
}

func (u *UIServer) handleAPITemplates(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 35*time.Second)
	defer cancel()
	u.ensurePoolURL(ctx)

	if r.Method == http.MethodDelete {
		name := strings.TrimSpace(r.URL.Query().Get("name"))
		if err := ValidateID(name); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		var res map[string]any
		if _, err := u.client.doJSON(ctx, http.MethodDelete, u.client.ControlPlaneURL+"/v1/templates/"+url.PathEscape(name), nil, nil, &res); err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, res)
		return
	}
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST or DELETE required"})
		return
	}
	var body struct {
		Name         string `json:"name"`
		BaseTemplate string `json:"base_template"`
		Image        string `json:"image"`
		SnapshotID   string `json:"snapshot_id"`
		FromSandbox  string `json:"from_sandbox"`
		Description  string `json:"description"`
		Force        bool   `json:"force"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	if err := ValidateID(body.Name); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	builtIns := BuiltInTemplates()
	if _, isBuiltIn := builtIns[strings.ToLower(body.Name)]; isBuiltIn {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("cannot override built-in template %q", body.Name)})
		return
	}
	if body.BaseTemplate == "" {
		body.BaseTemplate = "ubuntu"
	}
	snapID := strings.TrimSpace(body.SnapshotID)
	if snapID == "" && strings.TrimSpace(body.FromSandbox) != "" {
		resolvedSbx, err := u.client.ResolveUniqueSandboxID(ctx, strings.TrimSpace(body.FromSandbox))
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		autoSnapID := "snap-tpl-" + strings.TrimPrefix(body.Name, "tpl-")
		var snap SnapshotRecord
		if err := u.client.CallSandboxEndpoint(ctx, http.MethodPost, "/__session/snapshot", resolvedSbx, map[string]any{
			"sandbox_id":  resolvedSbx,
			"snapshot_id": autoSnapID,
			"force":       true,
		}, &snap); err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
			return
		}
		snapID = snap.SnapshotID
		if body.BaseTemplate == "ubuntu" && snap.Template != "" {
			body.BaseTemplate = snap.Template
		}
	}
	if body.Image == "" {
		if bSpec, ok := builtIns[strings.ToLower(body.BaseTemplate)]; ok && bSpec.Image != "" {
			body.Image = bSpec.Image
		} else {
			body.Image = "ubuntu:24.04"
		}
	}
	targetURL := u.client.ControlPlaneURL + "/v1/templates"
	if body.Force {
		targetURL += "?force=true"
	}
	var created TemplateSpec
	if _, err := u.client.doJSON(ctx, http.MethodPost, targetURL, nil, TemplateSpec{
		Name:         body.Name,
		BaseTemplate: body.BaseTemplate,
		Image:        body.Image,
		SnapshotID:   snapID,
		Description:  body.Description,
	}, &created); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, created)
}

func (u *UIServer) handleAPISnapshots(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "DELETE required"})
		return
	}
	id := strings.TrimSpace(r.URL.Query().Get("id"))
	if err := ValidateID(id); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	var res map[string]any
	if _, err := u.client.doJSON(ctx, http.MethodDelete, u.client.ControlPlaneURL+"/v1/snapshots/"+url.PathEscape(id), nil, nil, &res); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	writeCPJSON(w, status, v)
}

func renderConsoleHTML(nonce, csrfToken, projectID, region, cpURL, poolURL string) string {
	r := strings.NewReplacer(
		"__CSP_NONCE__", nonce,
		"__CSRF_TOKEN__", csrfToken,
		"__PROJECT_ID__", projectID,
		"__REGION__", region,
		"__CP_URL__", cpURL,
		"__POOL_URL__", poolURL,
	)
	return r.Replace(consoleHTMLTemplate)
}

const consoleHTMLTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="csrf-token" content="__CSRF_TOKEN__">
  <title>Cloud Run — Sandbox Dispenser Console</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Roboto+Mono:wght@400;500&display=swap" rel="stylesheet">
  <style nonce="__CSP_NONCE__">
    :root {
      --bg-canvas: #f8f9fa;
      --bg-surface: #ffffff;
      --bg-subtle: #f1f3f4;
      --bg-blue-tint: #e8f0fe;
      --border-hairline: #dadce0;
      --border-subtle: #e8eaed;
      --text-primary: #202124;
      --text-secondary: #5f6368;
      --text-muted: #80868b;
      --blue-primary: #1a73e8;
      --blue-hover: #1765cc;
      --green-ok: #1e8e3e;
      --green-bg: #e6f4ea;
      --amber-warn: #b06000;
      --amber-bg: #fef7e0;
      --red-err: #d93025;
      --red-bg: #fce8e6;
      --font-sans: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      --font-mono: 'Roboto Mono', monospace;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: var(--font-sans);
      background: var(--bg-canvas);
      color: var(--text-primary);
      font-size: 13px;
      line-height: 1.45;
      height: 100vh;
      display: flex;
      flex-direction: column;
      overflow: hidden;
    }
    /* Top Google Cloud Console Header */
    .gcp-topbar {
      height: 48px;
      background: var(--bg-surface);
      border-bottom: 1px solid var(--border-hairline);
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0 16px;
      flex-shrink: 0;
      z-index: 20;
    }
    .gcp-brand {
      display: flex;
      align-items: center;
      gap: 10px;
      font-weight: 600;
      font-size: 15px;
      color: var(--text-primary);
    }
    .gcp-brand-badge {
      background: var(--bg-blue-tint);
      color: var(--blue-primary);
      font-size: 11px;
      font-weight: 600;
      padding: 2px 8px;
      border-radius: 4px;
      letter-spacing: 0.2px;
    }
    .gcp-project-pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 4px 10px;
      border: 1px solid var(--border-hairline);
      border-radius: 4px;
      background: var(--bg-canvas);
      font-size: 12px;
      color: var(--text-secondary);
      font-family: var(--font-mono);
    }
    .gcp-topbar-actions {
      display: flex;
      align-items: center;
      gap: 10px;
    }
    /* Buttons */
    .btn {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      height: 32px;
      padding: 0 12px;
      border-radius: 4px;
      font-size: 12.5px;
      font-weight: 500;
      font-family: var(--font-sans);
      cursor: pointer;
      border: 1px solid var(--border-hairline);
      background: var(--bg-surface);
      color: var(--text-primary);
      transition: background 0.12s, border-color 0.12s;
    }
    .btn:hover { background: var(--bg-canvas); }
    .btn-primary {
      background: var(--blue-primary);
      border-color: var(--blue-primary);
      color: #ffffff;
    }
    .btn-primary:hover { background: var(--blue-hover); }
    .btn-danger {
      color: var(--red-err);
      border-color: #f4c7c3;
      background: #fff;
    }
    .btn-danger:hover { background: var(--red-bg); }
    .btn-sm {
      height: 26px;
      padding: 0 8px;
      font-size: 11.5px;
    }
    /* Main Workspace Layout */
    .gcp-workspace {
      display: flex;
      flex: 1;
      overflow: hidden;
    }
    .gcp-sidebar {
      width: 232px;
      background: var(--bg-surface);
      border-right: 1px solid var(--border-hairline);
      display: flex;
      flex-direction: column;
      padding: 12px 0;
      flex-shrink: 0;
    }
    .gcp-nav-section-title {
      padding: 4px 16px 8px;
      font-size: 11px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.5px;
      color: var(--text-muted);
    }
    .gcp-nav-item {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 9px 16px;
      color: var(--text-secondary);
      font-weight: 500;
      font-size: 13px;
      cursor: pointer;
      border-left: 3px solid transparent;
      user-select: none;
    }
    .gcp-nav-item:hover {
      background: var(--bg-canvas);
      color: var(--text-primary);
    }
    .gcp-nav-item.active {
      background: var(--bg-blue-tint);
      color: var(--blue-primary);
      border-left-color: var(--blue-primary);
      font-weight: 600;
    }
    .gcp-nav-count {
      font-size: 11px;
      font-family: var(--font-mono);
      padding: 1px 6px;
      border-radius: 10px;
      background: var(--bg-subtle);
      color: var(--text-secondary);
    }
    .gcp-nav-item.active .gcp-nav-count {
      background: #d2e3fc;
      color: var(--blue-primary);
    }
    .gcp-sidebar-footer {
      margin-top: auto;
      padding: 12px 16px;
      border-top: 1px solid var(--border-subtle);
      font-size: 11.5px;
      color: var(--text-secondary);
    }
    .gcp-main {
      flex: 1;
      overflow-y: auto;
      padding: 20px 24px 48px;
    }
    /* Cards & Grids */
    .kpi-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
      gap: 14px;
      margin-bottom: 18px;
    }
    .card {
      background: var(--bg-surface);
      border: 1px solid var(--border-hairline);
      border-radius: 6px;
      padding: 16px;
      margin-bottom: 18px;
    }
    .kpi-card {
      background: var(--bg-surface);
      border: 1px solid var(--border-hairline);
      border-radius: 6px;
      padding: 14px 16px;
    }
    .kpi-label {
      font-size: 11.5px;
      font-weight: 600;
      color: var(--text-secondary);
      text-transform: uppercase;
      letter-spacing: 0.4px;
      margin-bottom: 6px;
    }
    .kpi-value {
      font-size: 24px;
      font-weight: 600;
      color: var(--text-primary);
      font-family: var(--font-mono);
    }
    .kpi-sub {
      font-size: 12px;
      color: var(--text-secondary);
      margin-top: 4px;
    }
    .card-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 12px;
      padding-bottom: 10px;
      border-bottom: 1px solid var(--border-subtle);
    }
    .card-title {
      font-size: 14px;
      font-weight: 600;
      color: var(--text-primary);
    }
    /* Status Badges */
    .badge {
      display: inline-flex;
      align-items: center;
      gap: 4px;
      padding: 2px 8px;
      border-radius: 12px;
      font-size: 11px;
      font-weight: 600;
      font-family: var(--font-mono);
    }
    .badge-running { background: var(--green-bg); color: var(--green-ok); }
    .badge-suspended { background: var(--amber-bg); color: var(--amber-warn); }
    .badge-dead { background: var(--red-bg); color: var(--red-err); }
    .badge-info { background: var(--bg-blue-tint); color: var(--blue-primary); }
    .badge-neutral { background: var(--bg-subtle); color: var(--text-secondary); }
    /* Tables */
    .table-wrap {
      overflow-x: auto;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 12.5px;
    }
    th {
      text-align: left;
      padding: 9px 12px;
      background: var(--bg-canvas);
      border-bottom: 1px solid var(--border-hairline);
      font-size: 11px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.4px;
      color: var(--text-secondary);
    }
    td {
      padding: 10px 12px;
      border-bottom: 1px solid var(--border-subtle);
      vertical-align: middle;
    }
    tr:hover td {
      background: #f4f7fc;
    }
    .mono {
      font-family: var(--font-mono);
      font-size: 12px;
    }
    .link-btn {
      color: var(--blue-primary);
      font-weight: 500;
      cursor: pointer;
      background: none;
      border: none;
      font-family: var(--font-mono);
      font-size: 12px;
      text-align: left;
    }
    .link-btn:hover { text-decoration: underline; }
    /* Host Bin-Packing Strip */
    .host-strip-grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(290px, 1fr));
      gap: 12px;
    }
    .host-pack-card {
      border: 1px solid var(--border-hairline);
      border-radius: 6px;
      padding: 12px;
      background: var(--bg-canvas);
    }
    .slot-bar {
      display: grid;
      grid-template-columns: repeat(10, 1fr);
      gap: 4px;
      margin: 10px 0 8px;
    }
    .slot-cell {
      height: 14px;
      border-radius: 2px;
      background: #e0e3e7;
      border: 1px solid #cfd4da;
    }
    .slot-cell.filled {
      background: var(--blue-primary);
      border-color: var(--blue-hover);
    }
    .slot-cell.draining {
      background: var(--amber-warn);
      border-color: var(--amber-warn);
    }
    /* Filter Bar & Forms */
    .filter-bar {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      align-items: center;
      margin-bottom: 12px;
    }
    .input, .select {
      height: 32px;
      padding: 0 10px;
      border: 1px solid var(--border-hairline);
      border-radius: 4px;
      background: var(--bg-surface);
      color: var(--text-primary);
      font-size: 12.5px;
      font-family: var(--font-sans);
    }
    .input:focus, .select:focus {
      outline: 2px solid var(--blue-primary);
      border-color: transparent;
    }
    .form-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 12px;
      margin-bottom: 14px;
    }
    .form-field {
      display: flex;
      flex-direction: column;
      gap: 4px;
    }
    .form-field label {
      font-size: 11.5px;
      font-weight: 600;
      color: var(--text-secondary);
    }
    /* Right Slide-over Sandbox Detail & Live Exec Drawer */
    .drawer-backdrop {
      position: fixed;
      inset: 0;
      background: rgba(32, 33, 36, 0.35);
      display: none;
      justify-content: flex-end;
      z-index: 50;
    }
    .drawer-backdrop.open { display: flex; }
    .drawer {
      width: 660px;
      max-width: 94vw;
      background: var(--bg-surface);
      height: 100%;
      border-left: 1px solid var(--border-hairline);
      display: flex;
      flex-direction: column;
      box-shadow: -4px 0 20px rgba(0,0,0,0.12);
    }
    .drawer-header {
      padding: 14px 20px;
      border-bottom: 1px solid var(--border-hairline);
      display: flex;
      align-items: center;
      justify-content: space-between;
    }
    .drawer-body {
      padding: 18px 20px;
      overflow-y: auto;
      flex: 1;
    }
    .terminal-box {
      background: #1e1f22;
      color: #e8eaed;
      font-family: var(--font-mono);
      font-size: 12px;
      padding: 12px;
      border-radius: 4px;
      min-height: 110px;
      max-height: 280px;
      overflow-y: auto;
      white-space: pre-wrap;
      word-break: break-word;
      margin-top: 8px;
    }
    /* Modal */
    .modal-backdrop {
      position: fixed;
      inset: 0;
      background: rgba(32, 33, 36, 0.4);
      display: none;
      align-items: center;
      justify-content: center;
      z-index: 60;
    }
    .modal-backdrop.open { display: flex; }
    .modal {
      width: 500px;
      max-width: 92vw;
      background: var(--bg-surface);
      border-radius: 6px;
      border: 1px solid var(--border-hairline);
      padding: 20px;
      box-shadow: 0 8px 30px rgba(0,0,0,0.18);
    }
    /* Toast */
    .toast-container {
      position: fixed;
      bottom: 18px;
      right: 18px;
      z-index: 80;
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
    .toast {
      padding: 10px 14px;
      border-radius: 4px;
      background: #202124;
      color: #ffffff;
      font-size: 12.5px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.2);
      border-left: 4px solid var(--blue-primary);
    }
    .toast.error { border-left-color: var(--red-err); }
    .toast.success { border-left-color: var(--green-ok); }
    .hidden { display: none !important; }
  </style>
</head>
<body>
  <header class="gcp-topbar">
    <div class="gcp-brand">
      <span>Google Cloud Run</span>
      <span style="color:#dadce0;">/</span>
      <span>Sandbox Dispenser</span>
      <span class="gcp-brand-badge">SESSION POOL</span>
      <span class="gcp-project-pill" id="header-project-pill">__PROJECT_ID__ (__REGION__)</span>
    </div>
    <div class="gcp-topbar-actions">
      <label style="display:inline-flex;align-items:center;gap:6px;font-size:12px;color:var(--text-secondary);cursor:pointer;">
        <input type="checkbox" id="auto-refresh-toggle" checked>
        <span>Live Auto-Refresh (3s)</span>
      </label>
      <button class="btn" id="btn-manual-refresh" type="button">Refresh</button>
      <button class="btn btn-primary" id="btn-open-create" type="button">+ Create Sandbox</button>
    </div>
  </header>

  <div class="gcp-workspace">
    <aside class="gcp-sidebar">
      <div class="gcp-nav-section-title">Sandbox Fleet</div>
      <div class="gcp-nav-item active" data-tab="overview">
        <span>Overview &amp; Telemetry</span>
      </div>
      <div class="gcp-nav-item" data-tab="sandboxes">
        <span>Sandboxes</span>
        <span class="gcp-nav-count" id="nav-count-sandboxes">0</span>
      </div>
      <div class="gcp-nav-item" data-tab="hosts">
        <span>Host Session Clones</span>
        <span class="gcp-nav-count" id="nav-count-hosts">0</span>
      </div>
      <div class="gcp-nav-item" data-tab="config">
        <span>Config &amp; Templates</span>
      </div>
      <div class="gcp-nav-item" data-tab="security">
        <span>Security &amp; IAM / IAP</span>
        <span class="gcp-nav-count" id="nav-count-security">IAP</span>
      </div>
      <div class="gcp-sidebar-footer">
        <div style="font-weight:600;margin-bottom:4px;">Control Plane Endpoint</div>
        <div class="mono" style="font-size:10.5px;word-break:break-all;color:var(--text-muted);" id="sidebar-cp-url">__CP_URL__</div>
      </div>
    </aside>

    <main class="gcp-main">
      <!-- TAB 1: OVERVIEW -->
      <section id="tab-overview">
        <div class="kpi-grid">
          <div class="kpi-card">
            <div class="kpi-label">Active Sandboxes</div>
            <div class="kpi-value" id="kpi-running-sbx">0</div>
            <div class="kpi-sub" id="kpi-sub-sbx">0 suspended / 0 total</div>
          </div>
          <div class="kpi-card">
            <div class="kpi-label">Active Host Clones</div>
            <div class="kpi-value" id="kpi-active-hosts">0</div>
            <div class="kpi-sub" id="kpi-sub-hosts">4 vCPU / 8Gi per host clone</div>
          </div>
          <div class="kpi-card">
            <div class="kpi-label">Bin-Packing Efficiency</div>
            <div class="kpi-value" id="kpi-binpack-pct">0.0%</div>
            <div class="kpi-sub" id="kpi-sub-slots">0 / 0 capacity slots used</div>
          </div>
          <div class="kpi-card">
            <div class="kpi-label">Memory Overcommit</div>
            <div class="kpi-value" id="kpi-mem-overcommit">0.00x</div>
            <div class="kpi-sub" id="kpi-sub-mem">Nominal allocated vs physical RAM</div>
          </div>
          <div class="kpi-card">
            <div class="kpi-label">CPU Overcommit</div>
            <div class="kpi-value" id="kpi-cpu-overcommit">0.00x</div>
            <div class="kpi-sub" id="kpi-sub-cpu">Nominal vCPU vs physical vCPU</div>
          </div>
          <div class="kpi-card">
            <div class="kpi-label">Overload Protections</div>
            <div class="kpi-value" id="kpi-overload-protections">0 / 0</div>
            <div class="kpi-sub" id="kpi-sub-overload">Hot Evacuations / HTTP 429 Rejects</div>
          </div>
          <div class="kpi-card">
            <div class="kpi-label">Live Migrations &amp; Crash Recovery</div>
            <div class="kpi-value" id="kpi-migrations">0 / 0</div>
            <div class="kpi-sub">Migrations / Watchdog Recoveries (RPO=0)</div>
          </div>
          <div class="kpi-card">
            <div class="kpi-label">Fleet Uptime SLA</div>
            <div class="kpi-value" id="kpi-uptime">100.000%</div>
            <div class="kpi-sub" id="kpi-leader">Leader: -</div>
          </div>
        </div>

        <div class="card">
          <div class="card-header">
            <div class="card-title">Real-Time Session Pool Bin-Packing &amp; Resource Overcommit Strip</div>
            <div style="display:flex;gap:8px;">
              <button class="btn btn-sm" id="btn-overview-compact" type="button">Compact Fleet (Bin-Pack)</button>
            </div>
          </div>
          <div class="host-strip-grid" id="overview-host-strip"></div>
        </div>

        <div class="card">
          <div class="card-header">
            <div class="card-title">Resource Observability, Sizing Recommendations &amp; Time-Series Telemetry</div>
            <span class="badge badge-info">sbx metrics --history</span>
          </div>
          <div id="overview-recommendations-list" style="margin-bottom:12px;display:flex;flex-direction:column;gap:6px;"></div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Sample Time (UTC)</th>
                  <th>Host Session</th>
                  <th>Overload State</th>
                  <th>Active Sandboxes</th>
                  <th>Actual Memory RSS %</th>
                  <th>Available RAM</th>
                  <th>Memory Overcommit</th>
                  <th>CPU Usage %</th>
                  <th>CPU Overcommit</th>
                </tr>
              </thead>
              <tbody id="overview-history-tbody"></tbody>
            </table>
          </div>
        </div>

        <div class="card">
          <div class="card-header">
            <div class="card-title">Live Fleet Activity &amp; Migration Stream</div>
            <div style="display:flex;gap:8px;align-items:center;">
              <select class="select" id="event-type-filter" style="height:28px;font-size:12px;">
                <option value="">All Events</option>
                <option value="CREATE">CREATE</option>
                <option value="MIGRATE">MIGRATE</option>
                <option value="HOT_SHED">HOT_SHED</option>
                <option value="CRASH_RECOVERY">CRASH_RECOVERY</option>
                <option value="SUSPEND">SUSPEND</option>
                <option value="RESUME">RESUME</option>
                <option value="COMPACT">COMPACT</option>
                <option value="SECURITY_POLICY">SECURITY_POLICY</option>
              </select>
            </div>
          </div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Time (UTC)</th>
                  <th>Event</th>
                  <th>Sandbox ID</th>
                  <th>Host Session</th>
                  <th>Duration</th>
                  <th>Details</th>
                </tr>
              </thead>
              <tbody id="overview-events-tbody"></tbody>
            </table>
          </div>
        </div>
      </section>

      <!-- TAB 2: SANDBOXES -->
      <section id="tab-sandboxes" class="hidden">
        <div class="card">
          <div class="card-header">
            <div class="card-title">Managed Micro-Sandboxes</div>
            <button class="btn btn-primary btn-sm" id="btn-sbx-tab-create" type="button">+ Create Sandbox</button>
          </div>
          <div class="filter-bar">
            <input class="input" id="sbx-search-input" type="text" placeholder="Filter by sandbox_id, name, or template..." style="min-width:260px;">
            <select class="select" id="sbx-status-filter">
              <option value="">Status: ALL</option>
              <option value="RUNNING">RUNNING</option>
              <option value="SUSPENDED">SUSPENDED</option>
            </select>
            <input class="input" id="sbx-host-filter" type="text" placeholder="Filter by host_id...">
          </div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Sandbox ID</th>
                  <th>Name</th>
                  <th>Status</th>
                  <th>Template</th>
                  <th>Size (CPU/RAM)</th>
                  <th>Live RSS / CPU</th>
                  <th>Network Egress</th>
                  <th>Persistent</th>
                  <th>Host Session</th>
                  <th>Epoch</th>
                  <th>Migrations / Rec</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody id="sandboxes-tbody"></tbody>
            </table>
          </div>
        </div>
      </section>

      <!-- TAB 3: HOST CLONES -->
      <section id="tab-hosts" class="hidden">
        <div class="card">
          <div class="card-header">
            <div class="card-title">Cloud Run Session Pool Host Clones (sandbox-pool)</div>
            <button class="btn btn-primary btn-sm" id="btn-hosts-compact" type="button">Compact Fleet Now</button>
          </div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Host Session ID</th>
                  <th>Status</th>
                  <th>Overload State</th>
                  <th>Slot Utilization</th>
                  <th>Host Size</th>
                  <th>Memory (RSS / Overcommit)</th>
                  <th>CPU (Usage / Overcommit)</th>
                  <th>Rejects / Hot Evacs</th>
                  <th>Hosted Sandboxes</th>
                  <th>Last Heartbeat</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody id="hosts-tbody"></tbody>
            </table>
          </div>
        </div>
      </section>

      <!-- TAB 4: CONFIG & TEMPLATES -->
      <section id="tab-config" class="hidden">
        <div class="card">
          <div class="card-header">
            <div class="card-title">Fleet, Bin-Packing &amp; Overload Protection Configuration</div>
          </div>
          <form id="config-form">
            <div class="form-grid">
              <div class="form-field">
                <label for="cfg-max-density">Max Density (Sandboxes / Host)</label>
                <input class="input" id="cfg-max-density" type="number" min="1" max="100" required>
              </div>
              <div class="form-field">
                <label for="cfg-host-cpu">Host Clone vCPU (sandbox-pool)</label>
                <input class="input" id="cfg-host-cpu" type="text" placeholder="4" required>
              </div>
              <div class="form-field">
                <label for="cfg-host-mem">Host Clone Memory (sandbox-pool)</label>
                <input class="input" id="cfg-host-mem" type="text" placeholder="8Gi" required>
              </div>
              <div class="form-field">
                <label for="cfg-min-free-mb">Min Free Memory Guardrail (MB)</label>
                <input class="input" id="cfg-min-free-mb" type="number" min="32" max="65536" required>
              </div>
              <div class="form-field">
                <label for="cfg-min-free-pct">Min Free Memory Guardrail (%)</label>
                <input class="input" id="cfg-min-free-pct" type="number" step="0.1" min="1" max="90" required>
              </div>
              <div class="form-field">
                <label for="cfg-max-mem-overcommit">Max Nominal Memory Overcommit (x)</label>
                <input class="input" id="cfg-max-mem-overcommit" type="number" step="0.1" min="0.5" max="10" required>
              </div>
              <div class="form-field">
                <label for="cfg-hot-mem-pct">Hot-Host Memory Shedding Threshold (%)</label>
                <input class="input" id="cfg-hot-mem-pct" type="number" step="0.1" min="10" max="99" required>
              </div>
              <div class="form-field">
                <label for="cfg-hot-cpu-pct">Hot-Host CPU Shedding Threshold (%)</label>
                <input class="input" id="cfg-hot-cpu-pct" type="number" step="0.1" min="10" max="99" required>
              </div>
              <div class="form-field">
                <label for="cfg-sync-sec">Periodic GCS Sync (sec)</label>
                <input class="input" id="cfg-sync-sec" type="number" min="1" max="3600" required>
              </div>
              <div class="form-field">
                <label for="cfg-hb-sec">Watchdog Heartbeat Timeout (sec)</label>
                <input class="input" id="cfg-hb-sec" type="number" min="5" max="600" required>
              </div>
              <div class="form-field">
                <label for="cfg-auto-binpack">Auto Bin-Pack Compaction</label>
                <select class="select" id="cfg-auto-binpack">
                  <option value="true">Enabled (true)</option>
                  <option value="false">Disabled (false)</option>
                </select>
              </div>
              <div class="form-field">
                <label for="cfg-default-idle-timeout">Default Idle Auto-Suspend Timeout (sec, 0=off)</label>
                <input class="input" id="cfg-default-idle-timeout" type="number" min="0" max="86400" value="0" required>
              </div>
              <div class="form-field">
                <label for="cfg-target-rev">Target Revision Rollout Filter</label>
                <input class="input" id="cfg-target-rev" type="text" placeholder="(optional revision name)">
              </div>
            </div>
            <button class="btn btn-primary" type="submit">Save Fleet &amp; Overload Configuration</button>
          </form>
        </div>

        <div class="card">
          <div class="card-header">
            <div class="card-title">Sandbox Templates (Built-In &amp; Custom)</div>
          </div>
          <form id="template-create-form" style="margin-bottom:14px;">
            <div class="form-grid">
              <div class="form-field">
                <label for="tpl-name">New Template Name</label>
                <input class="input" id="tpl-name" type="text" placeholder="my-custom-tpl" required>
              </div>
              <div class="form-field">
                <label for="tpl-base">Base Template</label>
                <select class="select" id="tpl-base">
                  <option value="ubuntu">ubuntu</option>
                  <option value="python">python</option>
                  <option value="node">node</option>
                  <option value="postgres">postgres</option>
                </select>
              </div>
              <div class="form-field">
                <label for="tpl-from-sbx">From Live Sandbox ID (optional)</label>
                <input class="input" id="tpl-from-sbx" type="text" placeholder="sbx-...">
              </div>
              <div class="form-field">
                <label for="tpl-from-snap">Or From Snapshot ID (optional)</label>
                <input class="input" id="tpl-from-snap" type="text" placeholder="snap-...">
              </div>
              <div class="form-field">
                <label for="tpl-desc">Description</label>
                <input class="input" id="tpl-desc" type="text" placeholder="Custom golden environment">
              </div>
            </div>
            <button class="btn btn-primary btn-sm" type="submit">+ Register Custom Template</button>
          </form>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Type</th>
                  <th>Base Image</th>
                  <th>Backing Snapshot</th>
                  <th>Description</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody id="templates-tbody"></tbody>
            </table>
          </div>
        </div>

        <div class="card">
          <div class="card-header">
            <div class="card-title">Saved Filesystem Snapshots (GCS Overlay Archives)</div>
          </div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Snapshot ID</th>
                  <th>Source Sandbox</th>
                  <th>Template</th>
                  <th>Size (Bytes)</th>
                  <th>Created At</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody id="snapshots-tbody"></tbody>
            </table>
          </div>
        </div>
      </section>

      <!-- TAB 5: SECURITY & IAM / IAP -->
      <section id="tab-security" class="hidden">
        <div class="card">
          <div class="card-header">
            <div class="card-title">Cloud Identity-Aware Proxy (IAP) &amp; Self-Revocation Safeguard</div>
            <span class="badge badge-running" id="sec-iap-badge">IAP ENABLED (run.googleapis.com/iap-enabled: true)</span>
          </div>
          <div class="form-grid" style="margin-bottom:8px;">
            <div class="form-field">
              <label>Authenticated Caller (You)</label>
              <div class="mono" style="font-weight:600;color:var(--blue-primary);" id="sec-current-principal">user:admin@example.com</div>
            </div>
            <div class="form-field">
              <label>Fleet Deployer Principal (Seeded on Deploy)</label>
              <div class="mono" id="sec-deployer-email">admin@example.com</div>
            </div>
            <div class="form-field">
              <label>Self-Revocation Lockout Safeguard</label>
              <div style="font-size:12px;color:var(--green-ok);font-weight:600;">ACTIVE — Cannot revoke your own UI or Backend access</div>
            </div>
          </div>
          <div style="font-size:12px;color:var(--text-secondary);background:var(--bg-blue-tint);padding:10px 12px;border-radius:4px;">
            When deploying <code>sandbox-ui</code>, Cloud Identity-Aware Proxy (<code>run.googleapis.com/iap-enabled: 'true'</code>) is enabled and the deploying user is automatically added to both the UI IAP policy and the Backend Cloud Run Invoker policy. The server enforces a strict anti-lockout safeguard that blocks any request attempting to remove the currently authenticated user or critical Cloud Run / IAP service accounts.
          </div>
        </div>

        <div class="card">
          <div class="card-header">
            <div class="card-title">Web Console UI Access Policy (sandbox-ui — Cloud IAP &amp; Run Invoker)</div>
            <span class="badge badge-info">roles/iap.httpsResourceAccessor</span>
          </div>
          <form id="sec-add-ui-form" style="display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap;">
            <input class="input mono" id="sec-add-ui-input" type="text" placeholder="user:teammate@example.com or group:eng@example.com" style="min-width:320px;" required>
            <button class="btn btn-primary btn-sm" type="submit">+ Grant UI Access</button>
          </form>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>IAM Principal</th>
                  <th>Granted Roles</th>
                  <th>Safeguard Status</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody id="sec-ui-members-tbody"></tbody>
            </table>
          </div>
        </div>

        <div class="card">
          <div class="card-header">
            <div class="card-title">Backend Services Access Policy (sandbox-control-plane &amp; sandbox-pool)</div>
            <span class="badge badge-info">roles/run.invoker</span>
          </div>
          <form id="sec-add-backend-form" style="display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap;">
            <input class="input mono" id="sec-add-backend-input" type="text" placeholder="user:developer@example.com or serviceAccount:agent@..." style="min-width:320px;" required>
            <button class="btn btn-primary btn-sm" type="submit">+ Grant Backend Invoker Access</button>
          </form>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>IAM Principal</th>
                  <th>Granted Role</th>
                  <th>Target Services</th>
                  <th>Safeguard Status</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody id="sec-backend-members-tbody"></tbody>
            </table>
          </div>
        </div>
      </section>
    </main>
  </div>

  <!-- RIGHT DRAWER: SANDBOX DETAIL, LIVE EXEC & LOGS -->
  <div class="drawer-backdrop" id="sbx-drawer-backdrop">
    <div class="drawer">
      <div class="drawer-header">
        <div>
          <div style="font-size:15px;font-weight:600;" id="drawer-sbx-title">Sandbox Details</div>
          <div class="mono" style="font-size:11.5px;color:var(--text-secondary);" id="drawer-sbx-subtitle"></div>
        </div>
        <button class="btn btn-sm" id="btn-close-drawer" type="button">Close</button>
      </div>
      <div class="drawer-body">
        <div class="card" style="padding:12px;margin-bottom:14px;">
          <div style="font-weight:600;margin-bottom:8px;">Lifecycle &amp; Mobility Controls</div>
          <div style="display:flex;flex-wrap:wrap;gap:6px;" id="drawer-lifecycle-buttons"></div>
          <div style="display:flex;gap:8px;margin-top:10px;align-items:center;flex-wrap:wrap;">
            <input class="input" id="drawer-migrate-target" type="text" placeholder="Target host_id (optional)" style="height:28px;width:165px;">
            <button class="btn btn-sm" id="btn-drawer-migrate" type="button">Live Migrate</button>
            <input class="input" id="drawer-copy-name" type="text" placeholder="Clone name" style="height:28px;width:130px;">
            <button class="btn btn-sm" id="btn-drawer-copy" type="button">CoW Clone</button>
            <input class="input" id="drawer-snap-name" type="text" placeholder="Snapshot ID" style="height:28px;width:130px;">
            <button class="btn btn-sm" id="btn-drawer-snapshot" type="button">Save Snapshot</button>
          </div>
        </div>

        <div class="card" style="padding:12px;margin-bottom:14px;">
          <div style="font-weight:600;margin-bottom:8px;">HTTP / WebSocket Preview URL (/proxy/)</div>
          <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;">
            <label for="drawer-preview-port" style="font-size:12px;color:var(--text-secondary);">Port:</label>
            <input class="input mono" id="drawer-preview-port" type="number" min="1" max="65535" value="8000" style="width:100px;height:28px;">
            <button class="btn btn-sm btn-primary" id="btn-drawer-open-preview" type="button">Open Preview URL</button>
            <span class="mono" style="font-size:11.5px;color:var(--text-secondary);" id="drawer-preview-path-hint">/proxy/&lt;sandbox_id&gt;/8000/</span>
          </div>
        </div>

        <div class="card" style="padding:12px;margin-bottom:14px;">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
            <span style="font-weight:600;">Interactive Command Execution (sbx exec)</span>
            <label style="display:inline-flex;align-items:center;gap:6px;cursor:pointer;font-size:12px;">
              <input type="checkbox" id="drawer-exec-stream" checked>
              <span>Stream live output (SSE)</span>
            </label>
          </div>
          <form id="drawer-exec-form" style="display:flex;gap:8px;margin-bottom:8px;">
            <input class="input mono" id="drawer-exec-cmd" type="text" value="uname -a && id && ls -la /workspace" style="flex:1;" required>
            <button class="btn btn-primary" type="submit" id="btn-drawer-run-exec">Run</button>
          </form>
          <div style="font-size:11.5px;color:var(--text-secondary);" id="drawer-exec-meta">Ready to execute command inside sandbox namespace.</div>
          <pre class="terminal-box" id="drawer-exec-output"></pre>
        </div>

        <div class="card" style="padding:12px;margin-bottom:14px;">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;">
            <span style="font-weight:600;">Sandbox Execution Logs (sbx logs)</span>
            <div style="display:flex;gap:6px;align-items:center;">
              <select class="select" id="drawer-logs-tail" style="height:26px;font-size:11.5px;">
                <option value="25">Last 25 lines</option>
                <option value="100" selected>Last 100 lines</option>
                <option value="500">Last 500 lines</option>
              </select>
              <button class="btn btn-sm" id="btn-drawer-refresh-logs" type="button">Refresh Logs</button>
            </div>
          </div>
          <pre class="terminal-box" id="drawer-logs-output"></pre>
        </div>

        <div class="card" style="padding:12px;">
          <div style="font-weight:600;margin-bottom:8px;">Live Resource &amp; Egress Update (sbx update)</div>
          <form id="drawer-resize-form" style="display:flex;gap:8px;align-items:flex-end;flex-wrap:wrap;">
            <div class="form-field">
              <label for="drawer-cpu">vCPU</label>
              <input class="input" id="drawer-cpu" type="text" style="width:80px;height:28px;">
            </div>
            <div class="form-field">
              <label for="drawer-mem">Memory</label>
              <input class="input" id="drawer-mem" type="text" style="width:90px;height:28px;">
            </div>
            <div class="form-field">
              <label for="drawer-idle">Idle Timeout (sec)</label>
              <input class="input" id="drawer-idle" type="number" min="0" style="width:110px;height:28px;">
            </div>
            <div class="form-field">
              <label for="drawer-allow-egress">Network Egress</label>
              <select class="select" id="drawer-allow-egress" style="height:28px;font-size:12px;">
                <option value="false">Isolated (No Egress)</option>
                <option value="true">Allow Egress (--allow-egress)</option>
              </select>
            </div>
            <button class="btn btn-sm btn-primary" type="submit">Update Sandbox</button>
          </form>
        </div>
      </div>
    </div>
  </div>

  <!-- MODAL: CREATE SANDBOX -->
  <div class="modal-backdrop" id="create-modal-backdrop">
    <div class="modal">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;">
        <span style="font-size:15px;font-weight:600;">Create Cloud Run Micro-Sandbox</span>
        <button class="btn btn-sm" id="btn-close-create" type="button">Cancel</button>
      </div>
      <form id="create-sbx-form">
        <div class="form-grid">
          <div class="form-field">
            <label for="create-name">Sandbox Name (optional)</label>
            <input class="input" id="create-name" type="text" placeholder="my-agent-sbx">
          </div>
          <div class="form-field">
            <label for="create-template">Template</label>
            <select class="select" id="create-template">
              <option value="ubuntu">ubuntu (Ubuntu 24.04)</option>
              <option value="python">python (Python 3.12)</option>
              <option value="node">node (Node.js 22)</option>
              <option value="postgres">postgres (PostgreSQL 16)</option>
            </select>
          </div>
          <div class="form-field">
            <label for="create-cpu">vCPU Limit</label>
            <input class="input" id="create-cpu" type="text" value="1" required>
          </div>
          <div class="form-field">
            <label for="create-mem">Memory Limit</label>
            <input class="input" id="create-mem" type="text" value="1Gi" required>
          </div>
          <div class="form-field">
            <label for="create-host">Target Host Session (optional)</label>
            <input class="input" id="create-host" type="text" placeholder="Auto bin-pack">
          </div>
          <div class="form-field">
            <label for="create-snap">From Snapshot ID (optional)</label>
            <input class="input" id="create-snap" type="text" placeholder="snap-...">
          </div>
          <div class="form-field">
            <label for="sbx-idle-timeout">Idle Auto-Suspend (sec, 0=off)</label>
            <input class="input" id="sbx-idle-timeout" type="number" min="0" max="86400" value="0">
          </div>
        </div>
        <div style="margin-bottom:8px;">
          <label style="display:inline-flex;align-items:center;gap:6px;cursor:pointer;">
            <input type="checkbox" id="create-persistent" checked>
            <span>Persistent GCS Overlay &amp; RPO=0 Crash Recovery (--persistent)</span>
          </label>
        </div>
        <div style="margin-bottom:14px;">
          <label style="display:inline-flex;align-items:center;gap:6px;cursor:pointer;">
            <input type="checkbox" id="create-allow-egress">
            <span>Allow External Internet Egress inside Sandbox (--allow-egress)</span>
          </label>
        </div>
        <div style="display:flex;justify-content:flex-end;gap:8px;">
          <button class="btn btn-primary" type="submit" id="btn-submit-create">Create Sandbox</button>
        </div>
      </form>
    </div>
  </div>

  <!-- MODAL: CONFIRMATION -->
  <div class="modal-backdrop" id="confirm-modal-backdrop">
    <div class="modal" style="width:420px;">
      <div style="font-size:15px;font-weight:600;margin-bottom:8px;" id="confirm-modal-title">Confirm Action</div>
      <div style="color:var(--text-secondary);margin-bottom:16px;" id="confirm-modal-message"></div>
      <div style="display:flex;justify-content:flex-end;gap:8px;">
        <button class="btn" id="btn-confirm-cancel" type="button">Cancel</button>
        <button class="btn btn-danger" id="btn-confirm-ok" type="button">Confirm</button>
      </div>
    </div>
  </div>

  <div class="toast-container" id="toast-container"></div>

  <script nonce="__CSP_NONCE__">
    (function() {
      'use strict';

      var csrfMeta = document.querySelector('meta[name="csrf-token"]');
      var csrfToken = csrfMeta ? csrfMeta.getAttribute('content') : '';
      var latestOverview = null;
      var selectedSandboxID = '';
      var pendingConfirmFn = null;

      // Safe DOM constructor helper — strictly uses createElement/textContent/appendChild
      function el(tag, attrs, children) {
        var node = document.createElement(tag);
        if (attrs) {
          var keys = Object.keys(attrs);
          for (var i = 0; i < keys.length; i++) {
            var k = keys[i];
            var v = attrs[k];
            if (k === 'className') {
              node.className = v;
            } else if (k === 'text') {
              node.textContent = String(v);
            } else if (k === 'onclick' && typeof v === 'function') {
              node.addEventListener('click', v);
            } else if (v !== undefined && v !== null) {
              node.setAttribute(k, String(v));
            }
          }
        }
        if (Array.isArray(children)) {
          for (var j = 0; j < children.length; j++) {
            var c = children[j];
            if (c === null || c === undefined) continue;
            if (typeof c === 'string' || typeof c === 'number') {
              node.appendChild(document.createTextNode(String(c)));
            } else {
              node.appendChild(c);
            }
          }
        }
        return node;
      }

      function showToast(msg, kind) {
        var wrap = document.getElementById('toast-container');
        var item = el('div', { className: 'toast ' + (kind || ''), text: msg });
        wrap.appendChild(item);
        setTimeout(function() {
          if (item.parentNode) item.parentNode.removeChild(item);
        }, 4200);
      }

      function openConfirmModal(title, message, onConfirm) {
        document.getElementById('confirm-modal-title').textContent = title;
        document.getElementById('confirm-modal-message').textContent = message;
        pendingConfirmFn = onConfirm;
        document.getElementById('confirm-modal-backdrop').classList.add('open');
      }

      document.getElementById('btn-confirm-cancel').addEventListener('click', function() {
        pendingConfirmFn = null;
        document.getElementById('confirm-modal-backdrop').classList.remove('open');
      });
      document.getElementById('btn-confirm-ok').addEventListener('click', function() {
        var fn = pendingConfirmFn;
        pendingConfirmFn = null;
        document.getElementById('confirm-modal-backdrop').classList.remove('open');
        if (fn) fn();
      });

      function apiFetch(path, method, bodyObj) {
        var opts = {
          method: method || 'GET',
          headers: { 'Accept': 'application/json' },
          credentials: 'same-origin'
        };
        if (opts.method !== 'GET') {
          opts.headers['X-CSRF-Token'] = csrfToken;
          if (bodyObj !== undefined && bodyObj !== null) {
            opts.headers['Content-Type'] = 'application/json';
            opts.body = JSON.stringify(bodyObj);
          }
        }
        return fetch(path, opts).then(function(resp) {
          return resp.json().then(function(data) {
            if (!resp.ok) {
              var errMsg = (data && (data.message || data.error)) ? (data.message || data.error) : ('HTTP ' + resp.status);
              throw new Error(errMsg);
            }
            return data;
          });
        });
      }

      function statusBadge(status) {
        var s = String(status || '').toUpperCase();
        var cls = 'badge badge-neutral';
        if (s === 'RUNNING' || s === 'ACTIVE' || s === 'NORMAL' || s === 'CREATE' || s === 'RESUME') cls = 'badge badge-running';
        else if (s === 'SUSPENDED' || s === 'DRAINING' || s === 'ELEVATED' || s === 'MIGRATE' || s === 'SUSPEND' || s === 'COMPACT' || s === 'SECURITY_POLICY') cls = 'badge badge-suspended';
        else if (s === 'DEAD' || s === 'CRASH_RECOVERY' || s === 'ERROR' || s === 'ADMISSION_BLOCKED' || s === 'HOT_SHEDDING' || s === 'HOT_SHED') cls = 'badge badge-dead';
        return el('span', { className: cls, text: s || 'UNKNOWN' });
      }

      function formatBytes(bytes) {
        var b = Number(bytes || 0);
        if (!b || b <= 0) return '0 B';
        if (b >= 1073741824) return (b / 1073741824).toFixed(2) + ' GiB';
        if (b >= 1048576) return (b / 1048576).toFixed(1) + ' MiB';
        if (b >= 1024) return (b / 1024).toFixed(1) + ' KiB';
        return b + ' B';
      }

      function formatTime(iso) {
        if (!iso) return '-';
        var d = new Date(iso);
        if (isNaN(d.getTime())) return String(iso);
        return d.toISOString().replace('T', ' ').substring(0, 19);
      }

      function renderOverview(data) {
        var st = data.stats || {};
        var cfg = data.config || {};
        var met = data.metrics || {};
        document.getElementById('kpi-running-sbx').textContent = String(st.running_sandboxes || 0);
        document.getElementById('kpi-sub-sbx').textContent =
          String(st.suspended_sandboxes || 0) + ' suspended / ' + String(st.total_sandboxes || 0) + ' total';
        document.getElementById('kpi-active-hosts').textContent = String(st.active_hosts || 0);
        document.getElementById('kpi-sub-hosts').textContent =
          (cfg.host_cpu || '4') + ' vCPU / ' + (cfg.host_memory || '8Gi') + ' per host clone (max_density=' + (cfg.max_density || 10) + ')';
        var eff = typeof st.bin_pack_efficiency_pct === 'number' ? st.bin_pack_efficiency_pct.toFixed(1) : '0.0';
        document.getElementById('kpi-binpack-pct').textContent = eff + '%';
        document.getElementById('kpi-sub-slots').textContent =
          String(st.used_capacity_slots || 0) + ' / ' + String(st.total_capacity_slots || 0) + ' capacity slots used';

        var memRatio = typeof st.fleet_memory_overcommit_ratio === 'number' ? st.fleet_memory_overcommit_ratio : (Number(met.fleet_memory_overcommit_ratio) || 0);
        var cpuRatio = typeof st.fleet_cpu_overcommit_ratio === 'number' ? st.fleet_cpu_overcommit_ratio : (Number(met.fleet_cpu_overcommit_ratio) || 0);
        var hotEvacs = typeof st.total_hot_migrations === 'number' ? st.total_hot_migrations : (Number(met.total_hot_migrations) || 0);
        var memRejects = typeof st.total_memory_rejections === 'number' ? st.total_memory_rejections : (Number(met.total_memory_rejections) || 0);

        document.getElementById('kpi-mem-overcommit').textContent = memRatio.toFixed(2) + 'x';
        document.getElementById('kpi-sub-mem').textContent = 'Max guardrail: ' + Number(cfg.max_memory_overcommit_ratio || 2.0).toFixed(1) + 'x';
        document.getElementById('kpi-cpu-overcommit').textContent = cpuRatio.toFixed(2) + 'x';
        document.getElementById('kpi-sub-cpu').textContent = 'Hot CPU threshold: ' + Number(cfg.hot_cpu_threshold_pct || 85).toFixed(0) + '%';
        document.getElementById('kpi-overload-protections').textContent = String(hotEvacs) + ' / ' + String(memRejects);
        document.getElementById('kpi-sub-overload').textContent = 'Hot Evacuations / HTTP 429 Rejects';

        document.getElementById('kpi-migrations').textContent =
          String(st.total_migrations || 0) + ' / ' + String(st.total_crash_recoveries || 0);
        var sla = typeof st.fleet_uptime_sla_pct === 'number' ? st.fleet_uptime_sla_pct.toFixed(3) : '100.000';
        document.getElementById('kpi-uptime').textContent = sla + '%';
        document.getElementById('kpi-leader').textContent = 'Leader: ' + (st.leader_id || '-');

        document.getElementById('nav-count-sandboxes').textContent = String((data.sandboxes || []).length);
        document.getElementById('nav-count-hosts').textContent = String((data.hosts || []).length);

        // Render Host Bin-Packing Strip
        var strip = document.getElementById('overview-host-strip');
        strip.replaceChildren();
        var hosts = data.hosts || [];
        if (hosts.length === 0) {
          strip.appendChild(el('div', { text: 'No active host clones registered yet.', style: 'color:var(--text-secondary);' }));
        } else {
          for (var i = 0; i < hosts.length; i++) {
            var h = hosts[i];
            var maxD = h.max_density || cfg.max_density || 10;
            var activeN = h.active_sandboxes || 0;
            var slotBar = el('div', { className: 'slot-bar' });
            for (var s = 0; s < maxD && s < 20; s++) {
              var sCls = 'slot-cell';
              if (s < activeN) {
                sCls += (h.status === 'DRAINING') ? ' draining' : ' filled';
              }
              slotBar.appendChild(el('div', { className: sCls }));
            }
            var sbxChips = el('div', { style: 'display:flex;flex-wrap:wrap;gap:4px;margin-top:6px;' });
            var sids = h.sandbox_ids || [];
            for (var k = 0; k < sids.length; k++) {
              (function(sid) {
                sbxChips.appendChild(el('button', {
                  className: 'btn btn-sm mono',
                  style: 'height:22px;padding:0 6px;font-size:11px;',
                  text: sid,
                  onclick: function() { openSandboxDrawer(sid); }
                }));
              })(sids[k]);
            }
            var usedMemPct = Number(h.used_memory_pct || 0).toFixed(1) + '%';
            var hMemRatio = Number(h.memory_overcommit_ratio || 0).toFixed(2) + 'x';
            var hCpuPct = Number(h.cpu_usage_pct || 0).toFixed(1) + '%';
            var hCpuRatio = Number(h.cpu_overcommit_ratio || 0).toFixed(2) + 'x';
            var card = el('div', { className: 'host-pack-card' }, [
              el('div', { style: 'display:flex;justify-content:space-between;align-items:center;gap:4px;' }, [
                el('span', { className: 'mono', style: 'font-weight:600;', text: h.host_id }),
                el('div', { style: 'display:flex;gap:4px;' }, [
                  statusBadge(h.overload_state || 'NORMAL'),
                  statusBadge(h.status)
                ])
              ]),
              el('div', { style: 'font-size:11.5px;color:var(--text-secondary);margin-top:3px;' }, [
                'Slots: ' + activeN + ' / ' + maxD + ' • ' + (h.host_cpu || '4') + ' vCPU / ' + (h.host_memory || '8Gi') + ' • ' + (h.instance_state || 'idle')
              ]),
              el('div', { className: 'mono', style: 'font-size:11px;color:var(--text-primary);margin-top:4px;' }, [
                'RAM RSS: ' + usedMemPct + ' (overcommit ' + hMemRatio + ') • CPU: ' + hCpuPct + ' (' + hCpuRatio + ')'
              ]),
              slotBar,
              sbxChips
            ]);
            strip.appendChild(card);
          }
        }

        // Render Recommendations & Time-Series History
        var recWrap = document.getElementById('overview-recommendations-list');
        recWrap.replaceChildren();
        var recs = met.recommendations || [];
        for (var rIdx = 0; rIdx < recs.length; rIdx++) {
          recWrap.appendChild(el('div', {
            style: 'padding:8px 10px;border-radius:4px;background:var(--bg-canvas);border-left:3px solid var(--blue-primary);font-size:12px;',
            text: recs[rIdx]
          }));
        }

        var histTbody = document.getElementById('overview-history-tbody');
        histTbody.replaceChildren();
        var histMap = met.history || {};
        var histHostIDs = Object.keys(histMap).sort();
        var histRows = 0;
        for (var hh = 0; hh < histHostIDs.length; hh++) {
          var hid = histHostIDs[hh];
          var samples = histMap[hid] || [];
          var startIdx = samples.length > 5 ? samples.length - 5 : 0;
          for (var smpIdx = samples.length - 1; smpIdx >= startIdx; smpIdx--) {
            var smp = samples[smpIdx];
            histRows++;
            histTbody.appendChild(el('tr', null, [
              el('td', { className: 'mono', text: formatTime(smp.timestamp) }),
              el('td', { className: 'mono', style: 'font-weight:600;', text: hid }),
              el('td', null, [statusBadge(smp.overload_state || 'NORMAL')]),
              el('td', { className: 'mono', text: String(smp.active_sandboxes || 0) }),
              el('td', { className: 'mono', text: Number(smp.used_memory_pct || 0).toFixed(1) + '% (' + formatBytes(smp.used_memory_bytes) + ')' }),
              el('td', { className: 'mono', text: formatBytes(smp.available_memory_bytes) }),
              el('td', { className: 'mono', text: Number(smp.memory_overcommit_ratio || 0).toFixed(2) + 'x' }),
              el('td', { className: 'mono', text: Number(smp.cpu_usage_pct || 0).toFixed(1) + '%' }),
              el('td', { className: 'mono', text: Number(smp.cpu_overcommit_ratio || 0).toFixed(2) + 'x' })
            ]));
          }
        }
        if (histRows === 0) {
          histTbody.appendChild(el('tr', null, [
            el('td', { colspan: '9', style: 'text-align:center;color:var(--text-secondary);', text: 'No host resource telemetry samples collected yet.' })
          ]));
        }

        // Render Events Table
        var evFilter = document.getElementById('event-type-filter').value;
        var evTbody = document.getElementById('overview-events-tbody');
        evTbody.replaceChildren();
        var events = (data.events || []).slice().reverse();
        var shown = 0;
        for (var eIdx = 0; eIdx < events.length && shown < 50; eIdx++) {
          var ev = events[eIdx];
          if (evFilter && ev.type !== evFilter) continue;
          shown++;
          evTbody.appendChild(el('tr', null, [
            el('td', { className: 'mono', text: formatTime(ev.timestamp) }),
            el('td', null, [statusBadge(ev.type)]),
            el('td', { className: 'mono', text: ev.sandbox_id || '-' }),
            el('td', { className: 'mono', text: ev.host_id || '-' }),
            el('td', { className: 'mono', text: ev.duration_ms ? (ev.duration_ms + ' ms') : '-' }),
            el('td', { text: ev.message || '' })
          ]));
        }
        if (shown === 0) {
          evTbody.appendChild(el('tr', null, [
            el('td', { colspan: '6', style: 'text-align:center;color:var(--text-secondary);', text: 'No matching fleet events.' })
          ]));
        }
      }

      function renderSandboxes(data) {
        var tbody = document.getElementById('sandboxes-tbody');
        tbody.replaceChildren();
        var q = (document.getElementById('sbx-search-input').value || '').toLowerCase().trim();
        var stFilter = document.getElementById('sbx-status-filter').value;
        var hostFilter = (document.getElementById('sbx-host-filter').value || '').toLowerCase().trim();
        var list = data.sandboxes || [];
        var count = 0;
        for (var i = 0; i < list.length; i++) {
          var s = list[i];
          if (stFilter && s.status !== stFilter) continue;
          if (hostFilter && String(s.host_id || '').toLowerCase().indexOf(hostFilter) === -1) continue;
          if (q) {
            var hay = (String(s.sandbox_id) + ' ' + String(s.name || '') + ' ' + String(s.template || '')).toLowerCase();
            if (hay.indexOf(q) === -1) continue;
          }
          count++;
          (function(sbx) {
            var actCell = el('div', { style: 'display:flex;gap:4px;flex-wrap:wrap;' });
            actCell.appendChild(el('button', {
              className: 'btn btn-sm',
              text: 'Inspect / Exec',
              onclick: function() { openSandboxDrawer(sbx.sandbox_id); }
            }));
            if (sbx.status === 'RUNNING') {
              actCell.appendChild(el('button', {
                className: 'btn btn-sm',
                text: 'Suspend',
                onclick: function() { triggerLifecycle(sbx.sandbox_id, 'suspend'); }
              }));
            } else {
              actCell.appendChild(el('button', {
                className: 'btn btn-sm',
                text: 'Resume',
                onclick: function() { triggerLifecycle(sbx.sandbox_id, 'resume'); }
              }));
            }
            actCell.appendChild(el('button', {
              className: 'btn btn-sm btn-danger',
              text: 'Delete',
              onclick: function() {
                openConfirmModal('Delete Sandbox', 'Delete sandbox ' + sbx.sandbox_id + ' permanently?', function() {
                  triggerLifecycle(sbx.sandbox_id, 'delete');
                });
              }
            }));

            var rssStr = formatBytes(sbx.used_memory_bytes) + ' (' + Number(sbx.used_memory_pct || 0).toFixed(1) + '%) / CPU ' + Number(sbx.cpu_usage_pct || 0).toFixed(1) + '%';
            var egressBadge = sbx.allow_egress
              ? el('span', { className: 'badge badge-info', text: 'EGRESS ALLOWED' })
              : el('span', { className: 'badge badge-neutral', text: 'ISOLATED' });

            tbody.appendChild(el('tr', null, [
              el('td', null, [
                el('button', {
                  className: 'link-btn',
                  text: sbx.sandbox_id,
                  onclick: function() { openSandboxDrawer(sbx.sandbox_id); }
                })
              ]),
              el('td', { className: 'mono', text: sbx.name || sbx.sandbox_id }),
              el('td', null, [statusBadge(sbx.status)]),
              el('td', { className: 'mono', text: sbx.template || 'ubuntu' }),
              el('td', { className: 'mono', text: (sbx.cpu || '1') + ' / ' + (sbx.memory || '1Gi') }),
              el('td', { className: 'mono', text: rssStr }),
              el('td', null, [egressBadge]),
              el('td', { className: 'mono', text: sbx.persistent ? 'true' : 'false' }),
              el('td', { className: 'mono', text: sbx.host_id || '-' }),
              el('td', { className: 'mono', text: String(sbx.epoch || 1) }),
              el('td', { className: 'mono', text: String(sbx.migration_count || 0) + ' / ' + String(sbx.crash_recoveries || 0) }),
              el('td', null, [actCell])
            ]));
          })(s);
        }
        if (count === 0) {
          tbody.appendChild(el('tr', null, [
            el('td', { colspan: '12', style: 'text-align:center;color:var(--text-secondary);', text: 'No sandboxes match the current filter.' })
          ]));
        }
      }

      function renderHosts(data) {
        var tbody = document.getElementById('hosts-tbody');
        tbody.replaceChildren();
        var hosts = data.hosts || [];
        for (var i = 0; i < hosts.length; i++) {
          (function(h) {
            var actions = el('div', { style: 'display:flex;gap:6px;' }, [
              el('button', {
                className: 'btn btn-sm',
                text: 'Drain & Evacuate',
                onclick: function() {
                  openConfirmModal('Drain Host Clone', 'Live-migrate all sandboxes off ' + h.host_id + '?', function() {
                    runHostAction('drain', h.host_id);
                  });
                }
              }),
              el('button', {
                className: 'btn btn-sm btn-danger',
                text: 'Simulate Crash',
                onclick: function() {
                  openConfirmModal('Simulate Host Hard Crash', 'Simulate SIGKILL crash on ' + h.host_id + ' and trigger watchdog recovery?', function() {
                    runHostAction('simulate-crash', h.host_id);
                  });
                }
              })
            ]);
            var memInfo = Number(h.used_memory_pct || 0).toFixed(1) + '% RSS (' + formatBytes(h.used_memory_bytes) + ') • ' + Number(h.memory_overcommit_ratio || 0).toFixed(2) + 'x';
            var cpuInfo = Number(h.cpu_usage_pct || 0).toFixed(1) + '% • ' + Number(h.cpu_overcommit_ratio || 0).toFixed(2) + 'x';
            var rejHot = String(h.rejected_creations_memory || 0) + ' / ' + String(h.hot_migrations_triggered || 0);
            tbody.appendChild(el('tr', null, [
              el('td', { className: 'mono', style: 'font-weight:600;', text: h.host_id }),
              el('td', null, [statusBadge(h.status)]),
              el('td', null, [statusBadge(h.overload_state || 'NORMAL')]),
              el('td', { className: 'mono', text: String(h.active_sandboxes || 0) + ' / ' + String(h.max_density || 10) + ' (free=' + String(h.free_slots || 0) + ')' }),
              el('td', { className: 'mono', text: (h.host_cpu || '4') + ' vCPU / ' + (h.host_memory || '8Gi') }),
              el('td', { className: 'mono', text: memInfo }),
              el('td', { className: 'mono', text: cpuInfo }),
              el('td', { className: 'mono', text: rejHot }),
              el('td', { className: 'mono', text: (h.sandbox_ids || []).join(', ') || '-' }),
              el('td', { className: 'mono', text: formatTime(h.last_heartbeat_at) }),
              el('td', null, [actions])
            ]));
          })(hosts[i]);
        }
        if (hosts.length === 0) {
          tbody.appendChild(el('tr', null, [
            el('td', { colspan: '11', style: 'text-align:center;color:var(--text-secondary);', text: 'No host clones found.' })
          ]));
        }
      }

      function renderConfigAndTemplates(data) {
        var cfg = data.config || {};
        var activeEl = document.activeElement;
        var formEl = document.getElementById('config-form');
        if (!formEl.contains(activeEl)) {
          document.getElementById('cfg-max-density').value = String(cfg.max_density || 10);
          document.getElementById('cfg-host-cpu').value = cfg.host_cpu || '4';
          document.getElementById('cfg-host-mem').value = cfg.host_memory || '8Gi';
          document.getElementById('cfg-min-free-mb').value = String(cfg.min_free_memory_mb !== undefined ? cfg.min_free_memory_mb : 512);
          document.getElementById('cfg-min-free-pct').value = String(cfg.min_free_memory_pct !== undefined ? cfg.min_free_memory_pct : 15);
          document.getElementById('cfg-max-mem-overcommit').value = String(cfg.max_memory_overcommit_ratio || 2.0);
          document.getElementById('cfg-hot-mem-pct').value = String(cfg.hot_memory_threshold_pct || 85);
          document.getElementById('cfg-hot-cpu-pct').value = String(cfg.hot_cpu_threshold_pct || 90);
          document.getElementById('cfg-sync-sec').value = String(cfg.periodic_sync_sec || 10);
          document.getElementById('cfg-hb-sec').value = String(cfg.heartbeat_timeout_sec || 15);
          document.getElementById('cfg-auto-binpack').value = cfg.auto_bin_pack ? 'true' : 'false';
          document.getElementById('cfg-default-idle-timeout').value = String(cfg.default_idle_timeout_sec || 0);
          document.getElementById('cfg-target-rev').value = cfg.target_revision || '';
        }

        // Populate create modal template dropdown
        var tplSelect = document.getElementById('create-template');
        var curVal = tplSelect.value || 'ubuntu';
        tplSelect.replaceChildren();
        var tpls = data.templates || [];
        for (var i = 0; i < tpls.length; i++) {
          var t = tpls[i];
          var opt = el('option', { value: t.name, text: t.name + ' (' + (t.type || 'builtin') + ')' });
          if (t.name === curVal) opt.selected = true;
          tplSelect.appendChild(opt);
        }

        var tplTbody = document.getElementById('templates-tbody');
        tplTbody.replaceChildren();
        for (var j = 0; j < tpls.length; j++) {
          (function(tpl) {
            var act = el('span', { text: '-' });
            if (tpl.type === 'custom') {
              act = el('button', {
                className: 'btn btn-sm btn-danger',
                text: 'Delete',
                onclick: function() {
                  apiFetch('/api/templates?name=' + encodeURIComponent(tpl.name), 'DELETE')
                    .then(function() { showToast('Deleted template ' + tpl.name, 'success'); refreshAll(); })
                    .catch(function(err) { showToast(err.message, 'error'); });
                }
              });
            }
            tplTbody.appendChild(el('tr', null, [
              el('td', { className: 'mono', style: 'font-weight:600;', text: tpl.name }),
              el('td', null, [el('span', { className: 'badge badge-info', text: tpl.type || 'builtin' })]),
              el('td', { className: 'mono', text: tpl.image || '-' }),
              el('td', { className: 'mono', text: tpl.snapshot_id || '-' }),
              el('td', { text: tpl.description || '' }),
              el('td', null, [act])
            ]));
          })(tpls[j]);
        }

        var snapTbody = document.getElementById('snapshots-tbody');
        snapTbody.replaceChildren();
        var snaps = data.snapshots || [];
        for (var sIdx = 0; sIdx < snaps.length; sIdx++) {
          (function(sn) {
            var acts = el('div', { style: 'display:flex;gap:6px;' }, [
              el('button', {
                className: 'btn btn-sm',
                text: 'Launch Sandbox',
                onclick: function() {
                  document.getElementById('create-snap').value = sn.snapshot_id;
                  document.getElementById('create-modal-backdrop').classList.add('open');
                }
              }),
              el('button', {
                className: 'btn btn-sm btn-danger',
                text: 'Delete',
                onclick: function() {
                  apiFetch('/api/snapshots?id=' + encodeURIComponent(sn.snapshot_id), 'DELETE')
                    .then(function() { showToast('Deleted snapshot ' + sn.snapshot_id, 'success'); refreshAll(); })
                    .catch(function(err) { showToast(err.message, 'error'); });
                }
              })
            ]);
            snapTbody.appendChild(el('tr', null, [
              el('td', { className: 'mono', text: sn.snapshot_id }),
              el('td', { className: 'mono', text: sn.source_sandbox || '-' }),
              el('td', { className: 'mono', text: sn.template || '-' }),
              el('td', { className: 'mono', text: String(sn.size_bytes || 0) }),
              el('td', { className: 'mono', text: formatTime(sn.created_at) }),
              el('td', null, [acts])
            ]));
          })(snaps[sIdx]);
        }
        if (snaps.length === 0) {
          snapTbody.appendChild(el('tr', null, [
            el('td', { colspan: '6', style: 'text-align:center;color:var(--text-secondary);', text: 'No snapshots saved yet.' })
          ]));
        }
      }

      function isProtectedPrincipal(sec, member) {
        if (!sec) return false;
        if (sec.current_member && sec.current_member.toLowerCase() === String(member).toLowerCase()) return true;
        var prot = sec.protected_members || [];
        for (var i = 0; i < prot.length; i++) {
          if (String(prot[i]).toLowerCase() === String(member).toLowerCase()) return true;
        }
        return false;
      }

      function updateSecurityPolicy(action, member) {
        apiFetch('/api/security/policy', 'POST', { action: action, member: member })
          .then(function(updated) {
            if (latestOverview) {
              latestOverview.security = updated;
              renderSecurity(latestOverview);
            }
            showToast('Updated security policy (' + action + ': ' + member + ')', 'success');
            refreshAll();
          })
          .catch(function(err) {
            showToast('Security Policy Safeguard: ' + err.message, 'error');
          });
      }

      function renderSecurity(data) {
        var sec = data.security || {};
        var curMember = sec.current_member || 'user:admin@example.com';
        document.getElementById('sec-current-principal').textContent = curMember;
        document.getElementById('sec-deployer-email').textContent = sec.deployer_email || 'admin@example.com';
        var iapBadge = document.getElementById('sec-iap-badge');
        if (sec.iap_enabled !== false) {
          iapBadge.className = 'badge badge-running';
          iapBadge.textContent = 'IAP ENABLED (run.googleapis.com/iap-enabled: true)';
        } else {
          iapBadge.className = 'badge badge-suspended';
          iapBadge.textContent = 'IAP DISABLED';
        }

        var uiTbody = document.getElementById('sec-ui-members-tbody');
        uiTbody.replaceChildren();
        var uiMembers = sec.ui_access_members || [];
        for (var i = 0; i < uiMembers.length; i++) {
          (function(m) {
            var isSelf = m.toLowerCase() === curMember.toLowerCase();
            var statusCell = isSelf
              ? el('span', { className: 'badge badge-running', text: 'PROTECTED (YOU — CANNOT SELF-REVOKE)' })
              : el('span', { className: 'badge badge-info', text: 'AUTHORIZED UI ACCESSOR' });
            var actBtn = isSelf
              ? el('button', {
                  className: 'btn btn-sm',
                  style: 'opacity:0.75;',
                  text: 'Revoke (Blocked by Safeguard)',
                  onclick: function() {
                    updateSecurityPolicy('remove_ui_member', m);
                  }
                })
              : el('button', {
                  className: 'btn btn-sm btn-danger',
                  text: 'Revoke UI Access',
                  onclick: function() {
                    openConfirmModal('Revoke UI Access', 'Remove ' + m + ' from Cloud IAP UI access policy?', function() {
                      updateSecurityPolicy('remove_ui_member', m);
                    });
                  }
                });
            uiTbody.appendChild(el('tr', null, [
              el('td', { className: 'mono', style: 'font-weight:600;', text: m }),
              el('td', { className: 'mono', text: 'roles/iap.httpsResourceAccessor, roles/run.invoker' }),
              el('td', null, [statusCell]),
              el('td', null, [actBtn])
            ]));
          })(uiMembers[i]);
        }

        var backTbody = document.getElementById('sec-backend-members-tbody');
        backTbody.replaceChildren();
        var backMembers = sec.backend_access_members || [];
        for (var j = 0; j < backMembers.length; j++) {
          (function(m) {
            var isSelf = m.toLowerCase() === curMember.toLowerCase();
            var isProt = isProtectedPrincipal(sec, m);
            var statusBadgeEl = isSelf
              ? el('span', { className: 'badge badge-running', text: 'PROTECTED (YOU — CANNOT SELF-REVOKE)' })
              : (isProt
                  ? el('span', { className: 'badge badge-info', text: 'PROTECTED SYSTEM SERVICE ACCOUNT' })
                  : el('span', { className: 'badge badge-neutral', text: 'AUTHORIZED BACKEND INVOKER' }));
            var actBtn = isProt
              ? el('button', {
                  className: 'btn btn-sm',
                  style: 'opacity:0.75;',
                  text: 'Revoke (Blocked by Safeguard)',
                  onclick: function() {
                    updateSecurityPolicy('remove_backend_member', m);
                  }
                })
              : el('button', {
                  className: 'btn btn-sm btn-danger',
                  text: 'Revoke Backend Access',
                  onclick: function() {
                    openConfirmModal('Revoke Backend Access', 'Remove ' + m + ' from sandbox-control-plane & sandbox-pool invoker policy?', function() {
                      updateSecurityPolicy('remove_backend_member', m);
                    });
                  }
                });
            backTbody.appendChild(el('tr', null, [
              el('td', { className: 'mono', style: 'font-weight:600;', text: m }),
              el('td', { className: 'mono', text: 'roles/run.invoker' }),
              el('td', { className: 'mono', text: 'sandbox-control-plane, sandbox-pool' }),
              el('td', null, [statusBadgeEl]),
              el('td', null, [actBtn])
            ]));
          })(backMembers[j]);
        }
      }

      function findSandbox(id) {
        if (!latestOverview || !latestOverview.sandboxes) return null;
        for (var i = 0; i < latestOverview.sandboxes.length; i++) {
          if (latestOverview.sandboxes[i].sandbox_id === id) return latestOverview.sandboxes[i];
        }
        return null;
      }

      function openSandboxDrawer(sandboxID) {
        selectedSandboxID = sandboxID;
        var sbx = findSandbox(sandboxID);
        document.getElementById('drawer-sbx-title').textContent = sbx ? (sbx.name ? (sbx.name + ' (' + sbx.sandbox_id + ')') : sbx.sandbox_id) : sandboxID;
        document.getElementById('drawer-sbx-subtitle').textContent = sbx
          ? ('Host: ' + sbx.host_id + ' • Status: ' + sbx.status + ' • Template: ' + sbx.template + ' • Egress: ' + (sbx.allow_egress ? 'Allowed' : 'Isolated') + ' • Epoch: ' + sbx.epoch)
          : sandboxID;
        if (sbx) {
          document.getElementById('drawer-cpu').value = sbx.cpu || '1';
          document.getElementById('drawer-mem').value = sbx.memory || '1Gi';
          document.getElementById('drawer-idle').value = String(sbx.idle_timeout_sec || 0);
          document.getElementById('drawer-allow-egress').value = sbx.allow_egress ? 'true' : 'false';
        }
        var pPort = document.getElementById('drawer-preview-port').value || '8000';
        document.getElementById('drawer-preview-path-hint').textContent = '/proxy/' + sandboxID + '/' + pPort + '/';
        var btnWrap = document.getElementById('drawer-lifecycle-buttons');
        btnWrap.replaceChildren();
        btnWrap.appendChild(el('button', {
          className: 'btn btn-sm',
          text: 'Suspend',
          onclick: function() { triggerLifecycle(sandboxID, 'suspend'); }
        }));
        btnWrap.appendChild(el('button', {
          className: 'btn btn-sm',
          text: 'Resume',
          onclick: function() { triggerLifecycle(sandboxID, 'resume'); }
        }));
        btnWrap.appendChild(el('button', {
          className: 'btn btn-sm btn-danger',
          text: 'Delete Sandbox',
          onclick: function() {
            openConfirmModal('Delete Sandbox', 'Delete sandbox ' + sandboxID + '?', function() {
              triggerLifecycle(sandboxID, 'delete');
              document.getElementById('sbx-drawer-backdrop').classList.remove('open');
            });
          }
        }));
        document.getElementById('sbx-drawer-backdrop').classList.add('open');
        loadDrawerLogs();
      }

      function loadDrawerLogs() {
        if (!selectedSandboxID) return;
        var tail = document.getElementById('drawer-logs-tail').value || '100';
        apiFetch('/api/sandboxes/logs?sandbox_id=' + encodeURIComponent(selectedSandboxID) + '&tail=' + encodeURIComponent(tail), 'GET')
          .then(function(res) {
            document.getElementById('drawer-logs-output').textContent = res.logs || '(no logs yet)';
          })
          .catch(function(err) {
            document.getElementById('drawer-logs-output').textContent = 'Error loading logs: ' + err.message;
          });
      }

      function triggerLifecycle(sandboxID, action, extra) {
        var payload = { action: action, sandbox_id: sandboxID };
        if (extra) {
          var ks = Object.keys(extra);
          for (var i = 0; i < ks.length; i++) payload[ks[i]] = extra[ks[i]];
        }
        apiFetch('/api/sandboxes/lifecycle', 'POST', payload)
          .then(function() {
            showToast('Action ' + action + ' succeeded for ' + sandboxID, 'success');
            refreshAll();
          })
          .catch(function(err) {
            showToast('Action ' + action + ' failed: ' + err.message, 'error');
          });
      }

      function runHostAction(action, hostID) {
        apiFetch('/api/hosts/action', 'POST', { action: action, host_id: hostID || '' })
          .then(function(res) {
            var msg = (res && res.message) ? res.message : ('Host action ' + action + ' completed');
            showToast(msg, 'success');
            refreshAll();
          })
          .catch(function(err) {
            showToast('Host action failed: ' + err.message, 'error');
          });
      }

      function refreshAll() {
        apiFetch('/api/overview', 'GET')
          .then(function(data) {
            latestOverview = data;
            renderOverview(data);
            renderSandboxes(data);
            renderHosts(data);
            renderConfigAndTemplates(data);
            renderSecurity(data);
            if (selectedSandboxID && document.getElementById('sbx-drawer-backdrop').classList.contains('open')) {
              var sbx = findSandbox(selectedSandboxID);
              if (sbx) {
                document.getElementById('drawer-sbx-subtitle').textContent =
                  'Host: ' + sbx.host_id + ' • Status: ' + sbx.status + ' • Template: ' + sbx.template + ' • Egress: ' + (sbx.allow_egress ? 'Allowed' : 'Isolated') + ' • Epoch: ' + sbx.epoch;
              }
            }
          })
          .catch(function(err) {
            showToast('Failed to refresh overview: ' + err.message, 'error');
          });
      }

      // Navigation tabs
      var navItems = document.querySelectorAll('.gcp-nav-item');
      for (var n = 0; n < navItems.length; n++) {
        navItems[n].addEventListener('click', function() {
          var target = this.getAttribute('data-tab');
          for (var k = 0; k < navItems.length; k++) navItems[k].classList.remove('active');
          this.classList.add('active');
          var tabs = ['overview', 'sandboxes', 'hosts', 'config', 'security'];
          for (var t = 0; t < tabs.length; t++) {
            var sec = document.getElementById('tab-' + tabs[t]);
            if (tabs[t] === target) sec.classList.remove('hidden');
            else sec.classList.add('hidden');
          }
        });
      }

      // Event bindings
      document.getElementById('btn-manual-refresh').addEventListener('click', refreshAll);
      document.getElementById('btn-overview-compact').addEventListener('click', function() { runHostAction('compact'); });
      document.getElementById('btn-hosts-compact').addEventListener('click', function() { runHostAction('compact'); });
      document.getElementById('event-type-filter').addEventListener('change', function() {
        if (latestOverview) renderOverview(latestOverview);
      });
      document.getElementById('sbx-search-input').addEventListener('input', function() {
        if (latestOverview) renderSandboxes(latestOverview);
      });
      document.getElementById('sbx-status-filter').addEventListener('change', function() {
        if (latestOverview) renderSandboxes(latestOverview);
      });
      document.getElementById('sbx-host-filter').addEventListener('input', function() {
        if (latestOverview) renderSandboxes(latestOverview);
      });

      document.getElementById('btn-open-create').addEventListener('click', function() {
        document.getElementById('create-modal-backdrop').classList.add('open');
      });
      document.getElementById('btn-sbx-tab-create').addEventListener('click', function() {
        document.getElementById('create-modal-backdrop').classList.add('open');
      });
      document.getElementById('btn-close-create').addEventListener('click', function() {
        document.getElementById('create-modal-backdrop').classList.remove('open');
      });
      document.getElementById('btn-close-drawer').addEventListener('click', function() {
        document.getElementById('sbx-drawer-backdrop').classList.remove('open');
      });

      document.getElementById('create-sbx-form').addEventListener('submit', function(e) {
        e.preventDefault();
        var idleVal = parseInt(document.getElementById('sbx-idle-timeout').value || '0', 10);
        var payload = {
          name: document.getElementById('create-name').value.trim(),
          template: document.getElementById('create-template').value,
          cpu: document.getElementById('create-cpu').value.trim(),
          memory: document.getElementById('create-mem').value.trim(),
          target_host_id: document.getElementById('create-host').value.trim(),
          from_snapshot: document.getElementById('create-snap').value.trim(),
          idle_timeout_sec: isNaN(idleVal) ? 0 : idleVal,
          persistent: document.getElementById('create-persistent').checked,
          allow_egress: document.getElementById('create-allow-egress').checked
        };
        apiFetch('/api/sandboxes/create', 'POST', payload)
          .then(function(rec) {
            document.getElementById('create-modal-backdrop').classList.remove('open');
            showToast('Created sandbox ' + rec.sandbox_id + ' on ' + rec.host_id, 'success');
            refreshAll();
          })
          .catch(function(err) {
            showToast('Create failed: ' + err.message, 'error');
          });
      });

      document.getElementById('drawer-preview-port').addEventListener('input', function() {
        var pPort = document.getElementById('drawer-preview-port').value || '8000';
        var sid = selectedSandboxID || '<sandbox_id>';
        document.getElementById('drawer-preview-path-hint').textContent = '/proxy/' + sid + '/' + pPort + '/';
      });

      document.getElementById('btn-drawer-open-preview').addEventListener('click', function() {
        if (!selectedSandboxID) return;
        var pPort = parseInt(document.getElementById('drawer-preview-port').value || '8000', 10);
        if (!pPort || pPort < 1 || pPort > 65535) {
          showToast('Invalid preview port (must be 1..65535)', 'error');
          return;
        }
        var url = '/proxy/' + encodeURIComponent(selectedSandboxID) + '/' + String(pPort) + '/';
        window.open(url, '_blank', 'noopener,noreferrer');
      });

      document.getElementById('drawer-exec-form').addEventListener('submit', function(e) {
        e.preventDefault();
        if (!selectedSandboxID) return;
        var cmdStr = document.getElementById('drawer-exec-cmd').value;
        var useStream = document.getElementById('drawer-exec-stream').checked;
        var outBox = document.getElementById('drawer-exec-output');
        var metaBox = document.getElementById('drawer-exec-meta');
        if (useStream && window.ReadableStream && window.TextDecoder) {
          metaBox.textContent = 'Streaming live output (SSE)...';
          outBox.textContent = '';
          fetch('/api/sandboxes/exec-stream', {
            method: 'POST',
            credentials: 'same-origin',
            headers: {
              'Content-Type': 'application/json',
              'X-CSRF-Token': csrfToken
            },
            body: JSON.stringify({
              sandbox_id: selectedSandboxID,
              command: ['sh', '-c', cmdStr],
              stream: true
            })
          }).then(function(resp) {
            if (!resp.ok || !resp.body) {
              throw new Error('HTTP ' + resp.status);
            }
            var reader = resp.body.getReader();
            var decoder = new TextDecoder();
            var buf = '';
            function pump() {
              return reader.read().then(function(res) {
                if (res.done) {
                  if (!outBox.textContent) outBox.textContent = '(empty output)';
                  loadDrawerLogs();
                  return;
                }
                buf += decoder.decode(res.value, { stream: true });
                var lines = buf.split('\n');
                buf = lines.pop() || '';
                for (var i = 0; i < lines.length; i++) {
                  var line = lines[i].trim();
                  if (line.indexOf('data:') === 0) {
                    var jsonStr = line.substring(5).trim();
                    if (!jsonStr) continue;
                    try {
                      var evt = JSON.parse(jsonStr);
                      if (evt.type === 'stdout' || evt.type === 'stderr') {
                        outBox.textContent += (evt.data || '');
                        outBox.scrollTop = outBox.scrollHeight;
                      } else if (evt.type === 'exit') {
                        metaBox.textContent = 'Exit Code: ' + evt.exit_code + ' • Duration: ' + (evt.duration_ms || 0) + ' ms • Host: ' + (evt.host_id || '-');
                      } else if (evt.type === 'error') {
                        metaBox.textContent = 'Stream error: ' + (evt.error || '');
                      }
                    } catch (_e) {}
                  }
                }
                return pump();
              });
            }
            return pump();
          }).catch(function(err) {
            metaBox.textContent = 'Streaming error';
            outBox.textContent = err.message;
          });
          return;
        }
        metaBox.textContent = 'Running command...';
        apiFetch('/api/sandboxes/exec', 'POST', {
          sandbox_id: selectedSandboxID,
          command: ['sh', '-c', cmdStr]
        }).then(function(res) {
          metaBox.textContent =
            'Exit Code: ' + res.exit_code + ' • Duration: ' + res.duration_ms + ' ms • Host: ' + res.host_id;
          var combined = (res.stdout || '') + (res.stderr ? ('\n[stderr]\n' + res.stderr) : '');
          outBox.textContent = combined || '(empty output)';
          loadDrawerLogs();
        }).catch(function(err) {
          metaBox.textContent = 'Execution error';
          outBox.textContent = err.message;
        });
      });

      document.getElementById('btn-drawer-refresh-logs').addEventListener('click', loadDrawerLogs);
      document.getElementById('drawer-logs-tail').addEventListener('change', loadDrawerLogs);

      document.getElementById('btn-drawer-migrate').addEventListener('click', function() {
        if (!selectedSandboxID) return;
        var th = document.getElementById('drawer-migrate-target').value.trim();
        triggerLifecycle(selectedSandboxID, 'migrate', { target_host_id: th });
      });
      document.getElementById('btn-drawer-copy').addEventListener('click', function() {
        if (!selectedSandboxID) return;
        var cname = document.getElementById('drawer-copy-name').value.trim() || (selectedSandboxID + '-clone');
        triggerLifecycle(selectedSandboxID, 'copy', { target_name: cname, persistent: true });
      });
      document.getElementById('btn-drawer-snapshot').addEventListener('click', function() {
        if (!selectedSandboxID) return;
        var sname = document.getElementById('drawer-snap-name').value.trim();
        triggerLifecycle(selectedSandboxID, 'snapshot', { snapshot_id: sname, force: true });
      });

      document.getElementById('drawer-resize-form').addEventListener('submit', function(e) {
        e.preventDefault();
        if (!selectedSandboxID) return;
        apiFetch('/api/sandboxes/update', 'POST', {
          sandbox_id: selectedSandboxID,
          cpu: document.getElementById('drawer-cpu').value.trim(),
          memory: document.getElementById('drawer-mem').value.trim(),
          idle_timeout_sec: parseInt(document.getElementById('drawer-idle').value || '0', 10),
          allow_egress: document.getElementById('drawer-allow-egress').value === 'true'
        }).then(function() {
          showToast('Updated resources & egress for ' + selectedSandboxID, 'success');
          refreshAll();
        }).catch(function(err) {
          showToast('Update failed: ' + err.message, 'error');
        });
      });

      document.getElementById('config-form').addEventListener('submit', function(e) {
        e.preventDefault();
        apiFetch('/api/config', 'POST', {
          max_density: parseInt(document.getElementById('cfg-max-density').value || '10', 10),
          host_cpu: document.getElementById('cfg-host-cpu').value.trim(),
          host_memory: document.getElementById('cfg-host-mem').value.trim(),
          min_free_memory_mb: parseInt(document.getElementById('cfg-min-free-mb').value || '512', 10),
          min_free_memory_pct: parseFloat(document.getElementById('cfg-min-free-pct').value || '15'),
          max_memory_overcommit_ratio: parseFloat(document.getElementById('cfg-max-mem-overcommit').value || '2.0'),
          hot_memory_threshold_pct: parseFloat(document.getElementById('cfg-hot-mem-pct').value || '85'),
          hot_cpu_threshold_pct: parseFloat(document.getElementById('cfg-hot-cpu-pct').value || '90'),
          periodic_sync_sec: parseInt(document.getElementById('cfg-sync-sec').value || '10', 10),
          heartbeat_timeout_sec: parseInt(document.getElementById('cfg-hb-sec').value || '15', 10),
          auto_bin_pack: document.getElementById('cfg-auto-binpack').value === 'true',
          default_idle_timeout_sec: parseInt(document.getElementById('cfg-default-idle-timeout').value || '0', 10),
          target_revision: document.getElementById('cfg-target-rev').value.trim()
        }).then(function() {
          showToast('Saved Fleet & Overload Configuration', 'success');
          refreshAll();
        }).catch(function(err) {
          showToast('Config update failed: ' + err.message, 'error');
        });
      });

      document.getElementById('sec-add-ui-form').addEventListener('submit', function(e) {
        e.preventDefault();
        var member = document.getElementById('sec-add-ui-input').value.trim();
        if (!member) return;
        document.getElementById('sec-add-ui-input').value = '';
        updateSecurityPolicy('add_ui_member', member);
      });

      document.getElementById('sec-add-backend-form').addEventListener('submit', function(e) {
        e.preventDefault();
        var member = document.getElementById('sec-add-backend-input').value.trim();
        if (!member) return;
        document.getElementById('sec-add-backend-input').value = '';
        updateSecurityPolicy('add_backend_member', member);
      });

      document.getElementById('template-create-form').addEventListener('submit', function(e) {
        e.preventDefault();
        apiFetch('/api/templates', 'POST', {
          name: document.getElementById('tpl-name').value.trim(),
          base_template: document.getElementById('tpl-base').value,
          from_sandbox: document.getElementById('tpl-from-sbx').value.trim(),
          snapshot_id: document.getElementById('tpl-from-snap').value.trim(),
          description: document.getElementById('tpl-desc').value.trim(),
          force: true
        }).then(function(tpl) {
          showToast('Registered template ' + tpl.name, 'success');
          document.getElementById('tpl-name').value = '';
          refreshAll();
        }).catch(function(err) {
          showToast('Template creation failed: ' + err.message, 'error');
        });
      });

      refreshAll();
      setInterval(function() {
        var toggle = document.getElementById('auto-refresh-toggle');
        if (toggle && toggle.checked) {
          refreshAll();
        }
      }, 3000);
    })();
  </script>
</body>
</html>
`
