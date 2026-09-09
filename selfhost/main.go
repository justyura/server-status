package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

const maxPayload = 32 << 20

type config struct {
	listen       string
	origin       *url.URL
	token        string
	staticDir    string
	pollInterval time.Duration
	publicBase   string
}

type hub struct {
	cfg     config
	client  *http.Client
	mu      sync.RWMutex
	payload []byte
	at      time.Time
	clients map[*wsClient]struct{}
	wake    chan struct{}
}

type wsClient struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func (c *wsClient) write(payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return c.conn.WriteMessage(websocket.TextMessage, payload)
}

type server struct {
	cfg config
	hub *hub
	up  websocket.Upgrader
}

func getenv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func loadConfig() (config, error) {
	origin, err := url.Parse(getenv("MMWX_ORIGIN", "http://127.0.0.1:12889"))
	if err != nil || origin.Host == "" {
		return config{}, errors.New("MMWX_ORIGIN is invalid")
	}
	seconds, err := strconv.Atoi(getenv("PROBE_POLL_INTERVAL_SECONDS", "3"))
	if err != nil || seconds < 3 || seconds > 60 {
		return config{}, errors.New("PROBE_POLL_INTERVAL_SECONDS must be between 3 and 60")
	}
	cfg := config{
		listen:       getenv("LISTEN_ADDR", "127.0.0.1:12890"),
		origin:       origin,
		token:        strings.TrimSpace(os.Getenv("PROBE_TOKEN")),
		staticDir:    getenv("STATIC_DIR", "./dist"),
		pollInterval: time.Duration(seconds) * time.Second,
		publicBase:   strings.TrimSuffix(getenv("PUBLIC_BASE_PATH", "/probe"), "/"),
	}
	if cfg.token == "" {
		return config{}, errors.New("PROBE_TOKEN is required")
	}
	return cfg, nil
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}
	h := &hub{
		cfg:     cfg,
		client:  &http.Client{Timeout: 15 * time.Second},
		clients: make(map[*wsClient]struct{}),
		wake:    make(chan struct{}, 1),
	}
	s := &server{
		cfg: cfg,
		hub: h,
		up: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				origin := r.Header.Get("Origin")
				if origin == "" {
					return true
				}
				u, err := url.Parse(origin)
				return err == nil && strings.EqualFold(u.Host, r.Host)
			},
		},
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	go h.run(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.health)
	mux.HandleFunc("/api/probe", s.probe)
	mux.HandleFunc("/api/series", s.series)
	mux.HandleFunc("/api/stream", s.stream)
	mux.HandleFunc("/api/theme-config", s.themeConfig)
	mux.HandleFunc("/api/visitor", s.visitor)
	mux.HandleFunc("/login", s.login)
	mux.HandleFunc("/", s.assets)

	httpServer := &http.Server{
		Addr:              cfg.listen,
		Handler:           securityHeaders(mux),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       90 * time.Second,
	}
	go func() {
		log.Printf("Wtyura Probe listening on %s, serving %s", cfg.listen, cfg.staticDir)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	<-ctx.Done()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)
	h.closeClients()
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
		next.ServeHTTP(w, r)
	})
}

func (h *hub) run(ctx context.Context) {
	ticker := time.NewTicker(h.cfg.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.refresh(ctx)
		case <-h.wake:
			h.refresh(ctx)
		}
	}
}

func (h *hub) request(ctx context.Context, path, rawQuery string) (*http.Response, error) {
	target := *h.cfg.origin
	target.Path = path
	target.RawQuery = rawQuery
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-MMwx-Probe-Token", h.cfg.token)
	return h.client.Do(req)
}

func (h *hub) refresh(ctx context.Context) {
	resp, err := h.request(ctx, "/api/public/probe-servers", "")
	if err != nil {
		log.Printf("snapshot request: %v", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("snapshot returned %d", resp.StatusCode)
		return
	}
	payload, err := io.ReadAll(io.LimitReader(resp.Body, maxPayload+1))
	if err != nil || len(payload) == 0 || len(payload) > maxPayload || !json.Valid(payload) {
		log.Printf("invalid snapshot payload: size=%d err=%v", len(payload), err)
		return
	}
	h.mu.Lock()
	h.payload = append(h.payload[:0], payload...)
	h.at = time.Now()
	clients := make([]*wsClient, 0, len(h.clients))
	for conn := range h.clients {
		clients = append(clients, conn)
	}
	h.mu.Unlock()
	for _, conn := range clients {
		if err := conn.write(payload); err != nil {
			h.remove(conn)
		}
	}
}

func (h *hub) snapshot(ctx context.Context) ([]byte, time.Time, error) {
	h.mu.RLock()
	payload, at := append([]byte(nil), h.payload...), h.at
	h.mu.RUnlock()
	if len(payload) > 0 && time.Since(at) < 12*time.Second {
		return payload, at, nil
	}
	h.refresh(ctx)
	h.mu.RLock()
	defer h.mu.RUnlock()
	if len(h.payload) == 0 {
		return nil, time.Time{}, errors.New("snapshot unavailable")
	}
	return append([]byte(nil), h.payload...), h.at, nil
}

func (h *hub) add(conn *wsClient) {
	h.mu.Lock()
	h.clients[conn] = struct{}{}
	h.mu.Unlock()
	select {
	case h.wake <- struct{}{}:
	default:
	}
}

func (h *hub) remove(conn *wsClient) {
	h.mu.Lock()
	if _, ok := h.clients[conn]; ok {
		delete(h.clients, conn)
		_ = conn.conn.Close()
	}
	h.mu.Unlock()
}

func (h *hub) closeClients() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for conn := range h.clients {
		_ = conn.conn.Close()
	}
	h.clients = make(map[*wsClient]struct{})
}

func onlyGet(w http.ResponseWriter, r *http.Request) bool {
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		return true
	}
	http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	return false
}

func (s *server) health(w http.ResponseWriter, r *http.Request) {
	if !onlyGet(w, r) {
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, "ok\n")
}

func (s *server) probe(w http.ResponseWriter, r *http.Request) {
	if !onlyGet(w, r) {
		return
	}
	payload, _, err := s.hub.snapshot(r.Context())
	if err != nil {
		http.Error(w, "Probe snapshot unavailable", http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("X-Probe-Source", "selfhost-hub")
	w.Header().Set("X-Probe-Cache", "HIT")
	_, _ = w.Write(payload)
}

func (s *server) series(w http.ResponseWriter, r *http.Request) {
	if !onlyGet(w, r) {
		return
	}
	resp, err := s.hub.request(r.Context(), "/api/public/probe-series", r.URL.RawQuery)
	if err != nil {
		http.Error(w, "Upstream unavailable", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
	w.Header().Set("Cache-Control", "private, no-store")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, io.LimitReader(resp.Body, maxPayload))
}

func (s *server) stream(w http.ResponseWriter, r *http.Request) {
	conn, err := s.up.Upgrade(w, r, http.Header{"X-Probe-Hub": []string{"selfhost-shared"}})
	if err != nil {
		return
	}
	client := &wsClient{conn: conn}
	s.hub.add(client)
	defer s.hub.remove(client)
	if payload, _, err := s.hub.snapshot(r.Context()); err == nil {
		_ = client.write(payload)
	}
	conn.SetReadLimit(1024)
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}

func (s *server) themeConfig(w http.ResponseWriter, r *http.Request) {
	if !onlyGet(w, r) {
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=60")
	_, _ = io.WriteString(w, `{}`)
}

func clientIP(r *http.Request) string {
	if value := strings.TrimSpace(strings.Split(r.Header.Get("X-Forwarded-For"), ",")[0]); value != "" {
		return value
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	return r.RemoteAddr
}

func (s *server) visitor(w http.ResponseWriter, r *http.Request) {
	if !onlyGet(w, r) {
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "private, no-store")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ip": clientIP(r), "risk": nil, "proxy": "unknown", "type": "",
	})
}

func (s *server) login(w http.ResponseWriter, r *http.Request) {
	if !onlyGet(w, r) {
		return
	}
	payload, _, err := s.hub.snapshot(r.Context())
	if err != nil {
		http.Redirect(w, r, s.cfg.publicBase+"/", http.StatusFound)
		return
	}
	var settings struct {
		BlockLogin bool `json:"block_login"`
	}
	if json.Unmarshal(payload, &settings) != nil || settings.BlockLogin {
		http.Redirect(w, r, s.cfg.publicBase+"/", http.StatusFound)
		return
	}
	target := *s.cfg.origin
	target.Path = "/login"
	target.RawQuery = ""
	http.Redirect(w, r, target.String(), http.StatusFound)
}

func (s *server) assets(w http.ResponseWriter, r *http.Request) {
	if !onlyGet(w, r) {
		return
	}
	path := filepath.Clean(strings.TrimPrefix(r.URL.Path, "/"))
	if path == "." || path == "" {
		path = "index.html"
	}
	full := filepath.Join(s.cfg.staticDir, path)
	root, _ := filepath.Abs(s.cfg.staticDir)
	abs, _ := filepath.Abs(full)
	if !strings.HasPrefix(abs, root+string(os.PathSeparator)) && abs != root {
		http.Error(w, "Not found", http.StatusNotFound)
		return
	}
	info, err := os.Stat(full)
	if err != nil || info.IsDir() {
		full = filepath.Join(s.cfg.staticDir, "index.html")
	}
	if ext := filepath.Ext(full); ext != "" {
		if kind := mime.TypeByExtension(ext); kind != "" {
			w.Header().Set("Content-Type", kind)
		}
	}
	if strings.Contains(filepath.Base(full), "-") && filepath.Ext(full) != ".html" {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		w.Header().Set("Cache-Control", "no-cache")
	}
	http.ServeFile(w, r, full)
}

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.LUTC | log.Lmsgprefix)
	log.SetPrefix("wtyura-probe: ")
}
