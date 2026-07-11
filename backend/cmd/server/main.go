package main

//go:generate go run github.com/google/wire/cmd/wire

import (
	"context"
	_ "embed"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	_ "github.com/Wei-Shaw/sub2api/ent/runtime"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/handler"
	"github.com/Wei-Shaw/sub2api/internal/pkg/geminicli"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/setup"
	"github.com/Wei-Shaw/sub2api/internal/web"
	"github.com/koval/agymimic/fingerprint"

	"github.com/gin-gonic/gin"
)

//go:embed VERSION
var embeddedVersion string

// Build-time variables (can be set by ldflags)
var (
	Version   = ""
	Commit    = "unknown"
	Date      = "unknown"
	BuildType = "source" // "source" for manual builds, "release" for CI builds (set by ldflags)
)

func init() {
	// 如果 Version 已通过 ldflags 注入（例如 -X main.Version=...），则不要覆盖。
	if strings.TrimSpace(Version) != "" {
		return
	}

	// 默认从 embedded VERSION 文件读取版本号（编译期打包进二进制）。
	Version = strings.TrimSpace(embeddedVersion)
	if Version == "" {
		Version = "0.0.0-dev"
	}
}

// initLogger configures the default slog handler based on gin.Mode().
// In non-release mode, Debug level logs are enabled.
func main() {
	logger.InitBootstrap()
	defer logger.Sync()

	// Parse command line flags
	setupMode := flag.Bool("setup", false, "Run setup wizard in CLI mode")
	showVersion := flag.Bool("version", false, "Show version information")
	flag.Parse()

	if *showVersion {
		log.Printf("Sub2API %s (commit: %s, built: %s)\n", Version, Commit, Date)
		return
	}

	// CLI setup mode
	if *setupMode {
		if err := setup.RunCLI(); err != nil {
			log.Fatalf("Setup failed: %v", err)
		}
		return
	}

	// Check if setup is needed
	if setup.NeedsSetup() {
		// Check if auto-setup is enabled (for Docker deployment)
		if setup.AutoSetupEnabled() {
			log.Println("Auto setup mode enabled...")
			if err := setup.AutoSetupFromEnv(); err != nil {
				log.Fatalf("Auto setup failed: %v", err)
			}
			// Continue to main server after auto-setup
		} else {
			log.Println("First run detected, starting setup wizard...")
			runSetupServer()
			return
		}
	}

	// Normal server mode
	runMainServer()
}

func runSetupServer() {
	r := gin.New()
	r.Use(middleware.Recovery())
	r.Use(middleware.CORS(config.CORSConfig{}))
	r.Use(middleware.SecurityHeaders(config.CSPConfig{Enabled: true, Policy: config.DefaultCSPPolicy}, nil))

	// Register setup routes
	setup.RegisterRoutes(r)

	// Serve embedded frontend if available
	if web.HasEmbeddedFrontend() {
		r.Use(web.ServeEmbeddedFrontend())
	}

	// Get server address from config.yaml or environment variables (SERVER_HOST, SERVER_PORT)
	// This allows users to run setup on a different address if needed
	addr := config.GetServerAddress()
	log.Printf("Setup wizard available at http://%s", addr)
	log.Println("Complete the setup wizard to configure Sub2API")

	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	server := &http.Server{
		Addr:              addr,
		Handler:           r,
		ReadHeaderTimeout: 30 * time.Second,
		IdleTimeout:       120 * time.Second,
		Protocols:         protocols,
	}

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("Failed to start setup server: %v", err)
	}
}

func runMainServer() {
	cfg, err := config.LoadForBootstrap()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	if err := logger.Init(logger.OptionsFromConfig(cfg.Log)); err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}
	if cfg.RunMode == config.RunModeSimple {
		log.Println("⚠️  WARNING: Running in SIMPLE mode - billing and quota checks are DISABLED")
	}

	buildInfo := handler.BuildInfo{
		Version:   Version,
		BuildType: BuildType,
	}

	app, err := initializeApplication(buildInfo)
	if err != nil {
		log.Fatalf("Failed to initialize application: %v", err)
	}
	defer app.Cleanup()

	// Long-lived background context for periodic workers (gemini-cli version
	// poller, etc). Cancelled on shutdown so goroutines exit cleanly before
	// the process closes its TCP listeners.
	bgCtx, bgCancel := context.WithCancel(context.Background())
	defer bgCancel()

	// Track the upstream @google/gemini-cli release so impersonation UA stays
	// current without redeploys.
	geminicli.StartVersionAutoUpdater(bgCtx, geminicli.DefaultAutoUpdateInterval)

	// Track the live agy.exe identity (CLI version + Go build string) so
	// the antigravity_native backend's User-Agent and Unleash metrics
	// platformVersion always match what real agy.exe currently advertises.
	// Polls Google's auto-updater every 6h; one-shot eager refresh at
	// startup populates the cache before the first chat request.
	// Note: we pin platform="windows_amd64" because that manifest serves a
	// raw .exe (debug/buildinfo can read it directly). darwin/arm64 ships
	// as .tar.gz — same Go build string inside, but extra extraction step
	// we don't need for fingerprinting. The advertised UA tail stays
	// darwin/arm64 (set by internal/headers.go) regardless of what platform
	// we probe; this only controls which binary we open for buildinfo.
	fpRefresher := fingerprint.Install(bgCtx, fingerprint.Options{
		Platform: "windows_amd64",
		CacheDir: filepath.Join(os.TempDir(), "sub2api-agy-fingerprint"),
		Logger:   func(f string, a ...any) { log.Printf("[fingerprint] "+f, a...) },
	})
	defer fpRefresher.Stop()

	// Chat history logger — async writer with daily rotation + 500 MB
	// total size cap. Captures upstream Gemini-format request/response
	// pairs per native antigravity request for post-hoc diagnosis. Safe
	// to start unconditionally; the service no-ops when global setting
	// is disabled. Stop on shutdown to drain in-flight entries.
	if app.ChatHistory != nil {
		app.ChatHistory.Start(bgCtx)
		defer app.ChatHistory.Stop()
	}

	// 启动服务器
	go func() {
		if err := app.Server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	log.Printf("Server started on %s", app.Server.Addr)

	// 等待中断信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := app.Server.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exited")
}
