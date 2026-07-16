package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nikon11211/logger"
)

func main() {
	cfg := logger.Config{
		Module:                 "auth",
		LogLevel:               "debug",
		CallerDepth:            3,
		KafkaLogLevel:          "error",
		PrettyPrint:            true,
		TraceEnabled:           true,
		GormTrace:              true,
		GormSlowQueryThreshold: 200,
		Color:                  true,
		KafkaConfig: logger.KafkaConfig{
			ProduceConfig: logger.ProduceConfig{
				Brokers: []string{"localhost:9092"},
				Topic:   "system-logs",
				TLS: logger.TLSConfig{
					Enabled:    false,
					MinVersion: "1.2",
					MaxVersion: "1.3",
				},
				SASL: logger.SASLConfig{
					Enabled:   false,
					Mechanism: "SCRAM-SHA-512",
				},
				Timeout: logger.TimeoutConfig{
					Dial:           30 * time.Second,
					ConnIdle:       5 * time.Minute,
					Session:        10 * time.Second,
					ProduceRequest: 5 * time.Second,
				},
			},
			Producer: logger.ProducerConfig{
				Partitioner:   logger.PartitionerUniformBytes,
				RequireAcks:   logger.AckAll,
				Compression:   []logger.CompressionType{logger.CompressionLz4, logger.CompressionSnappy},
				RecordRetries: 10,
				BatchMaxBytes: 1048576,
			},
		},
	}

	log, err := logger.New(cfg)
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}
	defer func(log *logger.Logger) {
		err := log.Close()
		if err != nil {

		}
	}(log)

	log.AppStats()

	ctx := context.Background()

	log.DebugCtx(ctx, "Initializing database connection pool")
	log.InfoCtx(ctx, "Database connection established")
	log.WarnCtx(ctx, "High memory usage detected: 85%")

	userID := 42
	log.InfoCtxf(ctx, "User %d profile updated successfully", userID)

	if err := simulateDatabaseError(); err != nil {
		log.ErrorCtxf(ctx, "Database operation failed: %v", err)
	}

	authLogger := log.WithGroup("auth")
	authLogger.InfoCtx(ctx, "User authentication started")
	authLogger.InfoCtxf(ctx, "Token generated for user %d", userID)

	server := &http.Server{
		Addr:         ":8080",
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
	}

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		log.InfoCtx(r.Context(), "Health check endpoint called")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{"status": "healthy", "timestamp": "` + time.Now().Format(time.RFC3339) + `"}`))
		if err != nil {
			return
		}
	})

	http.HandleFunc("/api/users", func(w http.ResponseWriter, r *http.Request) {
		log.InfoCtxf(r.Context(), "API request: %s %s", r.Method, r.URL.Path)

		time.Sleep(100 * time.Millisecond)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{"users": []}`))
		if err != nil {
			return
		}

		log.DebugCtxf(r.Context(), "API request completed in %v", 100*time.Millisecond)
	})

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.InfoCtx(ctx, "HTTP server starting on :8080")
		log.InfoCtx(ctx, "Health check available at /health")
		log.InfoCtx(ctx, "API endpoint available at /api/users")

		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.ErrorCtxf(ctx, "HTTP server error: %v", err)
		}
	}()

	<-quit
	log.WarnCtx(ctx, "Received shutdown signal, starting graceful shutdown...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.ErrorCtxf(ctx, "Server forced to shutdown: %v", err)
	}

	log.InfoCtx(ctx, "Server stopped successfully")
	log.InfoCtx(ctx, "All connections drained, exiting")
}

func simulateDatabaseError() error {
	return fmt.Errorf("connection timeout after 30s: dial tcp 10.0.0.1:5432: i/o timeout")
}
