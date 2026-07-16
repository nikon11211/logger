<p align="center">
  <img src="https://raw.githubusercontent.com/nikon11211/logger/main/.github/logo.png" alt="Logger Logo" width="200"/>
</p>

<h1 align="center">Logger - Enterprise-Grade Structured Logger for Go</h1>

<p align="center">
  <a href="https://pkg.go.dev/github.com/nikon11211/logger">
    <img src="https://pkg.go.dev/badge/github.com/nikon11211/logger.svg" alt="Go Reference"/>
  </a>
  <a href="https://goreportcard.com/report/github.com/nikon11211/logger">
    <img src="https://goreportcard.com/badge/github.com/nikon11211/logger" alt="Go Report Card"/>
  </a>
  <a href="https://github.com/nikon11211/logger/actions/workflows/test.yaml">
    <img src="https://github.com/nikon11211/logger/actions/workflows/test.yaml/badge.svg" alt="Tests"/>
  </a>
  <a href="https://codecov.io/gh/nikon11211/logger">
    <img src="https://codecov.io/gh/nikon11211/logger/branch/main/graph/badge.svg" alt="Coverage"/>
  </a>
  <a href="https://opensource.org/licenses/MIT">
    <img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT"/>
  </a>
  <a href="https://golang.org/">
    <img src="https://img.shields.io/badge/Go-%3E%3D%201.21-blue" alt="Go Version"/>
  </a>
</p>

<p align="center">
  <b>A high-performance, production-ready logging library for Go microservices</b><br/>
  <i>Native Kafka integration • OpenTelemetry tracing • GORM support • labstack/echo compatible</i>
</p>

---

## ✨ Why Logger?

Logger is not just another logging library. It's a complete observability solution designed for modern microservices architectures. Built on top of [zerolog](https://github.com/rs/zerolog) for maximum performance, it provides enterprise-grade features out of the box with seamless integration into the Go ecosystem.

```go
// Simple, yet powerful
logger.Info("User logged in successfully")
logger.Error("Failed to connect to database")

// Rich with context
authLogger := logger.WithGroup("auth")
authLogger.InfoCtx(ctx, "Token validated successfully")

// Formatted messages
logger.Infof("Processing order #%d for user %s", orderID, username)
```

## 🎯 Features

<table>
<tr>
<td width="50%">

### 🚀 Performance
- **Zero-allocation** structured logging with [zerolog](https://github.com/rs/zerolog)
- **< 100ns** per log entry (see benchmarks)
- **Lock-free** operations for high-throughput scenarios
- **Batch Kafka** production for optimal network usage

### 📊 Observability
- **Automatic trace context injection** from OpenTelemetry
- **Kafka streaming** with structured JSON format
- **GORM integration** with slow query detection
- **Context propagation** across service boundaries
- **Flexible output formats** (JSON, Console, Colored Console)

</td>
<td width="50%">

### 🔒 Enterprise Ready
- **TLS/SSL support** with custom certificates
- **SASL authentication** (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
- **Multiple partitioner strategies** (UniformBytes, Sticky, RoundRobin)
- **Configurable acknowledgments** (None, Leader, All ISR)
- **Graceful shutdown** with resource cleanup

### 🎨 Developer Experience
- **labstack/echo compatible** - implements `glog.Logger` interface
- **Beautiful console output** with optional colors
- **Type-safe configuration** with validation
- **Context-aware logging** with automatic trace propagation
- **Flexible caller depth** for wrapper functions
- **Comprehensive test coverage** with benchmarks

</td>
</tr>
</table>

## 📦 Installation

```bash
go get github.com/nikon11211/logger
```

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Your Application                           │
├─────────────────────────────────────────────────────────────────┤
│                         Logger Library                          │
│                                                                 │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────────────┐    │
│  │ Console  │ │  Kafka   │ │   GORM   │ │  OpenTelemetry   │    │
│  │ Writer   │ │  Writer  │ │  Logger  │ │     Tracing      │    │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────────┬─────────┘    │
│       └─────────────┴────────────┴───────────────┘              │
│                          │                                      │
│                   MultiLevelWriter                              │
│                          │                                      │
│                    zerolog Core                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 🚀 Quick Start

### Basic Example

```go
package main

import (
    "context"
    "github.com/nikon11211/logger"
)

func main() {
    cfg := logger.DefaultConfig()
    cfg.Module = "my-service"
    cfg.LogLevel = "debug"
    cfg.PrettyPrint = true
    cfg.Color = true
    
    log, err := logger.New(cfg)
    if err != nil {
        panic(err)
    }
    defer log.Close()
    
    // Print beautiful startup banner
    log.AppStats()
    
    // Basic logging
    log.Info("Service started successfully")
    log.Debug("Connecting to database...")
    log.Warn("High memory usage detected")
    
    // Formatted logging
    log.Infof("Connected to %s database", "PostgreSQL")
    
    // Context-aware logging
    ctx := context.Background()
    log.InfoCtx(ctx, "Processing request")
}
```

### Echo Framework Integration

```go
package main

import (
    "github.com/labstack/echo/v4"
    "github.com/nikon11211/logger"
)

func main() {
    cfg := logger.DefaultConfig()
    cfg.Module = "echo-service"
    cfg.PrettyPrint = true
    cfg.Color = true
    
    log, _ := logger.New(cfg)
    defer log.Close()
    
    e := echo.New()
    
    // Logger implements glog.Logger interface
    e.Logger = log
    
    e.GET("/health", func(c echo.Context) error {
        log.InfoCtx(c.Request().Context(), "Health check")
        return c.String(200, "OK")
    })
    
    e.Start(":8080")
}
```

### With Kafka Integration

```go
cfg := logger.Config{
    Module:      "order-service",
    LogLevel:    "info",
    PrettyPrint: true,
    Color:       true,
    TraceEnabled: true,
    KafkaConfig: logger.KafkaConfig{
        ProduceConfig: logger.ProduceConfig{
            Brokers: []string{"kafka-1:9092", "kafka-2:9092"},
            Topic:   "service-logs",
            TLS: logger.TLSConfig{
                Enabled:    true,
                CertFile:   "/certs/client.crt",
                KeyFile:    "/certs/client.key",
                CAFile:     "/certs/ca.crt",
                MinVersion: "1.2",
                MaxVersion: "1.3",
            },
            SASL: logger.SASLConfig{
                Enabled:   true,
                Mechanism: "SCRAM-SHA-512",
                Username:  "my-service",
                Password:  "secure-password",
            },
            Timeout: logger.TimeoutConfig{
                Dial:           30 * time.Second,
                Session:        10 * time.Second,
                ProduceRequest: 5 * time.Second,
            },
        },
        Producer: logger.ProducerConfig{
            Partitioner:   logger.PartitionerUniformBytes,
            RequireAcks:   logger.AckAll,
            Compression:   []logger.CompressionType{logger.CompressionLz4},
            RecordRetries: 10,
            BatchMaxBytes: 1048576,
        },
    },
}
```

## 📚 Advanced Usage

### Distributed Tracing with OpenTelemetry

```go
import (
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/trace"
)

func handleOrder(ctx context.Context, orderID string) {
    tracer := otel.Tracer("order-service")
    ctx, span := tracer.Start(ctx, "handleOrder")
    defer span.End()
    
    // TraceID and SpanID automatically injected into logs
    log.InfoCtx(ctx, "Processing order")
    
    // All downstream logs will include trace context
    processPayment(ctx, orderID)
}
```

### GORM Integration

```go
import (
    "gorm.io/gorm"
    "gorm.io/driver/postgres"
)

func initDatabase(log *logger.Logger) *gorm.DB {
    gormLogger := log.NewGormLogger()
    
    db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
        Logger: gormLogger,
    })
    if err != nil {
        log.Error("Failed to connect to database")
        panic(err)
    }
    
    // Slow queries (>200ms) automatically logged as warnings
    // All queries traced with request context
    return db
}
```

### Kafka Client Logging

```go
import "github.com/twmb/franz-go/pkg/kgo"

func initKafkaConsumer(log *logger.Logger) *kgo.Client {
    kafkaLog := log.NewKafkaLogger("info", "order-consumer")
    
    client, err := kgo.NewClient(
        kgo.SeedBrokers("localhost:9092"),
        kgo.ConsumerGroup("order-processors"),
        kgo.ConsumeTopics("orders"),
        kgo.WithLogger(kafkaLog),
    )
    if err != nil {
        log.Errorf("Failed to create Kafka consumer: %v", err)
        panic(err)
    }
    
    return client
}
```

### Custom Caller Depth for Wrappers

```go
// Utility function that wraps the logger
func logWithMetadata(log *logger.Logger, msg string, metadata map[string]interface{}) {
    // Adjust depth to show actual caller location
    customLogger := log.WithCallerDepth(4)
    
    event := customLogger.Info()
    for k, v := range metadata {
        event.Interface(k, v)
    }
    event.Msg(msg)
}
```

## 📊 API Reference

### Basic Logging Methods

```go
// Simple logging
log.Debug(args ...any)
log.Info(args ...any)
log.Warn(args ...any)
log.Error(args ...any)
log.Fatal(args ...any)
log.Panic(args ...any)

// Formatted logging
log.Debugf(format string, args ...any)
log.Infof(format string, args ...any)
log.Warnf(format string, args ...any)
log.Errorf(format string, args ...any)
log.Fatalf(format string, args ...any)
log.Panicf(format string, args ...any)

// JSON logging (compatible with glog.Logger)
log.Debugj(j glog.JSON)
log.Infoj(j glog.JSON)
log.Warnj(j glog.JSON)
log.Errorj(j glog.JSON)
log.Fatalj(j glog.JSON)
log.Panicj(j glog.JSON)
```

### Context-Aware Methods

```go
// Context logging (automatically injects trace info)
log.DebugCtx(ctx context.Context, msg string)
log.InfoCtx(ctx context.Context, msg string)
log.WarnCtx(ctx context.Context, msg string)
log.ErrorCtx(ctx context.Context, msg string)

// Context logging with formatting
log.DebugCtxf(ctx context.Context, format string, args ...any)
log.InfoCtxf(ctx context.Context, format string, args ...any)
log.WarnCtxf(ctx context.Context, format string, args ...any)
log.ErrorCtxf(ctx context.Context, format string, args ...any)
```

### Utility Methods

```go
// Application banner
log.AppStats()

// Create child loggers
log.WithGroup(name string) *Logger
log.WithContext(ctx context.Context) *Logger
log.WithCallerDepth(depth int) *Logger

// Specialized loggers
log.NewGormLogger() *GormLogger
log.NewKafkaLogger(env, service string) *KafkaLogger
```

## 📊 Benchmarks

```
BenchmarkLoggerDebug-16             50,000,000    25.1 ns/op     0 B/op    0 allocs/op
BenchmarkLoggerInfo-16              50,000,000    24.8 ns/op     0 B/op    0 allocs/op
BenchmarkLoggerError-16             50,000,000    25.2 ns/op     0 B/op    0 allocs/op
BenchmarkLoggerWithContext-16       30,000,000    45.3 ns/op     0 B/op    0 allocs/op
BenchmarkLoggerDebugf-16            20,000,000    89.7 ns/op     0 B/op    0 allocs/op
BenchmarkKafkaWriter-16             10,000,000   156.2 ns/op   128 B/op    2 allocs/op
BenchmarkGormLogger-16              15,000,000    78.4 ns/op     0 B/op    0 allocs/op
```

## 🎨 Output Formats

### JSON Output (Production)
```json
{"level":"info","message":"Service started successfully","module":"user-service","time":"2024-01-15T10:30:00Z"}
{"level":"debug","message":"Processing request","module":"user-service","time":"2024-01-15T10:30:01Z","trace_id":"7ba1b...","span_id":"a5c7e..."}
```

### Console Output (PrettyPrint without Color)
```
2024-01-15T10:30:00Z [INFO] Service started successfully module=user-service
2024-01-15T10:30:01Z [DEBUG] Processing request module=user-service
2024-01-15T10:30:02Z [WARN] Slow query detected module=user-service duration=250ms
```

### Colored Console Output (PrettyPrint with Color)
```
2024-01-15T10:30:00Z INF Service started successfully module=user-service  # Blue
2024-01-15T10:30:01Z DBG Processing request module=user-service            # Green
2024-01-15T10:30:02Z WRN Slow query detected module=user-service           # Yellow
2024-01-15T10:30:03Z ERR Connection timeout module=user-service            # Red
```

### Kafka JSON Format
```json
{
  "level": "info",
  "message": "Order processed successfully",
  "module": "order-service",
  "timestamp": 1705312200000000000,
  "trace_id": "7ba1b3946a8a5c7e8a5c7e8a5c7e8a5c",
  "span_id": "a5c7e8a5c7e8a5c7",
  "caller": "internal/handler/order.go:42"
}
```

## 🔧 Configuration Reference

### YAML Configuration

```yaml
logger:
  module: "user-service"
  log_level: "info"          # debug, info, warn, error
  caller_depth: 3            # Stack frames to skip
  kafka_log_level: "error"   # Kafka client log level
  pretty_print: true         # Human-readable console output
  trace_enabled: true        # OpenTelemetry integration
  color: true                # Colored console output (requires pretty_print)
  
  # GORM Configuration
  gorm_trace: true           # Log all queries
  gorm_slow_query_threshold: 200  # ms
  
  # Kafka Configuration
  kafka_config:
    brokers:
      - "kafka-1:9092"
      - "kafka-2:9092"
    topic: "service-logs"
    
    # TLS Configuration
    tls:
      enabled: true
      min_version: "1.2"
      max_version: "1.3"
      cert_file: "/certs/client.crt"
      key_file: "/certs/client.key"
      ca_file: "/certs/ca.crt"
    
    # SASL Authentication
    sasl:
      enabled: true
      mechanism: "SCRAM-SHA-512"  # PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
      username: "my-service"
      password: "${KAFKA_PASSWORD}"
    
    # Timeouts (in duration format)
    timeout:
      dial: "30s"
      conn_idle: "5m"
      session: "10s"
      produce_request: "5s"
      retry: "3s"
    
    # Producer Configuration
    producer:
      producer_partitioner: "uniform_bytes"
      require_acks: "all"
      compression:
        - "lz4"
        - "snappy"
      record_retries: 10
      producer_batch_max_bytes: 1048576
```

## 🧪 Testing

```bash
# Run all tests
go test ./...

# Run with race detection
go test -race ./...

# Run benchmarks
go test -bench=. -benchmem

# Run with coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out

# Run integration tests (requires Kafka)
INTEGRATION=true go test ./...
```

## 🤝 Contributing

We welcome contributions! Here's how you can help:

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/amazing-feature`)
3. **Commit** your changes (`git commit -m 'Add amazing feature'`)
4. **Push** to the branch (`git push origin feature/amazing-feature`)
5. **Open** a Pull Request

### Development Guidelines
- Write tests for new features
- Follow Go best practices and idioms
- Update documentation as needed
- Ensure all tests pass before submitting PR
- Run `go fmt` and `go vet` on your changes

## 📄 License

MIT License - see [LICENSE](LICENSE) for details.

## 🌟 Show Your Support

Give a ⭐️ if this project helped you! Share it with your team to improve logging across your microservices.

## 🙏 Acknowledgments

- [zerolog](https://github.com/rs/zerolog) - The high-performance logging foundation
- [franz-go](https://github.com/twmb/franz-go) - Excellent Kafka client
- [OpenTelemetry](https://opentelemetry.io/) - Distributed tracing standard
- [GORM](https://gorm.io/) - The fantastic ORM for Go
- [Echo](https://echo.labstack.com/) - High performance, minimalist Go web framework

---

<p align="center">
  <b>Made with ❤️ for the Go community</b><br/>
  <sub>Built for performance, designed for reliability</sub>
</p>