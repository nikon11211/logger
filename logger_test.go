package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm/logger"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	assert.Equal(t, "info", cfg.LogLevel)
	assert.Equal(t, 3, cfg.CallerDepth)
	assert.Equal(t, "error", cfg.KafkaLogLevel)
	assert.True(t, cfg.PrettyPrint)
	assert.False(t, cfg.TraceEnabled)
	assert.False(t, cfg.GormTrace)
	assert.Equal(t, uint(200), cfg.GormSlowQueryThreshold)
	assert.False(t, cfg.Color)
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config",
			cfg: Config{
				Module:                 "test",
				LogLevel:               "info",
				KafkaLogLevel:          "error",
				CallerDepth:            3,
				GormSlowQueryThreshold: 200,
			},
			wantErr: false,
		},
		{
			name: "missing module",
			cfg: Config{
				LogLevel:      "info",
				KafkaLogLevel: "error",
			},
			wantErr: true,
			errMsg:  "module name is required",
		},
		{
			name: "invalid log level",
			cfg: Config{
				Module:        "test",
				LogLevel:      "invalid",
				KafkaLogLevel: "error",
			},
			wantErr: true,
			errMsg:  "invalid log level",
		},
		{
			name: "invalid kafka log level",
			cfg: Config{
				Module:        "test",
				LogLevel:      "info",
				KafkaLogLevel: "invalid",
			},
			wantErr: true,
			errMsg:  "invalid kafka log level",
		},
		{
			name: "negative caller depth",
			cfg: Config{
				Module:        "test",
				LogLevel:      "info",
				KafkaLogLevel: "error",
				CallerDepth:   -1,
			},
			wantErr: true,
			errMsg:  "caller depth must be non-negative",
		},
		{
			name: "kafka config without topic",
			cfg: Config{
				Module:        "test",
				LogLevel:      "info",
				KafkaLogLevel: "error",
				KafkaConfig: KafkaConfig{
					ProduceConfig: ProduceConfig{
						Brokers: []string{"localhost:9092"},
					},
				},
			},
			wantErr: true,
			errMsg:  "kafka topic is required",
		},
		{
			name: "invalid batch max bytes",
			cfg: Config{
				Module:        "test",
				LogLevel:      "info",
				KafkaLogLevel: "error",
				KafkaConfig: KafkaConfig{
					ProduceConfig: ProduceConfig{
						Brokers: []string{"localhost:9092"},
						Topic:   "test",
					},
					Producer: ProducerConfig{
						BatchMaxBytes: -1,
					},
				},
			},
			wantErr: true,
			errMsg:  "batch max bytes must be greater than 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestNewLogger(t *testing.T) {
	cfg := Config{
		Module:                 "test-service",
		LogLevel:               "debug",
		KafkaLogLevel:          "error",
		PrettyPrint:            false,
		CallerDepth:            3,
		GormSlowQueryThreshold: 200,
	}

	logger, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, logger)
	defer logger.Close()

	assert.Equal(t, "test-service", logger.module)
	assert.Equal(t, zerolog.DebugLevel, logger.GetLevel())
}

func TestGlobalLogger(t *testing.T) {
	globalMu.Lock()
	globalLogger = nil
	globalMu.Unlock()

	cfg := Config{
		Module:                 "test",
		LogLevel:               "debug",
		KafkaLogLevel:          "info",
		PrettyPrint:            true,
		CallerDepth:            3,
		GormSlowQueryThreshold: 200,
	}

	logger, err := New(cfg)
	require.NoError(t, err)
	defer logger.Close()

	global, _ := GetLogger()
	assert.NotNil(t, global)
	assert.Equal(t, logger, global)
}

func TestLoggerLevels(t *testing.T) {
	var buf bytes.Buffer

	cfg := Config{
		Module:                 "test",
		LogLevel:               "debug",
		KafkaLogLevel:          "error",
		PrettyPrint:            false,
		GormSlowQueryThreshold: 200,
	}

	logger, err := New(cfg)
	require.NoError(t, err)
	defer logger.Close()

	logger.Logger = zerolog.New(&buf).With().Timestamp().Logger()

	tests := []struct {
		name    string
		logFunc func()
		level   string
		message string
	}{
		{
			name:    "Debug",
			logFunc: func() { logger.Debug("debug message") },
			level:   "debug",
			message: "debug message",
		},
		{
			name:    "Info",
			logFunc: func() { logger.Info("info message") },
			level:   "info",
			message: "info message",
		},
		{
			name:    "Warn",
			logFunc: func() { logger.Warn("warn message") },
			level:   "warn",
			message: "warn message",
		},
		{
			name:    "Error",
			logFunc: func() { logger.Error("error message") },
			level:   "error",
			message: "error message",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf.Reset()
			tt.logFunc()

			var logEntry map[string]interface{}
			err := json.Unmarshal(buf.Bytes(), &logEntry)
			require.NoError(t, err)

			assert.Equal(t, tt.level, logEntry["level"])
			assert.Equal(t, tt.message, logEntry["message"])
		})
	}
}

func TestFormattedLogging(t *testing.T) {
	var buf bytes.Buffer

	cfg := Config{
		Module:                 "test",
		LogLevel:               "debug",
		KafkaLogLevel:          "error",
		PrettyPrint:            false,
		GormSlowQueryThreshold: 200,
	}

	logger, err := New(cfg)
	require.NoError(t, err)
	defer logger.Close()

	logger.Logger = zerolog.New(&buf).With().Timestamp().Logger()

	t.Run("Debugf", func(t *testing.T) {
		buf.Reset()
		logger.Debugf("user %d logged in", 42)

		var logEntry map[string]interface{}
		require.NoError(t, json.Unmarshal(buf.Bytes(), &logEntry))
		assert.Equal(t, "user 42 logged in", logEntry["message"])
	})

	t.Run("Infof", func(t *testing.T) {
		buf.Reset()
		logger.Infof("processing %d items", 100)

		var logEntry map[string]interface{}
		require.NoError(t, json.Unmarshal(buf.Bytes(), &logEntry))
		assert.Equal(t, "processing 100 items", logEntry["message"])
	})

	t.Run("ErrorF", func(t *testing.T) {
		buf.Reset()
		logger.Errorf("connection failed: %v", fmt.Errorf("timeout"))

		var logEntry map[string]interface{}
		require.NoError(t, json.Unmarshal(buf.Bytes(), &logEntry))
		assert.Contains(t, logEntry["message"], "connection failed: timeout")
	})
}

func TestContextLogging(t *testing.T) {
	var buf bytes.Buffer

	cfg := Config{
		Module:                 "test",
		LogLevel:               "debug",
		KafkaLogLevel:          "error",
		PrettyPrint:            false,
		TraceEnabled:           true,
		GormSlowQueryThreshold: 200,
	}

	logger, err := New(cfg)
	require.NoError(t, err)
	defer logger.Close()

	logger.Logger = zerolog.New(&buf).With().Timestamp().Logger()

	ctx := context.Background()
	spanCtx := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:  trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
	})
	ctx = trace.ContextWithSpanContext(ctx, spanCtx)

	t.Run("DebugCtx", func(t *testing.T) {
		buf.Reset()
		logger.DebugCtx(ctx, "debug with ctx")

		var logEntry map[string]interface{}
		require.NoError(t, json.Unmarshal(buf.Bytes(), &logEntry))
		assert.Equal(t, "debug with ctx", logEntry["message"])
		assert.Contains(t, logEntry, "trace_id")
		assert.Contains(t, logEntry, "span_id")
	})
}

func TestWithGroup(t *testing.T) {
	var buf bytes.Buffer

	cfg := Config{
		Module:                 "test",
		LogLevel:               "debug",
		KafkaLogLevel:          "error",
		PrettyPrint:            false,
		GormSlowQueryThreshold: 200,
	}

	logger, err := New(cfg)
	require.NoError(t, err)
	defer logger.Close()

	groupLogger := logger.WithGroup("auth")
	require.NotNil(t, groupLogger)

	groupLogger.Logger = zerolog.New(&buf).With().Timestamp().Str("group", "auth").Logger()

	groupLogger.Info("auth event")

	t.Logf("Log output: %s", buf.String())

	var logEntry map[string]interface{}
	err = json.Unmarshal(buf.Bytes(), &logEntry)
	require.NoError(t, err)

	assert.Equal(t, "auth", logEntry["group"])
	assert.Equal(t, "auth event", logEntry["message"])
}

func TestWithCallerDepth(t *testing.T) {
	cfg := Config{
		Module:                 "test",
		LogLevel:               "debug",
		KafkaLogLevel:          "error",
		PrettyPrint:            false,
		CallerDepth:            2,
		GormSlowQueryThreshold: 200,
	}

	logger, err := New(cfg)
	require.NoError(t, err)
	defer logger.Close()

	t.Run("nil receiver", func(t *testing.T) {
		var nilLogger *Logger
		newLogger := nilLogger.WithCallerDepth(4)
		assert.Nil(t, newLogger)
	})

	t.Run("adjust depth", func(t *testing.T) {
		newLogger := logger.WithCallerDepth(4)
		assert.NotEqual(t, logger, newLogger)
		assert.Equal(t, 4, newLogger.callerDepth)
		assert.Equal(t, 2, logger.callerDepth)
	})
}

func TestAppStats(t *testing.T) {
	var buf bytes.Buffer

	cfg := Config{
		Module:                 "test-service",
		LogLevel:               "debug",
		KafkaLogLevel:          "error",
		PrettyPrint:            false,
		GormSlowQueryThreshold: 200,
	}

	logger, err := New(cfg)
	require.NoError(t, err)
	defer logger.Close()

	logger.Logger = zerolog.New(&buf).With().Timestamp().Logger()
	logger.AppStats()

	output := buf.String()
	assert.Contains(t, output, "test-service")
	assert.Contains(t, output, "debug")
}

func TestGetLevel(t *testing.T) {
	cfg := Config{
		Module:                 "test",
		LogLevel:               "warn",
		KafkaLogLevel:          "error",
		PrettyPrint:            false,
		GormSlowQueryThreshold: 200,
	}

	logger, err := New(cfg)
	require.NoError(t, err)
	defer logger.Close()

	assert.Equal(t, zerolog.WarnLevel, logger.GetLevel())
}

func TestMultiLevelWriter(t *testing.T) {
	var buf1, buf2 bytes.Buffer

	writer1 := zerolog.New(&buf1)
	writer2 := zerolog.New(&buf2)

	multi := NewMultiLevelWriter(writer1, writer2)

	testMsg := []byte("test message")
	n, err := multi.Write(testMsg)
	require.NoError(t, err)
	assert.Equal(t, len(testMsg), n)
	assert.NotEmpty(t, buf1.String())
	assert.NotEmpty(t, buf2.String())
}

func TestNilLoggerSafety(t *testing.T) {
	var nilLogger *Logger

	assert.Nil(t, nilLogger.WithContext(context.Background()))
	assert.Nil(t, nilLogger.WithGroup("test"))
	assert.Nil(t, nilLogger.WithCallerDepth(3))
	assert.Nil(t, nilLogger.NewGormLogger())
	assert.NoError(t, nilLogger.Close())

	assert.NotPanics(t, func() {
		nilLogger.Debug("test")
		nilLogger.Info("test")
		nilLogger.Warn("test")
		nilLogger.Error("test")
	})
}

func TestPartitionerCreation(t *testing.T) {
	tests := []struct {
		name    string
		pt      PartitionerType
		wantErr bool
	}{
		{"UniformBytes", PartitionerUniformBytes, false},
		{"LeastBackup", PartitionerLeastBackup, false},
		{"Manual", PartitionerManual, false},
		{"RoundRobin", PartitionerRoundRobin, false},
		{"StickyKey", PartitionerStickyKey, false},
		{"Sticky", PartitionerSticky, false},
		{"Invalid", "invalid", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			partitioner, err := createPartitioner(tt.pt)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, partitioner)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, partitioner)
			}
		})
	}
}

func TestAcksCreation(t *testing.T) {
	acks := createAcks(AckNone)
	assert.NotNil(t, acks)

	acks = createAcks(AckLeader)
	assert.NotNil(t, acks)

	acks = createAcks(AckAll)
	assert.NotNil(t, acks)
}

func TestCompressionCodecs(t *testing.T) {
	compressions := []CompressionType{
		CompressionLz4,
		CompressionSnappy,
		CompressionGzip,
		CompressionZstd,
		CompressionNone,
	}

	codecs := createCompressionCodecs(compressions)
	assert.Len(t, codecs, 5)
}

func TestTLSVersionParsing(t *testing.T) {
	tests := []struct {
		version string
		wantErr bool
	}{
		{"1.0", false},
		{"1.1", false},
		{"1.2", false},
		{"1.3", false},
		{"1.4", true},
		{"invalid", true},
	}

	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			ver, err := parseTLSVersion(tt.version)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotZero(t, ver)
			}
		})
	}
}

func TestKafkaLogLevelParsing(t *testing.T) {
	tests := []struct {
		level string
	}{
		{"debug"},
		{"info"},
		{"warn"},
		{"warning"},
		{"error"},
		{"unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.level, func(t *testing.T) {
			level := parseKafkaLogLevel(tt.level)
			assert.NotZero(t, level)
		})
	}
}

func TestKafkaLogger(t *testing.T) {
	var buf bytes.Buffer

	kl := &KafkaLogger{
		level:  kgo.LogLevelInfo,
		Logger: zerolog.New(&buf),
	}

	assert.Equal(t, kgo.LogLevelInfo, kl.Level())

	kl.Log(kgo.LogLevelInfo, "test message", "key1", "value1")

	output := buf.String()
	assert.Contains(t, output, "test message")
	assert.Contains(t, output, "key1")
	assert.Contains(t, output, "value1")
}

func TestSetLevel(t *testing.T) {
	cfg := Config{
		Module:                 "test",
		LogLevel:               "info",
		KafkaLogLevel:          "error",
		PrettyPrint:            false,
		GormSlowQueryThreshold: 200,
	}

	logger, err := New(cfg)
	require.NoError(t, err)
	defer logger.Close()

	t.Run("valid level change", func(t *testing.T) {
		err := logger.SetLevel("debug")
		assert.NoError(t, err)
		assert.Equal(t, zerolog.DebugLevel, logger.GetLevel())
	})

	t.Run("invalid level", func(t *testing.T) {
		err := logger.SetLevel("invalid")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid log level")
	})

	t.Run("nil logger", func(t *testing.T) {
		var nilLogger *Logger
		err := nilLogger.SetLevel("debug")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "logger is nil")
	})
}

func TestPrintMethods(t *testing.T) {
	var buf bytes.Buffer

	cfg := Config{
		Module:                 "test",
		LogLevel:               "debug",
		KafkaLogLevel:          "error",
		PrettyPrint:            false,
		GormSlowQueryThreshold: 200,
	}

	logger, err := New(cfg)
	require.NoError(t, err)
	defer logger.Close()

	logger.Logger = zerolog.New(&buf).With().Logger()

	t.Run("Print", func(t *testing.T) {
		buf.Reset()
		logger.Print("test print")
		assert.Contains(t, buf.String(), "test print")
	})

	t.Run("Printf", func(t *testing.T) {
		buf.Reset()
		logger.Printf("formatted %s", "print")
		assert.Contains(t, buf.String(), "formatted print")
	})

	t.Run("Printj", func(t *testing.T) {
		buf.Reset()
		logger.Printj(map[string]interface{}{"key": "value"})
		assert.NotEmpty(t, buf.String())
	})
}

func TestDebugjInfojWarnjErrorj(t *testing.T) {
	var buf bytes.Buffer

	cfg := Config{
		Module:                 "test",
		LogLevel:               "debug",
		KafkaLogLevel:          "error",
		PrettyPrint:            false,
		GormSlowQueryThreshold: 200,
	}

	logger, err := New(cfg)
	require.NoError(t, err)
	defer logger.Close()

	logger.Logger = zerolog.New(&buf).With().Logger()

	t.Run("Debugj", func(t *testing.T) {
		buf.Reset()
		logger.Debugj(map[string]interface{}{"level": "debug"})
		assert.NotEmpty(t, buf.String())
	})

	t.Run("Infoj", func(t *testing.T) {
		buf.Reset()
		logger.Infoj(map[string]interface{}{"level": "info"})
		assert.NotEmpty(t, buf.String())
	})

	t.Run("Warnj", func(t *testing.T) {
		buf.Reset()
		logger.Warnj(map[string]interface{}{"level": "warn"})
		assert.NotEmpty(t, buf.String())
	})

	t.Run("Errorj", func(t *testing.T) {
		buf.Reset()
		logger.Errorj(map[string]interface{}{"level": "error"})
		assert.NotEmpty(t, buf.String())
	})
}

func TestFatalAndPanicMethods(t *testing.T) {
	cfg := Config{
		Module:                 "test",
		LogLevel:               "debug",
		KafkaLogLevel:          "error",
		PrettyPrint:            false,
		GormSlowQueryThreshold: 200,
	}

	logger, err := New(cfg)
	require.NoError(t, err)
	defer logger.Close()

	t.Run("Fatalj", func(t *testing.T) {
		var nilLogger *Logger
		nilLogger.Fatalj(map[string]interface{}{})
	})

	t.Run("Panicj", func(t *testing.T) {
		var nilLogger *Logger
		nilLogger.Panicj(map[string]interface{}{})
	})
}

func TestCtxfMethods(t *testing.T) {
	var buf bytes.Buffer

	cfg := Config{
		Module:                 "test",
		LogLevel:               "debug",
		KafkaLogLevel:          "error",
		PrettyPrint:            false,
		GormSlowQueryThreshold: 200,
	}

	logger, err := New(cfg)
	require.NoError(t, err)
	defer logger.Close()

	logger.Logger = zerolog.New(&buf).With().Logger()
	ctx := context.Background()

	t.Run("DebugCtxf", func(t *testing.T) {
		buf.Reset()
		logger.DebugCtxf(ctx, "debug %s", "test")
		var logEntry map[string]interface{}
		require.NoError(t, json.Unmarshal(buf.Bytes(), &logEntry))
		assert.Equal(t, "debug test", logEntry["message"])
	})

	t.Run("InfoCtxf", func(t *testing.T) {
		buf.Reset()
		logger.InfoCtxf(ctx, "info %s", "test")
		var logEntry map[string]interface{}
		require.NoError(t, json.Unmarshal(buf.Bytes(), &logEntry))
		assert.Equal(t, "info test", logEntry["message"])
	})

	t.Run("WarnCtxf", func(t *testing.T) {
		buf.Reset()
		logger.WarnCtxf(ctx, "warn %s", "test")
		var logEntry map[string]interface{}
		require.NoError(t, json.Unmarshal(buf.Bytes(), &logEntry))
		assert.Equal(t, "warn test", logEntry["message"])
	})

	t.Run("ErrorCtxf", func(t *testing.T) {
		buf.Reset()
		logger.ErrorCtxf(ctx, "error %s", "test")
		var logEntry map[string]interface{}
		require.NoError(t, json.Unmarshal(buf.Bytes(), &logEntry))
		assert.Equal(t, "error test", logEntry["message"])
	})
}

func TestNilLoggerMethods(t *testing.T) {
	var nilLogger *Logger

	t.Run("Print methods", func(t *testing.T) {
		assert.NotPanics(t, func() {
			nilLogger.Print("test")
			nilLogger.Printf("test %s", "arg")
			nilLogger.Printj(map[string]interface{}{})
		})
	})

	t.Run("Debug methods", func(t *testing.T) {
		assert.NotPanics(t, func() {
			nilLogger.Debug("test")
			nilLogger.Debugf("test %s", "arg")
			nilLogger.Debugj(map[string]interface{}{})
		})
	})

	t.Run("Info methods", func(t *testing.T) {
		assert.NotPanics(t, func() {
			nilLogger.Info("test")
			nilLogger.Infof("test %s", "arg")
			nilLogger.Infoj(map[string]interface{}{})
		})
	})

	t.Run("Warn methods", func(t *testing.T) {
		assert.NotPanics(t, func() {
			nilLogger.Warn("test")
			nilLogger.Warnf("test %s", "arg")
			nilLogger.Warnj(map[string]interface{}{})
		})
	})

	t.Run("Error methods", func(t *testing.T) {
		assert.NotPanics(t, func() {
			nilLogger.Error("test")
			nilLogger.Errorf("test %s", "arg")
			nilLogger.Errorj(map[string]interface{}{})
		})
	})

	t.Run("Fatal methods", func(t *testing.T) {
		assert.NotPanics(t, func() {
			nilLogger.Fatalj(map[string]interface{}{})
		})
	})

	t.Run("Panic methods", func(t *testing.T) {
		assert.NotPanics(t, func() {
			nilLogger.Panicj(map[string]interface{}{})
		})
	})

	t.Run("Context methods", func(t *testing.T) {
		ctx := context.Background()
		assert.NotPanics(t, func() {
			nilLogger.DebugCtx(ctx, "test")
			nilLogger.InfoCtx(ctx, "test")
			nilLogger.WarnCtx(ctx, "test")
			nilLogger.ErrorCtx(ctx, "test")
			nilLogger.DebugCtxf(ctx, "test %s", "arg")
			nilLogger.InfoCtxf(ctx, "test %s", "arg")
			nilLogger.WarnCtxf(ctx, "test %s", "arg")
			nilLogger.ErrorCtxf(ctx, "test %s", "arg")
		})
	})
}

func TestSetupConsoleWriter(t *testing.T) {
	t.Run("pretty print with color", func(t *testing.T) {
		cfg := Config{
			PrettyPrint: true,
			Color:       true,
		}
		writers := setupConsoleWriter(cfg)
		assert.Len(t, writers, 1)
	})

	t.Run("pretty print without color", func(t *testing.T) {
		cfg := Config{
			PrettyPrint: true,
			Color:       false,
		}
		writers := setupConsoleWriter(cfg)
		assert.Len(t, writers, 1)
	})

	t.Run("json output", func(t *testing.T) {
		cfg := Config{
			PrettyPrint: false,
		}
		writers := setupConsoleWriter(cfg)
		assert.Len(t, writers, 1)
	})
}

func TestMultiLevelWriterThreadSafety(t *testing.T) {
	multi := NewMultiLevelWriter()

	var buf bytes.Buffer
	writer := zerolog.New(&buf)

	done := make(chan bool)
	go func() {
		for i := 0; i < 100; i++ {
			multi.AddWriter(writer)
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			multi.Write([]byte("test"))
		}
		done <- true
	}()

	<-done
	<-done
}

func TestWriteLevelWithNonLevelWriter(t *testing.T) {
	var buf bytes.Buffer

	multi := NewMultiLevelWriter(&buf)

	n, err := multi.WriteLevel(zerolog.InfoLevel, []byte("test"))
	assert.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, "test", buf.String())
}

func TestNewKafkaLoggerEdgeCases(t *testing.T) {
	cfg := Config{
		Module:                 "test",
		LogLevel:               "debug",
		KafkaLogLevel:          "info",
		PrettyPrint:            false,
		GormSlowQueryThreshold: 200,
	}

	logger, err := New(cfg)
	require.NoError(t, err)
	defer logger.Close()

	t.Run("invalid env level", func(t *testing.T) {
		kl := logger.NewKafkaLogger("invalid", "test-service")
		assert.NotNil(t, kl)
		assert.Equal(t, kgo.LogLevelInfo, kl.Level())
	})

	t.Run("valid warn level", func(t *testing.T) {
		kl := logger.NewKafkaLogger("warn", "test-service")
		assert.NotNil(t, kl)
		assert.Equal(t, kgo.LogLevelWarn, kl.Level())
	})

	t.Run("valid warning level", func(t *testing.T) {
		kl := logger.NewKafkaLogger("warning", "test-service")
		assert.NotNil(t, kl)
		assert.Equal(t, kgo.LogLevelWarn, kl.Level())
	})

	t.Run("nil logger", func(t *testing.T) {
		var nilLogger *Logger
		kl := nilLogger.NewKafkaLogger("info", "test-service")
		assert.NotNil(t, kl)
		assert.Equal(t, kgo.LogLevelInfo, kl.Level())
	})
}

func TestNewLoggerWithInvalidConfig(t *testing.T) {
	cfg := Config{
		Module:                 "",
		LogLevel:               "info",
		KafkaLogLevel:          "error",
		GormSlowQueryThreshold: 200,
	}

	logger, err := New(cfg)
	assert.Error(t, err)
	assert.Nil(t, logger)
	assert.Contains(t, err.Error(), "invalid configuration")
}

func TestNewLoggerWithInvalidLogLevel(t *testing.T) {
	cfg := Config{
		Module:                 "test",
		LogLevel:               "invalid_level_xyz",
		KafkaLogLevel:          "error",
		GormSlowQueryThreshold: 200,
	}

	logger, err := New(cfg)
	assert.Error(t, err)
	assert.Nil(t, logger)
	assert.Contains(t, err.Error(), "invalid configuration: invalid log level: invalid_level_xyz (must be one of: debug, info, warn, error)")
}

func TestWithContextWithoutTraceEnabled(t *testing.T) {
	cfg := Config{
		Module:                 "test",
		LogLevel:               "debug",
		KafkaLogLevel:          "error",
		PrettyPrint:            false,
		TraceEnabled:           false,
		GormSlowQueryThreshold: 200,
	}

	logger, err := New(cfg)
	require.NoError(t, err)
	defer logger.Close()

	ctx := context.Background()
	spanCtx := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:  trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
	})
	ctx = trace.ContextWithSpanContext(ctx, spanCtx)

	newLogger := logger.WithContext(ctx)
	assert.NotNil(t, newLogger)
	assert.Equal(t, logger.module, newLogger.module)
}

func TestKafkaLoggerLogLevelFiltering(t *testing.T) {
	var buf bytes.Buffer

	kl := &KafkaLogger{
		level:  kgo.LogLevelWarn,
		Logger: zerolog.New(&buf),
	}

	kl.Log(kgo.LogLevelDebug, "debug message")
	assert.Empty(t, buf.String())

	kl.Log(kgo.LogLevelInfo, "info message")
	assert.Empty(t, buf.String())

	kl.Log(kgo.LogLevelWarn, "warn message")
	assert.Contains(t, buf.String(), "warn message")

	buf.Reset()
	kl.Log(kgo.LogLevelError, "error message")
	assert.Contains(t, buf.String(), "error message")
}

func TestDefaultSlowThreshold(t *testing.T) {
	cfg := Config{
		Module:                 "test",
		LogLevel:               "debug",
		KafkaLogLevel:          "error",
		PrettyPrint:            false,
		GormTrace:              true,
		GormSlowQueryThreshold: 0,
	}

	logger, err := New(cfg)
	require.NoError(t, err)
	defer logger.Close()

	gormLogger := logger.NewGormLogger()
	assert.NotNil(t, gormLogger)
	assert.Equal(t, defaultSlowThreshold, gormLogger.slowThreshold)
}

func TestKafkaLogEntryMarshal(t *testing.T) {
	entry := KafkaLogEntry{
		Level:     "info",
		Message:   "test message",
		Module:    "test-module",
		Timestamp: time.Now().UnixNano(),
		TraceID:   "trace-123",
		SpanID:    "span-456",
		Caller:    "test.go:42",
	}

	data, err := json.Marshal(entry)
	require.NoError(t, err)

	var decoded KafkaLogEntry
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, entry.Level, decoded.Level)
	assert.Equal(t, entry.Message, decoded.Message)
	assert.Equal(t, entry.Module, decoded.Module)
	assert.Equal(t, entry.TraceID, decoded.TraceID)
	assert.Equal(t, entry.SpanID, decoded.SpanID)
	assert.Equal(t, entry.Caller, decoded.Caller)
}

func TestNewKafkaWriterDefaultTopic(t *testing.T) {
	logger := &Logger{module: "test", config: Config{}}

	kw := newKafkaWriter(logger, nil, "")
	assert.Equal(t, defaultKafkaTopic, kw.topic)
	assert.Equal(t, defaultKafkaTimeout, kw.timeout)
}

func TestNewKafkaWriterCustomTopic(t *testing.T) {
	logger := &Logger{module: "test", config: Config{}}

	kw := newKafkaWriter(logger, nil, "custom-topic")
	assert.Equal(t, "custom-topic", kw.topic)
}

func TestKafkaWriterNilProducer(t *testing.T) {
	logger := &Logger{module: "test", config: Config{}}
	kw := newKafkaWriter(logger, nil, "test-topic")

	n, err := kw.Write([]byte("test message"))
	assert.NoError(t, err)
	assert.Equal(t, 12, n)
}

func TestKafkaWriterInvalidJSON(t *testing.T) {
	logger := &Logger{module: "test", config: Config{}}
	kw := newKafkaWriter(logger, nil, "test-topic")

	kw.producer = nil
	n, err := kw.Write([]byte("not json"))
	assert.NoError(t, err)
	assert.Equal(t, 8, n)
}

func TestKafkaWriterWriteLevel(t *testing.T) {
	logger := &Logger{module: "test", config: Config{}}
	kw := newKafkaWriter(logger, nil, "test-topic")

	n, err := kw.WriteLevel(0, []byte("test"))
	assert.NoError(t, err)
	assert.Equal(t, 4, n)
}

func TestKafkaLoggerNil(t *testing.T) {
	var kl *KafkaLogger
	assert.Equal(t, kgo.LogLevelNone, kl.Level())
}

func TestKafkaLoggerLogWithOddKeyvals(t *testing.T) {
	var buf bytes.Buffer

	kl := &KafkaLogger{
		level:  kgo.LogLevelInfo,
		Logger: zerolog.New(&buf),
	}

	kl.Log(kgo.LogLevelInfo, "test", "key1", "value1", "key2")

	output := buf.String()
	assert.Contains(t, output, "test")
	assert.Contains(t, output, "key1")
	assert.Contains(t, output, "key2")
}

func TestCreateCompressionCodecsInvalid(t *testing.T) {
	compressions := []CompressionType{"invalid"}
	codecs := createCompressionCodecs(compressions)
	assert.Empty(t, codecs)
}

func TestCreateAcksDefault(t *testing.T) {
	acks := createAcks("invalid")
	assert.NotNil(t, acks)
}

func TestBuildTLSDialerWithInvalidMinVersion(t *testing.T) {
	cfg := TLSConfig{
		Enabled:    true,
		MinVersion: "invalid",
	}

	_, err := buildTLSDialer(cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid TLS min version")
}

func TestBuildTLSDialerWithInvalidMaxVersion(t *testing.T) {
	cfg := TLSConfig{
		Enabled:    true,
		MinVersion: "1.2",
		MaxVersion: "invalid",
	}

	_, err := buildTLSDialer(cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid TLS max version")
}

func TestBuildTLSDialerWithInvalidCert(t *testing.T) {
	cfg := TLSConfig{
		Enabled:  true,
		CertFile: "/nonexistent/cert.pem",
		KeyFile:  "/nonexistent/key.pem",
	}

	_, err := buildTLSDialer(cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load client certificate")
}

func TestBuildTLSDialerWithInvalidCAFile(t *testing.T) {
	cfg := TLSConfig{
		Enabled: true,
		CAFile:  "/nonexistent/ca.pem",
	}

	_, err := buildTLSDialer(cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read CA certificate")
}

func TestBuildSASLOptionInvalidMechanism(t *testing.T) {
	cfg := SASLConfig{
		Enabled:   true,
		Mechanism: "INVALID",
	}

	_, err := buildSASLOption(cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported SASL mechanism")
}

func TestBuildTimeoutOptionsZeroValues(t *testing.T) {
	cfg := TimeoutConfig{}
	opts := buildTimeoutOptions(cfg)
	assert.Empty(t, opts)
}

func TestSetupKafkaProducerEmptyBrokers(t *testing.T) {
	cfg := Config{
		Module:        "test",
		KafkaLogLevel: "info",
	}

	producer, err := setupKafkaProducer(cfg)
	assert.NoError(t, err)
	assert.Nil(t, producer)
}

func TestGormLoggerLogMode(t *testing.T) {
	cfg := Config{
		Module:                 "test",
		LogLevel:               "debug",
		KafkaLogLevel:          "error",
		PrettyPrint:            false,
		GormTrace:              true,
		GormSlowQueryThreshold: 200,
	}

	log, err := New(cfg)
	require.NoError(t, err)
	defer log.Close()

	gl := log.NewGormLogger()
	require.NotNil(t, gl)

	result := gl.LogMode(logger.Info)
	assert.Equal(t, gl, result)
}

func TestGormLoggerInfoWarnError(t *testing.T) {
	var buf bytes.Buffer

	cfg := Config{
		Module:                 "test",
		LogLevel:               "debug",
		KafkaLogLevel:          "error",
		PrettyPrint:            false,
		GormTrace:              true,
		GormSlowQueryThreshold: 200,
	}

	log, err := New(cfg)
	require.NoError(t, err)
	defer log.Close()

	gl := log.NewGormLogger()
	require.NotNil(t, gl)

	gl.Logger.Logger = zerolog.New(&buf).With().Timestamp().Logger()

	ctx := context.Background()

	t.Run("Info", func(t *testing.T) {
		buf.Reset()
		gl.Info(ctx, "info %s", "message")

		var logEntry map[string]interface{}
		require.NoError(t, json.Unmarshal(buf.Bytes(), &logEntry))
		assert.Equal(t, "info message", logEntry["message"])
	})

	t.Run("Warn", func(t *testing.T) {
		buf.Reset()
		gl.Warn(ctx, "warn %s", "message")

		var logEntry map[string]interface{}
		require.NoError(t, json.Unmarshal(buf.Bytes(), &logEntry))
		assert.Equal(t, "warn message", logEntry["message"])
	})

	t.Run("Error", func(t *testing.T) {
		buf.Reset()
		gl.Error(ctx, "error %s", "message")

		var logEntry map[string]interface{}
		require.NoError(t, json.Unmarshal(buf.Bytes(), &logEntry))
		assert.Equal(t, "error message", logEntry["message"])
	})
}

func TestGormLoggerTrace(t *testing.T) {
	var buf bytes.Buffer

	cfg := Config{
		Module:                 "test",
		LogLevel:               "debug",
		KafkaLogLevel:          "error",
		PrettyPrint:            false,
		GormTrace:              true,
		GormSlowQueryThreshold: 100,
	}

	log, err := New(cfg)
	require.NoError(t, err)
	defer log.Close()

	gl := log.NewGormLogger()
	require.NotNil(t, gl)

	gl.Logger.Logger = zerolog.New(&buf).With().Timestamp().Logger()
	ctx := context.Background()

	t.Run("ignore trace", func(t *testing.T) {
		glNoTrace := &GormLogger{
			Logger:      gl.Logger,
			ignoreTrace: true,
		}

		buf.Reset()
		glNoTrace.Trace(ctx, time.Now(), func() (string, int64) {
			return "SELECT 1", 1
		}, nil)

		assert.Empty(t, buf.String())
	})

	t.Run("nil fc", func(t *testing.T) {
		buf.Reset()
		gl.Trace(ctx, time.Now(), nil, nil)
		assert.Empty(t, buf.String())
	})

	t.Run("error trace", func(t *testing.T) {
		buf.Reset()
		gl.Trace(ctx, time.Now(), func() (string, int64) {
			return "SELECT * FROM users", 0
		}, assert.AnError)

		var logEntry map[string]interface{}
		require.NoError(t, json.Unmarshal(buf.Bytes(), &logEntry))
		assert.Equal(t, "error", logEntry["level"])
		assert.Contains(t, logEntry["message"], "gorm error")
		assert.Contains(t, logEntry["message"], "SELECT * FROM users")
	})

	t.Run("slow query trace", func(t *testing.T) {
		buf.Reset()
		gl.Trace(ctx, time.Now().Add(-200*time.Millisecond), func() (string, int64) {
			return "SELECT * FROM large_table", 1000
		}, nil)

		var logEntry map[string]interface{}
		require.NoError(t, json.Unmarshal(buf.Bytes(), &logEntry))
		assert.Equal(t, "warn", logEntry["level"])
		assert.Contains(t, logEntry["message"], "slow query")
		assert.Contains(t, logEntry["message"], "SELECT * FROM large_table")
	})

	t.Run("normal trace", func(t *testing.T) {
		buf.Reset()
		gl.Trace(ctx, time.Now(), func() (string, int64) {
			return "SELECT 1", 1
		}, nil)

		var logEntry map[string]interface{}
		require.NoError(t, json.Unmarshal(buf.Bytes(), &logEntry))
		assert.Equal(t, "debug", logEntry["level"])
		assert.Contains(t, logEntry["message"], "query")
		assert.Contains(t, logEntry["message"], "SELECT 1")
	})
}

func BenchmarkLoggerDebug(b *testing.B) {
	cfg := Config{
		Module:                 "bench",
		LogLevel:               "debug",
		KafkaLogLevel:          "error",
		PrettyPrint:            false,
		GormSlowQueryThreshold: 200,
	}

	logger, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer logger.Close()

	logger.Logger = zerolog.New(zerolog.Nop()).With().Logger()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Debug("benchmark message")
	}
}

func BenchmarkLoggerInfo(b *testing.B) {
	cfg := Config{
		Module:                 "bench",
		LogLevel:               "debug",
		KafkaLogLevel:          "error",
		PrettyPrint:            false,
		GormSlowQueryThreshold: 200,
	}

	logger, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer logger.Close()

	logger.Logger = zerolog.New(zerolog.Nop()).With().Logger()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Info("benchmark message")
	}
}

func BenchmarkLoggerDebugF(b *testing.B) {
	cfg := Config{
		Module:                 "bench",
		LogLevel:               "debug",
		KafkaLogLevel:          "error",
		PrettyPrint:            false,
		GormSlowQueryThreshold: 200,
	}

	logger, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer logger.Close()

	logger.Logger = zerolog.New(zerolog.Nop()).With().Logger()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Debugf("benchmark %d", i)
	}
}

func BenchmarkLoggerWithContext(b *testing.B) {
	cfg := Config{
		Module:                 "bench",
		LogLevel:               "debug",
		KafkaLogLevel:          "error",
		PrettyPrint:            false,
		TraceEnabled:           true,
		GormSlowQueryThreshold: 200,
	}

	logger, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer logger.Close()

	logger.Logger = zerolog.New(zerolog.Nop()).With().Logger()

	ctx := context.Background()
	spanCtx := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:  trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
	})
	ctx = trace.ContextWithSpanContext(ctx, spanCtx)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.WithContext(ctx)
	}
}

func TestIntegrationKafkaProducer(t *testing.T) {
	if os.Getenv("INTEGRATION") == "" {
		t.Skip("Skipping integration test")
	}

	cfg := Config{
		Module:                 "integration-test",
		LogLevel:               "debug",
		KafkaLogLevel:          "error",
		PrettyPrint:            true,
		Color:                  true,
		GormSlowQueryThreshold: 200,
		KafkaConfig: KafkaConfig{
			ProduceConfig: ProduceConfig{
				Brokers: []string{"localhost:9092"},
				Topic:   "test-logs",
				Timeout: TimeoutConfig{
					Dial: 30 * time.Second,
				},
			},
			Producer: ProducerConfig{
				Partitioner:   PartitionerUniformBytes,
				RequireAcks:   AckAll,
				Compression:   []CompressionType{CompressionLz4},
				RecordRetries: 3,
				BatchMaxBytes: 1048576,
			},
		},
	}

	logger, err := New(cfg)
	require.NoError(t, err)
	defer logger.Close()

	require.NotNil(t, logger.producer)

	logger.Info("integration test message", "test_001")

	time.Sleep(1 * time.Second)
}
