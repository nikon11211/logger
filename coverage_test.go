package logger

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

type mockProducer struct {
	produceErr error
	closed     bool
	lastRecord *kgo.Record
}

func (m *mockProducer) ProduceSync(_ context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	if len(rs) > 0 {
		m.lastRecord = rs[0]
	}
	return kgo.ProduceResults{{Record: rs[0], Err: m.produceErr}}
}

func (m *mockProducer) Close() {
	m.closed = true
}

type failingWriter struct {
	err error
}

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

type levelTestWriter struct {
	buf      bytes.Buffer
	writeErr error
}

func (w *levelTestWriter) Write(p []byte) (int, error) {
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	return w.buf.Write(p)
}

func (w *levelTestWriter) WriteLevel(_ zerolog.Level, p []byte) (int, error) {
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	return w.buf.Write(p)
}

func TestMultiLevelWriterWriteError(t *testing.T) {
	multi := NewMultiLevelWriter(failingWriter{err: errors.New("write failed")})

	n, err := multi.Write([]byte("test"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "write failed")
	assert.Equal(t, 0, n)
}

func TestMultiLevelWriterWriteLevelLevelWriter(t *testing.T) {
	lw := &levelTestWriter{}
	multi := NewMultiLevelWriter(lw)

	n, err := multi.WriteLevel(zerolog.InfoLevel, []byte("leveled"))
	assert.NoError(t, err)
	assert.Equal(t, 7, n)
	assert.Equal(t, "leveled", lw.buf.String())
}

func TestMultiLevelWriterWriteLevelErrors(t *testing.T) {
	t.Run("level writer error", func(t *testing.T) {
		multi := NewMultiLevelWriter(&levelTestWriter{writeErr: errors.New("level fail")})
		n, err := multi.WriteLevel(zerolog.InfoLevel, []byte("test"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "level fail")
		assert.Equal(t, 0, n)
	})

	t.Run("plain writer error", func(t *testing.T) {
		multi := NewMultiLevelWriter(failingWriter{err: errors.New("plain fail")})
		n, err := multi.WriteLevel(zerolog.InfoLevel, []byte("test"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "plain fail")
		assert.Equal(t, 0, n)
	})
}

func TestGetLoggerNotInitialized(t *testing.T) {
	globalMu.Lock()
	globalLogger = nil
	globalMu.Unlock()

	_, err := GetLogger()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "logger not initialized")
}

func TestNewDefaultCallerDepth(t *testing.T) {
	cfg := Config{
		Module:        "test",
		LogLevel:      "info",
		KafkaLogLevel: "error",
	}

	l, err := New(cfg)
	require.NoError(t, err)
	defer l.Close()

	assert.Equal(t, 3, l.callerDepth)
	assert.Equal(t, "info", l.config.LogLevel)
}

func TestNewKafkaSetupError(t *testing.T) {
	cfg := Config{
		Module:        "test",
		LogLevel:      "info",
		KafkaLogLevel: "error",
		KafkaConfig: KafkaConfig{
			ProduceConfig: ProduceConfig{
				Brokers: []string{"localhost:9092"},
				Topic:   "test-topic",
			},
			Producer: ProducerConfig{
				Partitioner:   "invalid-partitioner",
				RequireAcks:   AckAll,
				Compression:   []CompressionType{CompressionLz4},
				RecordRetries: 3,
				BatchMaxBytes: 1048576,
			},
		},
	}

	l, err := New(cfg)
	assert.Error(t, err)
	assert.Nil(t, l)
	assert.Contains(t, err.Error(), "failed to setup kafka producer")
}

func TestNewWithKafkaProducer(t *testing.T) {
	cfg := Config{
		Module:        "test",
		LogLevel:      "info",
		KafkaLogLevel: "error",
		KafkaConfig: KafkaConfig{
			ProduceConfig: ProduceConfig{
				Brokers: []string{"localhost:9092"},
				Topic:   "test-topic",
			},
			Producer: ProducerConfig{
				Partitioner:   PartitionerManual,
				RequireAcks:   AckAll,
				RecordRetries: 3,
				BatchMaxBytes: 1048576,
			},
		},
	}

	l, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, l.producer)

	assert.NoError(t, l.Close())
}

func TestCloseWithProducer(t *testing.T) {
	mock := &mockProducer{}
	l := &Logger{
		Logger:   zerolog.New(zerolog.Nop()),
		producer: mock,
		config:   Config{},
	}

	assert.NoError(t, l.Close())
	assert.True(t, mock.closed)
}

func TestWarnf(t *testing.T) {
	var buf bytes.Buffer

	cfg := Config{
		Module:        "test",
		LogLevel:      "debug",
		KafkaLogLevel: "error",
		PrettyPrint:   false,
	}
	l, err := New(cfg)
	require.NoError(t, err)
	defer l.Close()

	l.Logger = zerolog.New(&buf).With().Timestamp().Logger()

	l.Warnf("warning %s", "message")

	var logEntry map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &logEntry))
	assert.Equal(t, "warning message", logEntry["message"])
	assert.Equal(t, "warn", logEntry["level"])
}

func TestFatalMethods(t *testing.T) {
	orig := zerolog.FatalExitFunc
	zerolog.FatalExitFunc = func() {}
	t.Cleanup(func() { zerolog.FatalExitFunc = orig })

	cfg := Config{
		Module:        "test",
		LogLevel:      "debug",
		KafkaLogLevel: "error",
		PrettyPrint:   false,
	}
	l, err := New(cfg)
	require.NoError(t, err)
	defer l.Close()

	t.Run("Fatal", func(t *testing.T) {
		var buf bytes.Buffer
		l.Logger = zerolog.New(&buf).With().Timestamp().Logger()
		l.Fatal("fatal message")
		var logEntry map[string]any
		require.NoError(t, json.Unmarshal(buf.Bytes(), &logEntry))
		assert.Equal(t, "fatal message", logEntry["message"])
		assert.Equal(t, "fatal", logEntry["level"])
	})

	t.Run("Fatalf", func(t *testing.T) {
		var buf bytes.Buffer
		l.Logger = zerolog.New(&buf).With().Timestamp().Logger()
		l.Fatalf("fatal %s", "formatted")
		var logEntry map[string]any
		require.NoError(t, json.Unmarshal(buf.Bytes(), &logEntry))
		assert.Equal(t, "fatal formatted", logEntry["message"])
	})

	t.Run("Fatalj", func(t *testing.T) {
		var buf bytes.Buffer
		l.Logger = zerolog.New(&buf).With().Logger()
		l.Fatalj(map[string]any{"fatal": true})
		assert.NotEmpty(t, buf.String())
	})

	t.Run("nil logger", func(t *testing.T) {
		var nilLogger *Logger
		assert.NotPanics(t, func() {
			nilLogger.Fatal("x")
			nilLogger.Fatalf("x %s", "y")
			nilLogger.Fatalj(map[string]any{})
		})
	})
}

func TestPanicMethods(t *testing.T) {
	cfg := Config{
		Module:        "test",
		LogLevel:      "debug",
		KafkaLogLevel: "error",
		PrettyPrint:   false,
	}
	l, err := New(cfg)
	require.NoError(t, err)
	defer l.Close()

	t.Run("Panic", func(t *testing.T) {
		var buf bytes.Buffer
		l.Logger = zerolog.New(&buf).With().Logger()

		assert.Panics(t, func() {
			l.Panic("panic message")
		})
		assert.NotEmpty(t, buf.String())
	})

	t.Run("Panicf", func(t *testing.T) {
		var buf bytes.Buffer
		l.Logger = zerolog.New(&buf).With().Logger()

		assert.Panics(t, func() {
			l.Panicf("panic %s", "formatted")
		})
		assert.NotEmpty(t, buf.String())
	})

	t.Run("Panicj", func(t *testing.T) {
		var buf bytes.Buffer
		l.Logger = zerolog.New(&buf).With().Logger()

		assert.NotPanics(t, func() {
			l.Panicj(map[string]any{"panic": true})
		})
		assert.NotEmpty(t, buf.String())
	})

	t.Run("nil logger", func(t *testing.T) {
		var nilLogger *Logger
		assert.NotPanics(t, func() {
			nilLogger.Panic("x")
			nilLogger.Panicf("x %s", "y")
			nilLogger.Panicj(map[string]any{})
		})
	})
}

func TestAppStatsNil(t *testing.T) {
	var nilLogger *Logger
	nilLogger.AppStats()
}

func TestSetupConsoleWriterOutput(t *testing.T) {
	writers := setupConsoleWriter(Config{PrettyPrint: true, Color: false})
	require.Len(t, writers, 1)

	_, err := writers[0].Write([]byte(`{"level":"info","time":"2024-01-01T00:00:00Z","message":"hello"}`))
	assert.NoError(t, err)

	_, err = writers[0].Write([]byte(`{"level":"trace","time":"2024-01-01T00:00:00Z","message":"trace msg"}`))
	assert.NoError(t, err)
}

func TestValidateNegativeRetries(t *testing.T) {
	cfg := Config{
		Module:        "test",
		LogLevel:      "info",
		KafkaLogLevel: "error",
		KafkaConfig: KafkaConfig{
			ProduceConfig: ProduceConfig{
				Brokers: []string{"localhost:9092"},
				Topic:   "test-topic",
			},
			Producer: ProducerConfig{
				RecordRetries: -1,
				BatchMaxBytes: 1048576,
			},
		},
	}

	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "record retries must be non-negative")
}

func TestKafkaWriterWriteSuccess(t *testing.T) {
	mock := &mockProducer{}
	l := &Logger{module: "test-mod", config: Config{TraceEnabled: true}}
	kw := &kafkaWriter{logger: l, producer: mock, timeout: time.Second, topic: "test-topic"}

	p := []byte(`{"level":"warn","message":"hello world","caller":"x.go:1","trace_id":"tr-1","span_id":"sp-1"}`)
	n, err := kw.Write(p)
	assert.NoError(t, err)
	assert.Equal(t, len(p), n)

	require.NotNil(t, mock.lastRecord)
	assert.Equal(t, "test-topic", mock.lastRecord.Topic)

	var entry KafkaLogEntry
	require.NoError(t, json.Unmarshal(mock.lastRecord.Value, &entry))
	assert.Equal(t, "warn", entry.Level)
	assert.Equal(t, "hello world", entry.Message)
	assert.Equal(t, "test-mod", entry.Module)
	assert.Equal(t, "tr-1", entry.TraceID)
	assert.Equal(t, "sp-1", entry.SpanID)
	assert.Equal(t, "x.go:1", entry.Caller)
	assert.NotZero(t, entry.Timestamp)
}

func TestKafkaWriterWriteDefaults(t *testing.T) {
	mock := &mockProducer{}
	l := &Logger{module: "test-mod", config: Config{TraceEnabled: true}}
	kw := &kafkaWriter{logger: l, producer: mock, timeout: time.Second, topic: "test-topic"}

	p := []byte(`{"other":"value","trace_id":"tr-1","span_id":"sp-1"}`)
	n, err := kw.Write(p)
	assert.NoError(t, err)
	assert.Equal(t, len(p), n)

	var entry KafkaLogEntry
	require.NoError(t, json.Unmarshal(mock.lastRecord.Value, &entry))
	assert.Equal(t, "info", entry.Level)
	assert.Equal(t, string(p), entry.Message)
	assert.Equal(t, "tr-1", entry.TraceID)
	assert.Equal(t, "sp-1", entry.SpanID)
}

func TestKafkaWriterWriteWithoutTrace(t *testing.T) {
	mock := &mockProducer{}
	l := &Logger{module: "test-mod", config: Config{TraceEnabled: false}}
	kw := &kafkaWriter{logger: l, producer: mock, timeout: time.Second, topic: "test-topic"}

	_, err := kw.Write([]byte(`{"level":"info","message":"no trace"}`))
	assert.NoError(t, err)

	var entry KafkaLogEntry
	require.NoError(t, json.Unmarshal(mock.lastRecord.Value, &entry))
	assert.Empty(t, entry.TraceID)
	assert.Empty(t, entry.SpanID)
}

func TestKafkaWriterWriteUnmarshalError(t *testing.T) {
	mock := &mockProducer{}
	l := &Logger{module: "test-mod", config: Config{}}
	kw := &kafkaWriter{logger: l, producer: mock, timeout: time.Second, topic: "test-topic"}

	n, err := kw.Write([]byte("{invalid json"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal log data")
	assert.Equal(t, 0, n)
}

func TestKafkaWriterWriteProduceError(t *testing.T) {
	mock := &mockProducer{produceErr: errors.New("kafka broker unreachable")}
	l := &Logger{module: "test-mod", config: Config{}}
	kw := &kafkaWriter{logger: l, producer: mock, timeout: time.Second, topic: "test-topic"}

	n, err := kw.Write([]byte(`{"level":"info","message":"boom"}`))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to produce message")
	assert.Equal(t, 0, n)
}

func TestKafkaWriterWriteMarshalError(t *testing.T) {
	orig := marshalLogEntry
	marshalLogEntry = func(any) ([]byte, error) { return nil, errors.New("marshal failed") }
	t.Cleanup(func() { marshalLogEntry = orig })

	mock := &mockProducer{}
	l := &Logger{module: "test-mod", config: Config{}}
	kw := &kafkaWriter{logger: l, producer: mock, timeout: time.Second, topic: "test-topic"}

	n, err := kw.Write([]byte(`{"level":"info","message":"boom"}`))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to marshal log entry")
	assert.Equal(t, 0, n)
}

func TestKafkaWriterWriteLevelWithProducer(t *testing.T) {
	mock := &mockProducer{}
	l := &Logger{module: "test-mod", config: Config{}}
	kw := &kafkaWriter{logger: l, producer: mock, timeout: time.Second, topic: "test-topic"}

	n, err := kw.WriteLevel(zerolog.InfoLevel, []byte(`{"level":"info","message":"leveled"}`))
	assert.NoError(t, err)
	assert.Equal(t, 36, n)
}

func TestNewParseLevelError(t *testing.T) {
	orig := parseLogLevel
	parseLogLevel = func(string) (zerolog.Level, error) { return 0, errors.New("parse failed") }
	t.Cleanup(func() { parseLogLevel = orig })

	cfg := Config{
		Module:        "test",
		LogLevel:      "info",
		KafkaLogLevel: "error",
	}

	l, err := New(cfg)
	assert.Error(t, err)
	assert.Nil(t, l)
	assert.Contains(t, err.Error(), "failed to parse log level")
}

func TestKafkaLoggerLogDebugAndDefault(t *testing.T) {
	var buf bytes.Buffer

	kl := &KafkaLogger{
		level:  kgo.LogLevelDebug,
		Logger: zerolog.New(&buf),
	}

	t.Run("debug level", func(t *testing.T) {
		buf.Reset()
		kl.Log(kgo.LogLevelDebug, "debug message", "key", "value")
		assert.Contains(t, buf.String(), "debug message")
	})

	t.Run("unknown level falls back to debug", func(t *testing.T) {
		buf.Reset()
		kl.Log(kgo.LogLevelNone, "none message", "key", "value")
		assert.Contains(t, buf.String(), "none message")
	})
}

func kafkaProducerConfig() Config {
	return Config{
		Module:        "test",
		LogLevel:      "info",
		KafkaLogLevel: "error",
		KafkaConfig: KafkaConfig{
			ProduceConfig: ProduceConfig{
				Brokers: []string{"localhost:9092"},
				Topic:   "test-topic",
				TLS: TLSConfig{
					Enabled:            true,
					InsecureSkipVerify: true,
					MinVersion:         "1.2",
					MaxVersion:         "1.3",
				},
				SASL: SASLConfig{
					Enabled:   true,
					Mechanism: "PLAIN",
					Username:  "user",
					Password:  "pass",
				},
				Timeout: TimeoutConfig{
					Dial:               5 * time.Second,
					ConnIdle:           10 * time.Second,
					RequestOverhead:    1 * time.Second,
					Rebalance:          10 * time.Second,
					Retry:              3 * time.Second,
					Session:            10 * time.Second,
					ProduceRequest:     5 * time.Second,
					RecordDelivery:     10 * time.Second,
					TransactionTimeout: 60 * time.Second,
				},
			},
			Producer: ProducerConfig{
				Partitioner:   PartitionerRoundRobin,
				RequireAcks:   AckAll,
				Compression:   []CompressionType{CompressionGzip, CompressionZstd},
				RecordRetries: 3,
				BatchMaxBytes: 1048576,
			},
		},
	}
}

func TestSetupKafkaProducerFull(t *testing.T) {
	producer, err := setupKafkaProducer(kafkaProducerConfig())
	require.NoError(t, err)
	require.NotNil(t, producer)
	producer.Close()
}

func TestSetupKafkaProducerInvalidPartitioner(t *testing.T) {
	cfg := kafkaProducerConfig()
	cfg.KafkaConfig.Producer.Partitioner = "invalid"

	producer, err := setupKafkaProducer(cfg)
	assert.Error(t, err)
	assert.Nil(t, producer)
	assert.Contains(t, err.Error(), "failed to create partitioner")
}

func TestSetupKafkaProducerTLSConfigError(t *testing.T) {
	cfg := kafkaProducerConfig()
	cfg.KafkaConfig.Producer.Partitioner = PartitionerManual
	cfg.KafkaConfig.TLS.MinVersion = "invalid"

	producer, err := setupKafkaProducer(cfg)
	assert.Error(t, err)
	assert.Nil(t, producer)
	assert.Contains(t, err.Error(), "failed to build TLS config")
}

func TestSetupKafkaProducerSASLError(t *testing.T) {
	cfg := kafkaProducerConfig()
	cfg.KafkaConfig.Producer.Partitioner = PartitionerManual
	cfg.KafkaConfig.SASL.Mechanism = "INVALID"

	producer, err := setupKafkaProducer(cfg)
	assert.Error(t, err)
	assert.Nil(t, producer)
	assert.Contains(t, err.Error(), "failed to build SASL config")
}

func TestBuildTLSDialerSuccess(t *testing.T) {
	certFile, keyFile := writeTestCert(t)

	cfg := TLSConfig{
		Enabled:            true,
		MinVersion:         "1.2",
		MaxVersion:         "1.3",
		InsecureSkipVerify: true,
		CertFile:           certFile,
		KeyFile:            keyFile,
		CAFile:             certFile,
	}

	dialer, err := buildTLSDialer(cfg)
	require.NoError(t, err)
	require.NotNil(t, dialer)
	assert.Equal(t, uint16(0x0303), dialer.Config.MinVersion)
	assert.Equal(t, uint16(0x0304), dialer.Config.MaxVersion)
	assert.True(t, dialer.Config.InsecureSkipVerify)
	assert.Len(t, dialer.Config.Certificates, 1)
	assert.NotNil(t, dialer.Config.RootCAs)
}

func TestBuildTLSDialerOnlyMinVersion(t *testing.T) {
	cfg := TLSConfig{
		Enabled:    true,
		MinVersion: "1.3",
	}

	dialer, err := buildTLSDialer(cfg)
	require.NoError(t, err)
	assert.Equal(t, uint16(0x0304), dialer.Config.MinVersion)
	assert.Zero(t, dialer.Config.MaxVersion)
}

func TestBuildTLSDialerInvalidCAPEM(t *testing.T) {
	dir := t.TempDir()
	badFile := filepath.Join(dir, "bad.pem")
	require.NoError(t, os.WriteFile(badFile, []byte("not a pem certificate"), 0o600))

	cfg := TLSConfig{
		Enabled: true,
		CAFile:  badFile,
	}

	_, err := buildTLSDialer(cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse CA certificate")
}

func writeTestCert(t *testing.T) (certFile, keyFile string) {
	t.Helper()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		IsCA:         true,
	}

	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	keyDER, err := x509.MarshalECPrivateKey(priv)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	dir := t.TempDir()
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")

	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))
	return certFile, keyFile
}

func TestBuildSASLOptionSuccess(t *testing.T) {
	tests := []struct {
		mechanism string
	}{
		{"PLAIN"},
		{"SCRAM-SHA-256"},
		{"SCRAM-SHA-512"},
	}

	for _, tt := range tests {
		t.Run(tt.mechanism, func(t *testing.T) {
			cfg := SASLConfig{
				Enabled:   true,
				Mechanism: tt.mechanism,
				Username:  "user",
				Password:  "pass",
			}

			opt, err := buildSASLOption(cfg)
			assert.NoError(t, err)
			assert.NotNil(t, opt)
		})
	}
}

func TestBuildTimeoutOptionsAll(t *testing.T) {
	cfg := TimeoutConfig{
		Dial:               5 * time.Second,
		ConnIdle:           10 * time.Second,
		RequestOverhead:    1 * time.Second,
		Rebalance:          10 * time.Second,
		Retry:              3 * time.Second,
		Session:            10 * time.Second,
		ProduceRequest:     5 * time.Second,
		RecordDelivery:     10 * time.Second,
		TransactionTimeout: 60 * time.Second,
	}

	opts := buildTimeoutOptions(cfg)
	assert.Len(t, opts, 9)
}
