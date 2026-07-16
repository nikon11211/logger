package logger

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

const (
	defaultKafkaTimeout = 5 * time.Second
	defaultKafkaTopic   = "service-logs"
)

type KafkaLogEntry struct {
	Level     string `json:"level"`
	Message   string `json:"message"`
	Module    string `json:"module"`
	Timestamp int64  `json:"timestamp"`
	TraceID   string `json:"trace_id,omitempty"`
	SpanID    string `json:"span_id,omitempty"`
	Caller    string `json:"caller"`
}

type kafkaWriter struct {
	logger   *Logger
	producer *kgo.Client
	timeout  time.Duration
	topic    string
}

func newKafkaWriter(l *Logger, producer *kgo.Client, topic string) *kafkaWriter {
	if topic == "" {
		topic = defaultKafkaTopic
	}

	return &kafkaWriter{
		logger:   l,
		producer: producer,
		timeout:  defaultKafkaTimeout,
		topic:    topic,
	}
}

func (kw *kafkaWriter) Write(p []byte) (n int, err error) {
	if kw.producer == nil {
		return len(p), nil
	}

	var logData map[string]interface{}
	if err := json.Unmarshal(p, &logData); err != nil {
		return 0, fmt.Errorf("kafka writer: failed to unmarshal log data: %w", err)
	}

	level, _ := logData["level"].(string)
	if level == "" {
		level = "info"
	}

	message, _ := logData["message"].(string)
	if message == "" {
		message = string(p)
	}

	entry := KafkaLogEntry{
		Level:     level,
		Message:   message,
		Module:    kw.logger.module,
		Timestamp: time.Now().UnixNano(),
		Caller:    fmt.Sprint(logData["caller"]),
	}

	if kw.logger.config.TraceEnabled {
		if traceID, ok := logData["trace_id"].(string); ok {
			entry.TraceID = traceID
		}
		if spanID, ok := logData["span_id"].(string); ok {
			entry.SpanID = spanID
		}
	}

	value, err := json.Marshal(entry)
	if err != nil {
		return 0, fmt.Errorf("kafka writer: failed to marshal log entry: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), kw.timeout)
	defer cancel()

	record := &kgo.Record{
		Topic: kw.topic,
		Value: value,
	}

	if err := kw.producer.ProduceSync(ctx, record).FirstErr(); err != nil {
		return 0, fmt.Errorf("kafka writer: failed to produce message: %w", err)
	}

	return len(p), nil
}

func (kw *kafkaWriter) WriteLevel(level zerolog.Level, p []byte) (n int, err error) {
	return kw.Write(p)
}

type KafkaLogger struct {
	level kgo.LogLevel
	zerolog.Logger
}

func (kl *KafkaLogger) Level() kgo.LogLevel {
	if kl == nil {
		return kgo.LogLevelNone
	}
	return kl.level
}

func (kl *KafkaLogger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	if kl.Level() < level {
		return
	}

	var event *zerolog.Event
	switch level {
	case kgo.LogLevelDebug:
		event = kl.Debug()
	case kgo.LogLevelInfo:
		event = kl.Info()
	case kgo.LogLevelWarn:
		event = kl.Warn()
	case kgo.LogLevelError:
		event = kl.Error()
	default:
		event = kl.Debug()
	}

	for i := 0; i < len(keyvals); i += 2 {
		key := fmt.Sprint(keyvals[i])
		var value interface{} = ""
		if i+1 < len(keyvals) {
			value = keyvals[i+1]
		}
		event = event.Interface(key, value)
	}

	event.Msg(msg)
}

func setupKafkaProducer(cfg Config) (*kgo.Client, error) {
	if len(cfg.KafkaConfig.Brokers) == 0 {
		return nil, nil
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.KafkaConfig.Brokers...),
		kgo.DefaultProduceTopic(cfg.KafkaConfig.Topic),
		kgo.RecordRetries(cfg.KafkaConfig.Producer.RecordRetries),
		kgo.ProducerBatchMaxBytes(cfg.KafkaConfig.Producer.BatchMaxBytes),
	}

	partitioner, err := createPartitioner(cfg.KafkaConfig.Producer.Partitioner)
	if err != nil {
		return nil, fmt.Errorf("failed to create partitioner: %w", err)
	}
	opts = append(opts, kgo.RecordPartitioner(partitioner))

	acks := createAcks(cfg.KafkaConfig.Producer.RequireAcks)
	opts = append(opts, kgo.RequiredAcks(acks))

	if len(cfg.KafkaConfig.Producer.Compression) > 0 {
		codecs := createCompressionCodecs(cfg.KafkaConfig.Producer.Compression)
		opts = append(opts, kgo.ProducerBatchCompression(codecs...))
	}

	if cfg.KafkaConfig.TLS.Enabled {
		tlsDialer, err := buildTLSDialer(cfg.KafkaConfig.TLS)
		if err != nil {
			return nil, fmt.Errorf("failed to build TLS config: %w", err)
		}
		opts = append(opts, kgo.Dialer(tlsDialer.DialContext))
	}

	if cfg.KafkaConfig.SASL.Enabled {
		saslOpt, err := buildSASLOption(cfg.KafkaConfig.SASL)
		if err != nil {
			return nil, fmt.Errorf("failed to build SASL config: %w", err)
		}
		opts = append(opts, saslOpt)
	}

	opts = append(opts, buildTimeoutOptions(cfg.KafkaConfig.Timeout)...)

	zerologLvl, _ := zerolog.ParseLevel(cfg.KafkaLogLevel)
	kafkaLogger := &KafkaLogger{
		level: parseKafkaLogLevel(cfg.KafkaLogLevel),
		Logger: zerolog.New(os.Stderr).
			With().
			Timestamp().
			Str("module", cfg.Module).
			Logger().
			Level(zerologLvl),
	}
	opts = append(opts, kgo.WithLogger(kafkaLogger))

	return kgo.NewClient(opts...)
}

func createPartitioner(pt PartitionerType) (kgo.Partitioner, error) {
	switch pt {
	case PartitionerUniformBytes:
		return kgo.UniformBytesPartitioner(32<<10, true, true, nil), nil
	case PartitionerLeastBackup:
		return kgo.LeastBackupPartitioner(), nil
	case PartitionerManual:
		return kgo.ManualPartitioner(), nil
	case PartitionerRoundRobin:
		return kgo.RoundRobinPartitioner(), nil
	case PartitionerStickyKey:
		return kgo.StickyKeyPartitioner(nil), nil
	case PartitionerSticky:
		return kgo.StickyPartitioner(), nil
	default:
		return nil, fmt.Errorf("unsupported partitioner type: %s", pt)
	}
}

func createAcks(ack AckType) kgo.Acks {
	switch ack {
	case AckNone:
		return kgo.NoAck()
	case AckLeader:
		return kgo.LeaderAck()
	case AckAll:
		return kgo.AllISRAcks()
	default:
		return kgo.AllISRAcks()
	}
}

func createCompressionCodecs(compressions []CompressionType) []kgo.CompressionCodec {
	codecs := make([]kgo.CompressionCodec, 0, len(compressions))

	for _, compression := range compressions {
		var codec kgo.CompressionCodec
		switch compression {
		case CompressionNone:
			codec = kgo.NoCompression()
		case CompressionGzip:
			codec = kgo.GzipCompression()
		case CompressionSnappy:
			codec = kgo.SnappyCompression()
		case CompressionLz4:
			codec = kgo.Lz4Compression()
		case CompressionZstd:
			codec = kgo.ZstdCompression()
		default:
			continue
		}
		codecs = append(codecs, codec)
	}

	return codecs
}

func buildTLSDialer(cfg TLSConfig) (*tls.Dialer, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: cfg.InsecureSkipVerify,
	}

	if cfg.MinVersion != "" {
		version, err := parseTLSVersion(cfg.MinVersion)
		if err != nil {
			return nil, fmt.Errorf("invalid TLS min version: %w", err)
		}
		tlsConfig.MinVersion = version
	}

	if cfg.MaxVersion != "" {
		version, err := parseTLSVersion(cfg.MaxVersion)
		if err != nil {
			return nil, fmt.Errorf("invalid TLS max version: %w", err)
		}
		tlsConfig.MaxVersion = version
	}

	if cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	if cfg.CAFile != "" {
		caCert, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}
		tlsConfig.RootCAs = caCertPool
	}

	return &tls.Dialer{Config: tlsConfig}, nil
}

func buildSASLOption(cfg SASLConfig) (kgo.Opt, error) {
	switch cfg.Mechanism {
	case "PLAIN":
		return kgo.SASL(plain.Auth{
			User: cfg.Username,
			Pass: cfg.Password,
		}.AsMechanism()), nil
	case "SCRAM-SHA-256":
		return kgo.SASL(scram.Auth{
			User: cfg.Username,
			Pass: cfg.Password,
		}.AsSha256Mechanism()), nil
	case "SCRAM-SHA-512":
		return kgo.SASL(scram.Auth{
			User: cfg.Username,
			Pass: cfg.Password,
		}.AsSha512Mechanism()), nil
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %s", cfg.Mechanism)
	}
}

func buildTimeoutOptions(cfg TimeoutConfig) []kgo.Opt {
	var opts []kgo.Opt

	if cfg.Dial > 0 {
		opts = append(opts, kgo.DialTimeout(cfg.Dial))
	}
	if cfg.ConnIdle > 0 {
		opts = append(opts, kgo.ConnIdleTimeout(cfg.ConnIdle))
	}
	if cfg.RequestOverhead > 0 {
		opts = append(opts, kgo.RequestTimeoutOverhead(cfg.RequestOverhead))
	}
	if cfg.Rebalance > 0 {
		opts = append(opts, kgo.RebalanceTimeout(cfg.Rebalance))
	}
	if cfg.Retry > 0 {
		opts = append(opts, kgo.RetryTimeout(cfg.Retry))
	}
	if cfg.Session > 0 {
		opts = append(opts, kgo.SessionTimeout(cfg.Session))
	}
	if cfg.ProduceRequest > 0 {
		opts = append(opts, kgo.ProduceRequestTimeout(cfg.ProduceRequest))
	}
	if cfg.RecordDelivery > 0 {
		opts = append(opts, kgo.RecordDeliveryTimeout(cfg.RecordDelivery))
	}
	if cfg.TransactionTimeout > 0 {
		opts = append(opts, kgo.TransactionTimeout(cfg.TransactionTimeout))
	}

	return opts
}

func parseTLSVersion(version string) (uint16, error) {
	switch version {
	case "1.0":
		return tls.VersionTLS10, nil
	case "1.1":
		return tls.VersionTLS11, nil
	case "1.2":
		return tls.VersionTLS12, nil
	case "1.3":
		return tls.VersionTLS13, nil
	default:
		return 0, fmt.Errorf("unsupported TLS version: %s", version)
	}
}

func parseKafkaLogLevel(level string) kgo.LogLevel {
	switch level {
	case "debug":
		return kgo.LogLevelDebug
	case "info":
		return kgo.LogLevelInfo
	case "warn", "warning":
		return kgo.LogLevelWarn
	case "error":
		return kgo.LogLevelError
	default:
		return kgo.LogLevelInfo
	}
}
