package logger

import (
	"fmt"
	"time"
)

type PartitionerType string

const (
	PartitionerUniformBytes PartitionerType = "uniform_bytes"
	PartitionerLeastBackup  PartitionerType = "least_backup"
	PartitionerManual       PartitionerType = "manual"
	PartitionerRoundRobin   PartitionerType = "round_robin"
	PartitionerStickyKey    PartitionerType = "sticky_key"
	PartitionerSticky       PartitionerType = "sticky"
)

type AckType string

const (
	AckNone   AckType = "none"
	AckLeader AckType = "leader"
	AckAll    AckType = "all"
)

type CompressionType string

const (
	CompressionNone   CompressionType = "none"
	CompressionGzip   CompressionType = "gzip"
	CompressionSnappy CompressionType = "snappy"
	CompressionLz4    CompressionType = "lz4"
	CompressionZstd   CompressionType = "zstd"
)

// Config holds the complete configuration for the Logger
type Config struct {
	// Module is the name of the service/module using the logger
	Module string `yaml:"module" mapstructure:"module" validate:"required"`

	// LogLevel sets the minimum log level (debug, info, warn, error)
	LogLevel string `yaml:"log_level" mapstructure:"log_level" validate:"required"`

	// CallerDepth controls how many stack frames to skip when reporting caller
	CallerDepth int `yaml:"caller_depth" mapstructure:"caller_depth"`

	// KafkaLogLevel sets the log level for Kafka client
	KafkaLogLevel string `yaml:"kafka_log_level" mapstructure:"kafka_log_level" validate:"required"`

	// PrettyPrint enables human-readable console output instead of JSON
	PrettyPrint bool `yaml:"pretty_print" mapstructure:"pretty_print"`

	// TraceEnabled enables OpenTelemetry trace context injection
	TraceEnabled bool `yaml:"trace_enabled" mapstructure:"trace_enabled"`

	// GormTrace enables GORM query tracing
	GormTrace bool `yaml:"gorm_trace" mapstructure:"gorm_trace"`

	// GormSlowQueryThreshold defines slow query threshold in milliseconds
	GormSlowQueryThreshold uint `yaml:"gorm_slow_query_threshold" mapstructure:"gorm_slow_query_threshold"`

	// Color enables colored output (only when PrettyPrint is true)
	Color bool `yaml:"color" mapstructure:"color"`

	// KafkaConfig holds Kafka producer configuration
	KafkaConfig KafkaConfig `mapstructure:"kafka_config" yaml:"kafka_config"`
}

// KafkaConfig holds Kafka-specific configuration
type KafkaConfig struct {
	// ProduceConfig contains common Kafka producer settings
	ProduceConfig `yaml:",inline" mapstructure:",squash"`

	// Producer contains Kafka producer-specific settings
	Producer ProducerConfig `yaml:"config" mapstructure:"producer" validate:"required"`
}

// ProduceConfig holds the common Kafka producer configuration
type ProduceConfig struct {
	// Brokers is a list of Kafka broker addresses
	Brokers []string `yaml:"brokers" mapstructure:"brokers" validate:"required"`

	// Topic is the Kafka topic for log messages
	Topic string `yaml:"topic" mapstructure:"topic" validate:"required"`

	// TLS holds TLS configuration
	TLS TLSConfig `yaml:"tls" mapstructure:"tls"`

	// SASL holds SASL configuration
	SASL SASLConfig `yaml:"sasl" mapstructure:"sasl"`

	// Timeout holds connection timeout settings
	Timeout TimeoutConfig `yaml:"timeout" mapstructure:"timeout"`

	// Metrics holds Prometheus metrics configuration
	Metrics MetricsConfig `yaml:"metrics" mapstructure:"metrics"`
}

type TLSConfig struct {
	Enabled            bool   `yaml:"enabled" mapstructure:"enabled"`
	Environment        string `yaml:"environment" mapstructure:"environment"`
	MinVersion         string `yaml:"min_version" mapstructure:"min_version"`
	MaxVersion         string `yaml:"max_version" mapstructure:"max_version"`
	CertFile           string `yaml:"cert_file" mapstructure:"cert_file"`
	KeyFile            string `yaml:"key_file" mapstructure:"key_file"`
	CAFile             string `yaml:"ca_file" mapstructure:"ca_file"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify" mapstructure:"insecure_skip_verify"`
}

type SASLConfig struct {
	Enabled   bool   `yaml:"enabled" mapstructure:"enabled"`
	Mechanism string `yaml:"mechanism" mapstructure:"mechanism"`
	Username  string `yaml:"username" mapstructure:"username"`
	Password  string `yaml:"password" mapstructure:"password"`
}

type TimeoutConfig struct {
	Dial               time.Duration `yaml:"dial" mapstructure:"dial"`
	ConnIdle           time.Duration `yaml:"conn_idle" mapstructure:"conn_idle"`
	RequestOverhead    time.Duration `yaml:"request_overhead" mapstructure:"request_overhead"`
	Rebalance          time.Duration `yaml:"rebalance" mapstructure:"rebalance"`
	Retry              time.Duration `yaml:"retry" mapstructure:"retry"`
	Session            time.Duration `yaml:"session" mapstructure:"session"`
	ProduceRequest     time.Duration `yaml:"produce_request" mapstructure:"produce_request"`
	RecordDelivery     time.Duration `yaml:"record_delivery" mapstructure:"record_delivery"`
	TransactionTimeout time.Duration `yaml:"transaction_timeout" mapstructure:"transaction_timeout"`
}

type MetricsConfig struct {
	Namespace   string `yaml:"namespace" mapstructure:"namespace"`
	Port        uint32 `yaml:"port" mapstructure:"port"`
	EnabledHTTP bool   `yaml:"enabled_http" mapstructure:"enabled_http"`
}

type ProducerConfig struct {
	Partitioner   PartitionerType   `yaml:"producer_partitioner" mapstructure:"producer_partitioner" validate:"required"`
	RequireAcks   AckType           `yaml:"require_acks" mapstructure:"require_acks" validate:"required"`
	Compression   []CompressionType `yaml:"compression" mapstructure:"compression" validate:"required"`
	RecordRetries int               `yaml:"record_retries" mapstructure:"record_retries" validate:"required"`
	BatchMaxBytes int32             `yaml:"producer_batch_max_bytes" mapstructure:"producer_batch_max_bytes" validate:"required"`
}

func DefaultConfig() Config {
	return Config{
		LogLevel:               "info",
		CallerDepth:            3,
		KafkaLogLevel:          "error",
		PrettyPrint:            true,
		TraceEnabled:           false,
		GormTrace:              false,
		GormSlowQueryThreshold: 200,
		Color:                  false,
		KafkaConfig: KafkaConfig{
			ProduceConfig: ProduceConfig{
				TLS: TLSConfig{
					MinVersion: "1.2",
					MaxVersion: "1.3",
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
}

func (c Config) Validate() error {
	if c.Module == "" {
		return fmt.Errorf("module name is required")
	}

	validLevels := map[string]bool{
		"debug": true,
		"info":  true,
		"warn":  true,
		"error": true,
	}

	if !validLevels[c.LogLevel] {
		return fmt.Errorf("invalid log level: %s (must be one of: debug, info, warn, error)", c.LogLevel)
	}

	if !validLevels[c.KafkaLogLevel] {
		return fmt.Errorf("invalid kafka log level: %s (must be one of: debug, info, warn, error)", c.KafkaLogLevel)
	}

	if c.CallerDepth < 0 {
		return fmt.Errorf("caller depth must be non-negative")
	}

	if len(c.KafkaConfig.Brokers) > 0 {
		if c.KafkaConfig.Topic == "" {
			return fmt.Errorf("kafka topic is required when brokers are configured")
		}
		if c.KafkaConfig.Producer.BatchMaxBytes <= 0 {
			return fmt.Errorf("batch max bytes must be greater than 0")
		}
		if c.KafkaConfig.Producer.RecordRetries < 0 {
			return fmt.Errorf("record retries must be non-negative")
		}
	}

	return nil
}
