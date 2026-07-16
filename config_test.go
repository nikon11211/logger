package logger

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPartitionerTypes(t *testing.T) {
	assert.Equal(t, PartitionerType("uniform_bytes"), PartitionerUniformBytes)
	assert.Equal(t, PartitionerType("least_backup"), PartitionerLeastBackup)
	assert.Equal(t, PartitionerType("manual"), PartitionerManual)
	assert.Equal(t, PartitionerType("round_robin"), PartitionerRoundRobin)
	assert.Equal(t, PartitionerType("sticky_key"), PartitionerStickyKey)
	assert.Equal(t, PartitionerType("sticky"), PartitionerSticky)
}

func TestAckTypes(t *testing.T) {
	assert.Equal(t, AckType("none"), AckNone)
	assert.Equal(t, AckType("leader"), AckLeader)
	assert.Equal(t, AckType("all"), AckAll)
}

func TestCompressionTypes(t *testing.T) {
	assert.Equal(t, CompressionType("none"), CompressionNone)
	assert.Equal(t, CompressionType("gzip"), CompressionGzip)
	assert.Equal(t, CompressionType("snappy"), CompressionSnappy)
	assert.Equal(t, CompressionType("lz4"), CompressionLz4)
	assert.Equal(t, CompressionType("zstd"), CompressionZstd)
}

func TestDefaultConfigValues(t *testing.T) {
	cfg := DefaultConfig()

	assert.Equal(t, "info", cfg.LogLevel)
	assert.Equal(t, 3, cfg.CallerDepth)
	assert.Equal(t, "error", cfg.KafkaLogLevel)
	assert.True(t, cfg.PrettyPrint)
	assert.False(t, cfg.TraceEnabled)
	assert.False(t, cfg.GormTrace)
	assert.Equal(t, uint(200), cfg.GormSlowQueryThreshold)
	assert.False(t, cfg.Color)

	assert.Equal(t, PartitionerUniformBytes, cfg.KafkaConfig.Producer.Partitioner)
	assert.Equal(t, AckAll, cfg.KafkaConfig.Producer.RequireAcks)
	assert.Equal(t, 3, cfg.KafkaConfig.Producer.RecordRetries)
	assert.Equal(t, int32(1048576), cfg.KafkaConfig.Producer.BatchMaxBytes)

	assert.Equal(t, "1.2", cfg.KafkaConfig.TLS.MinVersion)
	assert.Equal(t, "1.3", cfg.KafkaConfig.TLS.MaxVersion)
}

func TestTimeoutConfig(t *testing.T) {
	timeout := TimeoutConfig{
		Dial:               30 * time.Second,
		ConnIdle:           5 * time.Minute,
		RequestOverhead:    1 * time.Second,
		Rebalance:          30 * time.Second,
		Retry:              3 * time.Second,
		Session:            10 * time.Second,
		ProduceRequest:     5 * time.Second,
		RecordDelivery:     10 * time.Second,
		TransactionTimeout: 60 * time.Second,
	}

	assert.Equal(t, 30*time.Second, timeout.Dial)
	assert.Equal(t, 5*time.Minute, timeout.ConnIdle)
	assert.Equal(t, 1*time.Second, timeout.RequestOverhead)
	assert.Equal(t, 30*time.Second, timeout.Rebalance)
	assert.Equal(t, 3*time.Second, timeout.Retry)
	assert.Equal(t, 10*time.Second, timeout.Session)
	assert.Equal(t, 5*time.Second, timeout.ProduceRequest)
	assert.Equal(t, 10*time.Second, timeout.RecordDelivery)
	assert.Equal(t, 60*time.Second, timeout.TransactionTimeout)
}
