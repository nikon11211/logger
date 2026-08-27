package logger

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	glog "github.com/labstack/gommon/log"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace"
)

const (
	banner = `
╔══════════════════════════════════════════════════════════════════════╗
║                                                                      ║
║   ██╗      ██████╗  ██████╗  ██████╗ ███████╗██████╗                 ║
║   ██║     ██╔═══██╗██╔════╝ ██╔════╝ ██╔════╝██╔══██╗                ║
║   ██║     ██║   ██║██║  ███╗██║  ██╗ █████╗  ██████╔╝                ║
║   ██║     ██║   ██║██║   ██║██║   ██║██╔══╝  ██╔══██╗                ║
║   ███████╗╚██████╔╝╚██████╔╝╚██████╔╝███████╗██║  ██║                ║
║   ╚══════╝ ╚═════╝  ╚═════╝  ╚═════╝ ╚══════╝╚═╝  ╚═╝                ║
║                                                                      ║
║   %-64s                                                              ║
║   Log Level: %-54s                                                   ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝
`
)

var (
	globalLogger  *Logger
	globalMu      sync.RWMutex
	parseLogLevel = zerolog.ParseLevel
)

type Logger struct {
	zerolog.Logger
	module      string
	config      Config
	producer    kafkaProducer
	callerDepth int
	mu          sync.RWMutex
}

type MultiLevelWriter struct {
	mu      sync.RWMutex
	writers []io.Writer
}

func NewMultiLevelWriter(writers ...io.Writer) *MultiLevelWriter {
	return &MultiLevelWriter{writers: writers}
}

func (mw *MultiLevelWriter) Write(p []byte) (n int, err error) {
	mw.mu.RLock()
	defer mw.mu.RUnlock()

	for _, w := range mw.writers {
		n, err = w.Write(p)
		if err != nil {
			return n, err
		}
	}
	return len(p), nil
}

func (mw *MultiLevelWriter) AddWriter(w io.Writer) {
	mw.mu.Lock()
	defer mw.mu.Unlock()
	mw.writers = append(mw.writers, w)
}

func (mw *MultiLevelWriter) WriteLevel(level zerolog.Level, p []byte) (n int, err error) {
	mw.mu.RLock()
	defer mw.mu.RUnlock()

	for _, w := range mw.writers {
		if lw, ok := w.(zerolog.LevelWriter); ok {
			n, err = lw.WriteLevel(level, p)
		} else {
			n, err = w.Write(p)
		}
		if err != nil {
			return n, err
		}
	}
	return len(p), nil
}

func GetLogger() (*Logger, error) {
	globalMu.RLock()
	defer globalMu.RUnlock()

	if globalLogger == nil {
		return nil, errors.New("logger not initialized")
	}
	return globalLogger, nil
}

func New(cfg Config) (*Logger, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	level, err := parseLogLevel(cfg.LogLevel)
	if err != nil {
		return nil, fmt.Errorf("failed to parse log level: %w", err)
	}

	if cfg.CallerDepth <= 0 {
		cfg.CallerDepth = 3
	}

	writers := setupConsoleWriter(cfg)
	producer, err := setupKafkaProducer(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to setup kafka producer: %w", err)
	}

	multiWriter := NewMultiLevelWriter(writers...)

	baseLogger := zerolog.New(multiWriter).
		With().
		Timestamp().
		CallerWithSkipFrameCount(cfg.CallerDepth).
		Str("module", cfg.Module).
		Logger().
		Level(level)

	l := &Logger{
		Logger:      baseLogger,
		module:      cfg.Module,
		config:      cfg,
		callerDepth: cfg.CallerDepth,
	}

	if producer != nil {
		l.producer = producer
		kafkaW := newKafkaWriter(l, producer, cfg.KafkaConfig.Topic)
		multiWriter.AddWriter(kafkaW)
	}

	globalMu.Lock()
	globalLogger = l
	globalMu.Unlock()

	return l, nil
}

func (l *Logger) Close() error {
	if l == nil {
		return nil
	}

	var producer kafkaProducer
	l.mu.RLock()
	producer = l.producer
	logger := l.Logger
	l.mu.RUnlock()

	if producer != nil {
		producer.Close()
		logger.Info().Msg("Kafka producer closed successfully")
	}
	return nil
}

func (l *Logger) Print(i ...any) {
	if l == nil {
		return
	}
	logger, _ := l.current()
	logger.Print(i...)
}

func (l *Logger) Printf(format string, args ...any) {
	if l == nil {
		return
	}
	logger, _ := l.current()
	logger.Printf(format, args...)
}

func (l *Logger) Printj(j glog.JSON) {
	if l == nil {
		return
	}
	logger, _ := l.current()
	logger.Printf("%+v", j)
}

func (l *Logger) Debug(i ...any) {
	if l == nil {
		return
	}
	logger, _ := l.current()
	logger.Debug().Msg(fmt.Sprint(i...))
}

func (l *Logger) Debugf(format string, args ...any) {
	if l == nil {
		return
	}
	formattedMsg := fmt.Sprintf(format, args...)
	logger, _ := l.current()
	logger.Debug().Msg(formattedMsg)
}

func (l *Logger) Debugj(j glog.JSON) {
	if l == nil {
		return
	}
	logger, _ := l.current()
	logger.Printf("%+v", j)
}

func (l *Logger) Info(i ...any) {
	if l == nil {
		return
	}
	logger, _ := l.current()
	logger.Info().Msg(fmt.Sprint(i...))
}

func (l *Logger) Infof(format string, args ...any) {
	if l == nil {
		return
	}
	formattedMsg := fmt.Sprintf(format, args...)
	logger, _ := l.current()
	logger.Info().Msg(formattedMsg)
}

func (l *Logger) Infoj(j glog.JSON) {
	if l == nil {
		return
	}
	logger, _ := l.current()
	logger.Printf("%+v", j)
}

func (l *Logger) Warn(i ...any) {
	if l == nil {
		return
	}
	logger, _ := l.current()
	logger.Warn().Msg(fmt.Sprint(i...))
}

func (l *Logger) Warnf(format string, args ...any) {
	if l == nil {
		return
	}
	formattedMsg := fmt.Sprintf(format, args...)
	logger, _ := l.current()
	logger.Warn().Msg(formattedMsg)
}

func (l *Logger) Warnj(j glog.JSON) {
	if l == nil {
		return
	}
	logger, _ := l.current()
	logger.Printf("%+v", j)
}

func (l *Logger) Error(i ...any) {
	if l == nil {
		return
	}
	logger, _ := l.current()
	logger.Error().Msg(fmt.Sprint(i...))
}

func (l *Logger) Errorf(format string, args ...any) {
	if l == nil {
		return
	}
	formattedMsg := fmt.Sprintf(format, args...)
	logger, _ := l.current()
	logger.Error().Msg(formattedMsg)
}

func (l *Logger) Errorj(j glog.JSON) {
	if l == nil {
		return
	}
	logger, _ := l.current()
	logger.Printf("%+v", j)
}

func (l *Logger) Fatal(i ...any) {
	if l == nil {
		return
	}
	logger, _ := l.current()
	logger.Fatal().Msg(fmt.Sprint(i...))
}

func (l *Logger) Fatalj(j glog.JSON) {
	if l == nil {
		return
	}
	logger, _ := l.current()
	logger.Printf("%+v", j)
}

func (l *Logger) Fatalf(format string, args ...any) {
	if l == nil {
		return
	}
	formattedMsg := fmt.Sprintf(format, args...)
	logger, _ := l.current()
	logger.Fatal().Msg(formattedMsg)
}

func (l *Logger) Panic(i ...any) {
	if l == nil {
		return
	}
	logger, _ := l.current()
	logger.Panic().Msg(fmt.Sprint(i...))
}

func (l *Logger) Panicj(j glog.JSON) {
	if l == nil {
		return
	}
	logger, _ := l.current()
	logger.Printf("%+v", j)
}

func (l *Logger) Panicf(format string, args ...any) {
	if l == nil {
		return
	}
	formattedMsg := fmt.Sprintf(format, args...)
	logger, _ := l.current()
	logger.Panic().Msg(formattedMsg)
}

func (l *Logger) DebugCtx(ctx context.Context, msg string) {
	if l == nil {
		return
	}
	l.WithContext(ctx).Logger.Debug().Msg(msg)
}

func (l *Logger) InfoCtx(ctx context.Context, msg string) {
	if l == nil {
		return
	}
	l.WithContext(ctx).Logger.Info().Msg(msg)
}

func (l *Logger) WarnCtx(ctx context.Context, msg string) {
	if l == nil {
		return
	}
	l.WithContext(ctx).Logger.Warn().Msg(msg)
}

func (l *Logger) ErrorCtx(ctx context.Context, msg string) {
	if l == nil {
		return
	}
	l.WithContext(ctx).Logger.Error().Msg(msg)
}

func (l *Logger) DebugCtxf(ctx context.Context, format string, args ...any) {
	if l == nil {
		return
	}
	formattedMsg := fmt.Sprintf(format, args...)
	l.WithContext(ctx).Logger.Debug().Msg(formattedMsg)
}

func (l *Logger) InfoCtxf(ctx context.Context, format string, args ...any) {
	if l == nil {
		return
	}
	formattedMsg := fmt.Sprintf(format, args...)
	l.WithContext(ctx).Logger.Info().Msg(formattedMsg)
}

func (l *Logger) WarnCtxf(ctx context.Context, format string, args ...any) {
	if l == nil {
		return
	}
	formattedMsg := fmt.Sprintf(format, args...)
	l.WithContext(ctx).Logger.Warn().Msg(formattedMsg)
}

func (l *Logger) ErrorCtxf(ctx context.Context, format string, args ...any) {
	if l == nil {
		return
	}
	formattedMsg := fmt.Sprintf(format, args...)
	l.WithContext(ctx).Logger.Error().Msg(formattedMsg)
}

func (l *Logger) AppStats() {
	if l == nil {
		return
	}
	logger, cfg := l.current()
	msg := fmt.Sprintf(banner, l.module, cfg.LogLevel)
	logger.Info().Msg(msg)
}

func (l *Logger) GetLevel() zerolog.Level {
	logger, _ := l.current()
	return logger.GetLevel()
}

func (l *Logger) WithContext(ctx context.Context) *Logger {
	if l == nil {
		return nil
	}

	logger, cfg := l.current()

	if cfg.TraceEnabled {
		if span := trace.SpanFromContext(ctx); span != nil {
			spanCtx := span.SpanContext()
			if spanCtx.HasTraceID() {
				logger = logger.With().
					Str("trace_id", spanCtx.TraceID().String()).
					Str("span_id", spanCtx.SpanID().String()).
					Logger()
			}
		}
	}

	return &Logger{
		Logger:      logger,
		module:      l.module,
		config:      cfg,
		producer:    l.producer,
		callerDepth: l.callerDepth,
	}
}

func (l *Logger) WithGroup(name string) *Logger {
	if l == nil {
		return nil
	}
	logger, cfg := l.current()

	return &Logger{
		Logger:      logger.With().Str("group", name).Logger(),
		module:      l.module,
		config:      cfg,
		producer:    l.producer,
		callerDepth: l.callerDepth,
	}
}

func (l *Logger) WithCallerDepth(depth int) *Logger {
	if l == nil {
		return nil
	}
	logger, cfg := l.current()

	newLogger := logger.With().
		CallerWithSkipFrameCount(depth).
		Logger()

	return &Logger{
		Logger:      newLogger,
		module:      l.module,
		config:      cfg,
		producer:    l.producer,
		callerDepth: depth,
	}
}

func (l *Logger) NewKafkaLogger(env, service string) *KafkaLogger {
	if l == nil {
		log.Warn().Msg("logger: unable to prepare kafka logger")
		return &KafkaLogger{
			level:  kgo.LogLevelInfo,
			Logger: zerolog.Nop(),
		}
	}

	level := parseKafkaLogLevel(env)

	zerologLvl, err := zerolog.ParseLevel(env)
	if err != nil {
		zerologLvl = zerolog.InfoLevel
	}

	logger, _ := l.current()

	kafkaLogger := logger.With().
		Str("component", "kafka").
		Str("service", service).
		Str("module", "KAFKA_"+service).
		Logger().
		Level(zerologLvl)

	return &KafkaLogger{
		level:  level,
		Logger: kafkaLogger,
	}
}

func (l *Logger) SetLevel(level string) error {
	if l == nil {
		return errors.New("logger is nil")
	}

	newLevel, err := zerolog.ParseLevel(level)
	if err != nil {
		return fmt.Errorf("invalid log level %q: %w", level, err)
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	l.Logger = l.Level(newLevel)
	l.config.LogLevel = level
	return nil
}

func (l *Logger) current() (zerolog.Logger, Config) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.Logger, l.config
}

func setupConsoleWriter(cfg Config) []io.Writer {
	var writers []io.Writer

	if cfg.PrettyPrint {
		if cfg.Color {
			writers = append(writers, zerolog.ConsoleWriter{
				Out:        os.Stderr,
				TimeFormat: time.RFC3339,
			})
		} else {
			writers = append(writers, zerolog.ConsoleWriter{
				FormatLevel: func(i any) string {
					level := strings.ToUpper(fmt.Sprintf("%s", i))
					if len(level) == 4 {
						return fmt.Sprintf("[%s]", level)
					}
					return fmt.Sprintf("[%s]", level)
				},
				FormatMessage: func(i any) string {
					return fmt.Sprintf("%s", i)
				},
				NoColor:    true,
				Out:        os.Stderr,
				TimeFormat: time.RFC3339,
			})
		}
	} else {
		writers = append(writers, os.Stdout)
	}

	return writers
}
