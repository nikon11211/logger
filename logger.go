package logger

import (
	"context"
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
	globalLogger *Logger
	globalMu     sync.RWMutex
)

type Logger struct {
	zerolog.Logger
	module      string
	config      Config
	producer    *kgo.Client
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
		return nil, fmt.Errorf("logger not initialized")
	}
	return globalLogger, nil
}

func New(cfg Config) (*Logger, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	level, err := zerolog.ParseLevel(cfg.LogLevel)
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
		producer:    producer,
		callerDepth: cfg.CallerDepth,
	}

	if producer != nil {
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

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.producer != nil {
		l.producer.Close()
		l.Info("Kafka producer closed successfully")
	}
	return nil
}

func (l *Logger) Print(i ...any) {
	if l == nil {
		return
	}
	l.Logger.Print(i...)
}

func (l *Logger) Printf(format string, args ...any) {
	if l == nil {
		return
	}
	l.Logger.Printf(format, args...)
}

func (l *Logger) Printj(j glog.JSON) {
	if l == nil {
		return
	}
	l.Logger.Printf("%+v", j)
}

func (l *Logger) Debug(i ...any) {
	if l == nil {
		return
	}
	l.Logger.Debug().Msg(fmt.Sprint(i...))
}

func (l *Logger) Debugf(format string, args ...any) {
	if l == nil {
		return
	}
	formattedMsg := fmt.Sprintf(format, args...)
	l.Logger.Debug().Msg(formattedMsg)
}

func (l *Logger) Debugj(j glog.JSON) {
	if l == nil {
		return
	}
	l.Logger.Printf("%+v", j)
}

func (l *Logger) Info(i ...any) {
	if l == nil {
		return
	}
	l.Logger.Info().Msg(fmt.Sprint(i...))
}

func (l *Logger) Infof(format string, args ...any) {
	if l == nil {
		return
	}
	formattedMsg := fmt.Sprintf(format, args...)
	l.Logger.Info().Msg(formattedMsg)
}

func (l *Logger) Infoj(j glog.JSON) {
	if l == nil {
		return
	}
	l.Logger.Printf("%+v", j)
}

func (l *Logger) Warn(i ...any) {
	if l == nil {
		return
	}
	l.Logger.Warn().Msg(fmt.Sprint(i...))
}

func (l *Logger) Warnf(format string, args ...any) {
	if l == nil {
		return
	}
	formattedMsg := fmt.Sprintf(format, args...)
	l.Logger.Warn().Msg(formattedMsg)
}

func (l *Logger) Warnj(j glog.JSON) {
	if l == nil {
		return
	}
	l.Logger.Printf("%+v", j)
}

func (l *Logger) Error(i ...any) {
	if l == nil {
		return
	}
	l.Logger.Error().Msg(fmt.Sprint(i...))
}

func (l *Logger) Errorf(format string, args ...any) {
	if l == nil {
		return
	}
	formattedMsg := fmt.Sprintf(format, args...)
	l.Logger.Error().Msg(formattedMsg)
}

func (l *Logger) Errorj(j glog.JSON) {
	if l == nil {
		return
	}
	l.Logger.Printf("%+v", j)
}

func (l *Logger) Fatal(i ...any) {
	if l == nil {
		return
	}
	l.Logger.Fatal().Msg(fmt.Sprint(i...))
}

func (l *Logger) Fatalj(j glog.JSON) {
	if l == nil {
		return
	}
	l.Logger.Printf("%+v", j)
}

func (l *Logger) Fatalf(format string, args ...any) {
	if l == nil {
		return
	}
	formattedMsg := fmt.Sprintf(format, args...)
	l.Logger.Fatal().Msg(formattedMsg)
}

func (l *Logger) Panic(i ...any) {
	if l == nil {
		return
	}
	l.Logger.Panic().Msg(fmt.Sprint(i...))
}

func (l *Logger) Panicj(j glog.JSON) {
	if l == nil {
		return
	}
	l.Logger.Printf("%+v", j)
}

func (l *Logger) Panicf(format string, args ...any) {
	if l == nil {
		return
	}
	formattedMsg := fmt.Sprintf(format, args...)
	l.Logger.Panic().Msg(formattedMsg)
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
	msg := fmt.Sprintf(banner, l.module, l.config.LogLevel)
	l.Logger.Info().Msg(msg)
}

func (l *Logger) GetLevel() zerolog.Level {
	return l.Logger.GetLevel()
}

func (l *Logger) WithContext(ctx context.Context) *Logger {
	if l == nil {
		return nil
	}

	logger := l.Logger.With().Logger()

	if l.config.TraceEnabled {
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
		config:      l.config,
		producer:    l.producer,
		callerDepth: l.callerDepth,
	}
}

func (l *Logger) WithGroup(name string) *Logger {
	if l == nil {
		return nil
	}

	return &Logger{
		Logger:      l.Logger.With().Str("group", name).Logger(),
		module:      l.module,
		config:      l.config,
		producer:    l.producer,
		callerDepth: l.callerDepth,
	}
}

func (l *Logger) WithCallerDepth(depth int) *Logger {
	if l == nil {
		return nil
	}

	newLogger := l.Logger.With().
		CallerWithSkipFrameCount(depth).
		Logger()

	return &Logger{
		Logger:      newLogger,
		module:      l.module,
		config:      l.config,
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

	kafkaLogger := l.Logger.With().
		Str("component", "kafka").
		Str("service", service).
		Str("module", fmt.Sprintf("KAFKA_%s", service)).
		Logger().
		Level(zerologLvl)

	return &KafkaLogger{
		level:  level,
		Logger: kafkaLogger,
	}
}

func (l *Logger) SetLevel(level string) error {
	if l == nil {
		return fmt.Errorf("logger is nil")
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
				FormatLevel: func(i interface{}) string {
					level := strings.ToUpper(fmt.Sprintf("%s", i))
					if len(level) == 4 {
						return fmt.Sprintf("[%s]", level)
					}
					return fmt.Sprintf("[%s]", level)
				},
				FormatMessage: func(i interface{}) string {
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
