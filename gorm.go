package logger

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm/logger"
)

const (
	defaultSlowThreshold = 200 * time.Millisecond
)

// GormLogger implements gorm.Logger interface
type GormLogger struct {
	*Logger
	slowThreshold time.Duration
	traceAll      bool
	ignoreTrace   bool
}

func (l *Logger) NewGormLogger() *GormLogger {
	if l == nil {
		return nil
	}

	threshold := time.Duration(l.config.GormSlowQueryThreshold) * time.Millisecond
	if threshold == 0 {
		threshold = defaultSlowThreshold
	}

	return &GormLogger{
		Logger:        l.WithGroup(fmt.Sprintf("GORM_%s_LOGS", l.module)),
		slowThreshold: threshold,
		traceAll:      l.config.GormTrace,
		ignoreTrace:   !l.config.GormTrace,
	}
}

func (gl *GormLogger) LogMode(level logger.LogLevel) logger.Interface {
	return gl
}

func (gl *GormLogger) Info(ctx context.Context, msg string, data ...interface{}) {
	gl.Logger.InfoCtx(ctx, fmt.Sprintf(msg, data...))
}

func (gl *GormLogger) Warn(ctx context.Context, msg string, data ...interface{}) {
	gl.Logger.WarnCtx(ctx, fmt.Sprintf(msg, data...))
}

func (gl *GormLogger) Error(ctx context.Context, msg string, data ...interface{}) {
	gl.Logger.ErrorCtx(ctx, fmt.Sprintf(msg, data...))
}

func (gl *GormLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	if gl.ignoreTrace || fc == nil {
		return
	}

	elapsed := time.Since(begin)
	sql, rows := fc()

	switch {
	case err != nil:
		gl.Logger.ErrorCtxf(ctx, "gorm error: %v [%s] (%d rows)", err, sql, rows)
	case elapsed > gl.slowThreshold:
		gl.Logger.WarnCtxf(ctx, "slow query [%s]: %s (%d rows)", elapsed, sql, rows)
	case gl.traceAll:
		gl.Logger.DebugCtxf(ctx, "query: %s (%d rows, %v)", sql, rows, elapsed)
	}
}
