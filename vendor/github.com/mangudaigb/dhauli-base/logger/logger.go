package logger

import (
	"fmt"
	"log"

	"github.com/mangudaigb/dhauli-base/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var ()

type Logger struct {
	config   *config.Config
	log      *zap.Logger
	sugar    *zap.SugaredLogger
	useSugar bool
}

func NewLogger(config *config.Config) (*Logger, error) {
	mode := config.Logger.Env
	levelStr := config.Logger.Level
	encoding := config.Logger.Encoding
	outputPaths := config.Logger.OutputPaths
	errorOutputPaths := config.Logger.ErrorOutputPaths

	if len(outputPaths) == 0 {
		outputPaths = []string{"stdout"}
	}
	if len(errorOutputPaths) == 0 {
		errorOutputPaths = []string{"stderr"}
	}
	if encoding == "" {
		if mode == "dev" {
			encoding = "console"
		} else {
			encoding = "json"
		}
	}

	atomicLevel := zap.NewAtomicLevel()
	lvl, err := zapcore.ParseLevel(levelStr)
	if err != nil {
		if mode == "dev" {
			lvl = zapcore.DebugLevel
		} else {
			lvl = zapcore.InfoLevel
		}
	}
	atomicLevel.SetLevel(lvl)

	var cfg zap.Config
	if mode == "dev" {
		cfg = zap.NewDevelopmentConfig()
	} else {
		cfg = zap.NewProductionConfig()
	}
	cfg.Level = atomicLevel
	cfg.OutputPaths = outputPaths
	cfg.ErrorOutputPaths = errorOutputPaths
	cfg.Encoding = encoding

	encCfg := cfg.EncoderConfig
	encCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	encCfg.EncodeCaller = zapcore.ShortCallerEncoder
	cfg.EncoderConfig = encCfg

	zapLogger, err := cfg.Build(zap.AddCaller(), zap.AddCallerSkip(1), zap.AddStacktrace(zapcore.ErrorLevel))
	if err != nil {
		log.Fatalf("cannot build zap log: %w", err)
		return nil, err
	}
	logger := zapLogger
	sugar := logger.Sugar()

	useSugar := false
	if mode == "dev" {
		useSugar = true
	}
	return &Logger{
		config:   config,
		log:      logger,
		sugar:    sugar,
		useSugar: useSugar,
	}, nil
}

func (l *Logger) Fatal(msg string, fields ...interface{}) {
	if l.useSugar {
		l.sugar.Fatal(msg, fields)
	} else {
		l.log.Fatal(msg, toZapFields(fields...)...)
	}
}

func (l *Logger) Fatalf(msg string, fields ...interface{}) {
	if l.useSugar {
		l.sugar.Fatalf(msg, fields)
	} else {
		l.log.Fatal(fmt.Sprintf(msg, toZapFields(fields)))
	}
}

func (l *Logger) Error(msg string, fields ...interface{}) {
	if l.useSugar {
		l.sugar.Error(msg, fields)
	} else {
		l.log.Error(msg, toZapFields(fields...)...)
	}
}

func (l *Logger) Errorf(msg string, args ...interface{}) {
	if l.useSugar {
		l.sugar.Errorf(msg, args...)
	} else {
		l.log.Error(fmt.Sprintf(msg, args...))
	}
}

func (l *Logger) Info(msg string, fields ...interface{}) {
	if l.useSugar {
		l.sugar.Info(msg, fields)
	} else {
		l.log.Info(msg, toZapFields(fields...)...)
	}
}

func (l *Logger) Infof(msg string, args ...interface{}) {
	if l.useSugar {
		l.sugar.Infof(msg, args...)
	} else {
		l.log.Info(fmt.Sprintf(msg, args...))
	}
}

func (l *Logger) Debug(msg string, fields ...interface{}) {
	if l.useSugar {
		l.sugar.Debugf(msg, fields...)
	} else {
		l.log.Debug(msg, toZapFields(fields...)...)
	}
}

func (l *Logger) Sync() {
	if l.useSugar {
		_ = l.sugar.Sync()
	} else {
		_ = l.log.Sync()
	}
}

func toZapFields(args ...interface{}) []zap.Field {
	var fields []zap.Field

	for i := 0; i < len(args)-1; i += 2 {
		key, ok := args[i].(string)
		if !ok {
			// skip malformed pair
			continue
		}
		value := args[i+1]
		fields = append(fields, zap.Any(key, value))
	}
	return fields
}
