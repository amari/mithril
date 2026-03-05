package adapterszerolog

import (
	"strings"

	"github.com/rs/zerolog"
	"go.uber.org/fx/fxevent"
)

type fxLogger struct {
	inner    *zerolog.Logger // underlying zerolog logger
	logLvl   zerolog.Level   // log level for non-error events (default: zerolog.InfoLevel)
	errorLvl zerolog.Level   // log level for error events
}

var _ fxevent.Logger = (*fxLogger)(nil)

// New creates a new Logger that writes to the provided zerolog.Logger.
func NewFXLogger(logger *zerolog.Logger) fxevent.Logger {
	if logger == nil {
		nop := zerolog.Nop()
		logger = &nop
	}

	return &fxLogger{
		inner:    logger,
		logLvl:   zerolog.DebugLevel,
		errorLvl: zerolog.ErrorLevel,
	}
}

// err returns a zerolog event at the configured error level, or Error level by default.
func (l *fxLogger) err() *zerolog.Event {
	return l.inner.WithLevel(l.errorLvl)
}

// log returns a zerolog event at the configured log level, or Info level by default.
func (l *fxLogger) log() *zerolog.Event {
	return l.inner.WithLevel(l.logLvl)
}

// LogEvent logs the given Fx event to the underlying zerolog logger.
// It handles all standard fxevent.Event types and logs relevant fields for each.
func (l *fxLogger) LogEvent(event fxevent.Event) {
	switch e := event.(type) {
	case *fxevent.OnStartExecuting:
		l.log().Str("callee", e.FunctionName).Str("caller", e.CallerName).Msg("OnStart hook executing")
	case *fxevent.OnStartExecuted:
		if e.Err != nil {
			l.err().Str("callee", e.FunctionName).Str("caller", e.CallerName).Err(e.Err).Msg("OnStart hook failed")
		} else {
			l.log().Str("callee", e.FunctionName).Str("caller", e.CallerName).Str("runtime", e.Runtime.String()).Msg("OnStart hook executed")
		}
	case *fxevent.OnStopExecuting:
		l.log().Str("callee", e.FunctionName).Str("caller", e.CallerName).Msg("OnStop hook executing")
	case *fxevent.OnStopExecuted:
		if e.Err != nil {
			l.err().Str("callee", e.FunctionName).Str("caller", e.CallerName).Err(e.Err).Msg("OnStop hook failed")
		} else {
			l.log().Str("callee", e.FunctionName).Str("caller", e.CallerName).Str("runtime", e.Runtime.String()).Msg("OnStop hook executed")
		}
	case *fxevent.Supplied:
		var event *zerolog.Event
		if e.Err != nil {
			event = l.err()
		} else {
			event = l.log()
		}

		event = event.Str("type", e.TypeName).Strs("stacktrace", e.StackTrace).Strs("moduletrace", e.ModuleTrace)
		event = moduleName(event, e.ModuleName)

		if e.Err != nil {
			event.Err(e.Err).Msg("error encountered while applying options")
		} else {
			event.Msg("supplied")
		}
	case *fxevent.Provided:
		for _, rtype := range e.OutputTypeNames {
			event := l.log().Str("constructor", e.ConstructorName).Strs("stacktrace", e.StackTrace).Strs("moduletrace", e.ModuleTrace)
			event = moduleName(event, e.ModuleName)
			event = event.Str("type", rtype)
			event = maybeBool(event, "private", e.Private)
			event.Msg("provided")
		}
		if e.Err != nil {
			event := l.err().Strs("stacktrace", e.StackTrace).Strs("moduletrace", e.ModuleTrace)
			event = moduleName(event, e.ModuleName)
			event.Err(e.Err).Msg("error encountered while applying options")
		}
	case *fxevent.Replaced:
		for _, rtype := range e.OutputTypeNames {
			event := l.log().Strs("stacktrace", e.StackTrace).Strs("moduletrace", e.ModuleTrace)
			event = moduleName(event, e.ModuleName)
			event = event.Str("type", rtype)
			event.Msg("replaced")
		}
		if e.Err != nil {
			event := l.log().Strs("stacktrace", e.StackTrace).Strs("moduletrace", e.ModuleTrace)
			event = moduleName(event, e.ModuleName)
			event.Err(e.Err).Msg("error encountered while applying options")
		}
	case *fxevent.Decorated:
		for _, rtype := range e.OutputTypeNames {
			event := l.log().Str("decorator", e.DecoratorName).Strs("stacktrace", e.StackTrace).Strs("moduletrace", e.ModuleTrace)
			event = moduleName(event, e.ModuleName)
			event = event.Str("type", rtype)
			event.Msg("decorated")
		}
		if e.Err != nil {
			event := l.err().Strs("stacktrace", e.StackTrace).Strs("moduletrace", e.ModuleTrace)
			event = moduleName(event, e.ModuleName)
			event.Err(e.Err).Msg("error encountered while applying options")
		}
	case *fxevent.BeforeRun:
		event := l.log().Str("name", e.Name).Str("kind", e.Kind)
		event = moduleName(event, e.ModuleName)
		event.Msg("before run")
	case *fxevent.Run:
		if e.Err != nil {
			event := l.err().Str("name", e.Name).Str("kind", e.Kind)
			event = moduleName(event, e.ModuleName)
			event.Msg("error returned")
		} else {
			event := l.log().Str("name", e.Name).Str("kind", e.Kind).Str("runtime", e.Runtime.String())
			event = moduleName(event, e.ModuleName)
			event.Msg("run")
		}
	case *fxevent.Invoking:
		event := l.log().Str("function", e.FunctionName)
		event = moduleName(event, e.ModuleName)
		event.Msg("invoking")
	case *fxevent.Invoked:
		if e.Err != nil {
			event := l.err().Err(e.Err).Str("stack", e.Trace).Str("function", e.FunctionName)
			event = moduleName(event, e.ModuleName)
			event.Msg("invoke failed")
		}
	case *fxevent.Stopping:
		l.log().Str("signal", strings.ToUpper(e.Signal.String())).Msg("received signal")
	case *fxevent.Stopped:
		if e.Err != nil {
			l.err().Err(e.Err).Msg("stop failed")
		}
	case *fxevent.RollingBack:
		l.err().Err(e.StartErr).Msg("start failed, rolling back")
	case *fxevent.RolledBack:
		if e.Err != nil {
			l.err().Err(e.Err).Msg("rollback failed")
		}
	case *fxevent.Started:
		if e.Err != nil {
			l.err().Err(e.Err).Msg("start failed")
		} else {
			l.log().Msg("started")
		}
	case *fxevent.LoggerInitialized:
		if e.Err != nil {
			l.err().Err(e.Err).Msg("custom logger initialization failed")
		} else {
			l.log().Str("function", e.ConstructorName).Msg("initialized custom fxevent.Logger")
		}
	}
}

// moduleName adds the module name to the zerolog event if present.
func moduleName(event *zerolog.Event, name string) *zerolog.Event {
	if len(name) == 0 {
		return event
	}
	return event.Str("module", name)
}

// maybeBool adds a boolean field to the zerolog event if b is true.
func maybeBool(event *zerolog.Event, name string, b bool) *zerolog.Event {
	if b {
		return event.Bool(name, true)
	}
	return event
}
