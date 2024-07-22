package zstreams

import (
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// WorkFactoryOption interface to identify functional options
type WorkFactoryOption interface {
	apply(s *WorkFactory)
}

// WithLogger  provides option to override the logger to use. default is noop
func WithLogger(l Logger) WorkFactoryOption { return loggerOption{l} }

// WithTracerProvider provides option to specify the otel tracer provider used by the created work object.
// Defaults to nil (which means no tracing)
func WithTracerProvider(tp trace.TracerProvider) WorkFactoryOption {
	return tracerProviderOption{tp: tp}
}

// WithTextMapPropagator provides option to specify the otel text map propagator used by the created work object.
// Defaults to nil (which means no propagation of transport across transports)
func WithTextMapPropagator(p propagation.TextMapPropagator) WorkFactoryOption {
	return wfPropagatorsOption{p}
}

// WithWorkLifecycleHooks provides option to override the lifecycle hooks.  Default is noop.
func WithWorkLifecycleHooks(h LifecycleHooks) WorkFactoryOption {
	return LifecycleHooksOption{h}
}

type loggerOption struct{ l Logger }

func (s loggerOption) apply(wf *WorkFactory) {
	if s.l != nil {
		wf.logger = s.l
	}
}

type tracerProviderOption struct{ tp trace.TracerProvider }

func (s tracerProviderOption) apply(wf *WorkFactory) {
	if s.tp != nil {
		wf.tp = s.tp
	}
}

type wfPropagatorsOption struct {
	p propagation.TextMapPropagator
}

func (s wfPropagatorsOption) apply(wf *WorkFactory) {
	wf.p = s.p
}

type LifecycleHooksOption struct{ h LifecycleHooks }

func (h LifecycleHooksOption) apply(wf *WorkFactory) {
	wf.lifecycle = h.h
}
