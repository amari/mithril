package adaptersgrpcserver

import (
	"context"
	"strings"

	"github.com/rs/zerolog"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

func zerologContextWithFullMethod(c zerolog.Context, fullMethod string) zerolog.Context {
	if !strings.HasPrefix(fullMethod, "/") {
		return c
	}

	fullMethod = fullMethod[1:]
	i := strings.Index(fullMethod, "/")
	if i < 0 {
		return c
	}

	service := fullMethod[:i]
	method := fullMethod[i+1:]

	return c.
		Str(string(semconv.RPCServiceKey), service).
		Str(string(semconv.RPCMethodKey), method)
}

func zerologContextWithTraceContext(ctx context.Context, fields zerolog.Context) zerolog.Context {
	span := trace.SpanFromContext(ctx)
	sc := span.SpanContext()

	if !sc.IsValid() {
		return fields
	}

	if sc.TraceID().IsValid() {
		fields = fields.Str("trace.id", sc.TraceID().String())
	}
	if sc.SpanID().IsValid() {
		fields = fields.Str("span.id", sc.SpanID().String())
	}
	// TraceFlags is optional; include only if non-zero
	if sc.TraceFlags() != 0 {
		fields = fields.Str("trace.flags", sc.TraceFlags().String())
	}

	return fields
}

func zerologUnaryServerInterceptor(log *zerolog.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp any, err error) {
		newCtx := zerologContextWithTraceContext(ctx, zerologContextWithFullMethod(log.With(), info.FullMethod)).
			Logger().
			WithContext(ctx)

		return handler(newCtx, req)
	}
}

func zerologStreamServerInterceptor(log *zerolog.Logger) grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		newCtx := zerologContextWithTraceContext(ss.Context(), zerologContextWithFullMethod(log.With(), info.FullMethod)).
			Logger().
			WithContext(ss.Context())

		wrapped := &serverStream{
			ServerStream: ss,
			ctx:          newCtx,
		}

		return handler(srv, wrapped)
	}
}

type serverStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *serverStream) Context() context.Context {
	return s.ctx
}
