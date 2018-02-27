package basictracer

import (
	"encoding/hex"
	"fmt"
	"strings"

	opentracing "github.com/opentracing/opentracing-go"
)

// traceContextPropagator implements the draft Trace-Context header standard from
// https://w3c.github.io/distributed-tracing/report-trace-context.html
type traceContextPropagator struct {
	tracer *tracerImpl
}

const (
	traceContextVersion         = 0
	fieldNameTraceContextParent = "trace-parent"
	fieldNameTraceContextState  = "trace-state"
	fieldNameCorrelationContext = "correlation-context"
)

func (p *traceContextPropagator) Inject(
	spanContext opentracing.SpanContext,
	opaqueCarrier interface{},
) error {
	sc, ok := spanContext.(SpanContext)
	if !ok {
		return opentracing.ErrInvalidSpanContext
	}
	carrier, ok := opaqueCarrier.(opentracing.TextMapWriter)
	if !ok {
		return opentracing.ErrInvalidCarrier
	}
	var tcOptions int
	if sc.Sampled {
		tcOptions = 1
	}
	tcHeader := fmt.Sprintf("%x-%x-%x-%x",
		[]byte{traceContextVersion},
		sc.TraceID[:],
		sc.SpanID[:],
		[]byte{byte(tcOptions)},
	)
	carrier.Set(fieldNameTraceContextParent, tcHeader)

	// propagate W3C Trace Context State, if present
	if sc.TraceContextState != "" {
		carrier.Set(fieldNameTraceContextState, sc.TraceContextState)
	}

	// XXX use Correlation-Context for baggage propagation
	for k, v := range sc.Baggage {
		carrier.Set(prefixBaggage+k, v)
	}
	return nil
}

func (p *traceContextPropagator) Extract(
	opaqueCarrier interface{},
) (opentracing.SpanContext, error) {
	carrier, ok := opaqueCarrier.(opentracing.TextMapReader)
	if !ok {
		return nil, opentracing.ErrInvalidCarrier
	}
	var traceID TraceID
	var spanID SpanID
	var sampled, parentFound bool
	var traceState string
	err := carrier.ForeachKey(func(k, v string) error {
		switch strings.ToLower(k) {
		case fieldNameTraceContextParent:
			var tid, sid []byte
			var ok bool
			tid, sid, sampled, ok = parseTraceContextParent(v)
			if !ok {
				return opentracing.ErrSpanContextCorrupted
			}
			copy(traceID[:], tid)
			copy(spanID[:], sid)
			parentFound = true
		case fieldNameTraceContextState:
			if traceState != "" { // multiple Trace-State headers
				traceState += "," + v // combine as per RFC 7230
			} else {
				traceState = v
			}
		case fieldNameCorrelationContext:
			// URL-decode baggage
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !parentFound {
		return nil, opentracing.ErrSpanContextNotFound
	}
	sc := SpanContext{
		TraceID:           traceID,
		SpanID:            spanID,
		Sampled:           sampled,
		TraceContextState: traceState,
	}
	return sc, nil
}

// parseTraceContextParent parses the W3C Trace Parent HTTP header, following the draft spec
// in https://w3c.github.io/distributed-tracing/report-trace-context.html excerpted below.
func parseTraceContextParent(parentHeader string) (traceID, spanID []byte, sampled, ok bool) {
	// base16(<trace-id>)-base16(<span-id>)[-base16(<trace-options>)]
	// Trace-ID and Span-ID are required. Trace-Options is optional.
	// Character - is used as a delimiter between fields.
	parts := strings.Split(parentHeader, "-")
	if len(parts) != 3 && len(parts) != 4 {
		return nil, nil, false, false
	}

	// Version: a 1-byte representing a 8-bit unsigned integer. Version 255 reserved.
	ver, err := hex.DecodeString(parts[0])
	if err != nil {
		return nil, nil, false, false
	}
	// Currently only version 0 is supported
	if len(ver) != 1 || int(ver[0]) != traceContextVersion {
		return nil, nil, false, false
	}

	// Trace-ID: represented as a 16-bytes array. All bytes 0 is considered invalid.
	// Implementation may decide to completely ignore the Trace-Parent if the trace-id is invalid.
	traceID, err = hex.DecodeString(parts[1])
	if err != nil || len(traceID) != 16 || allZero(traceID) {
		return nil, nil, false, false
	}

	// Span-ID: the ID of the caller span (parent). It is represented as a 8-bytes array.
	// All bytes 0 is considered invalid.
	// Implementation may decide to completely ignore the Trace-Parent if the span-id is invalid.
	spanID, err = hex.DecodeString(parts[2])
	if err != nil || len(spanID) != 16 || allZero(spanID) {
		return nil, nil, false, false
	}

	// Trace-Options: a 1-byte representing a 8-bit unsigned integer.
	// The least significant bit (the 7th bit) provides recommendation whether the request should
	// be traced or not (1 recommends the request should be traced, 0 means the caller does not make
	// a decision to trace and the decision might be deferred). When Trace-Options is missing the
	// default value for this bit is 0.
	// The behavior of other bits is currently undefined.
	if len(parts) == 4 {
		opts, err := hex.DecodeString(parts[3])
		if err != nil || len(opts) != 1 {
			return nil, nil, false, false
		}
		sampled = bool(opts[0]&1 == 1)
	}

	return traceID, spanID, sampled, true
}

// allZero checks if a byte slice is all zeroes, such as invalid trace and span IDs.
func allZero(buf []byte) bool {
	for _, b := range buf {
		if b != 0 {
			return false
		}
	}
	return true
}
