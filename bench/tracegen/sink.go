package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/mem"
)

// A sink receives one framed request: 4-byte big-endian length followed by the
// serialized ExportTraceServiceRequest. The buffer is reused after return.
type sink interface {
	write(frame []byte, spans int) error
	close() error
}

// rawCodec lets us send the already-encoded request without re-marshalling.
type rawCodec struct{}

func (rawCodec) Name() string { return "proto" }
func (rawCodec) Marshal(v any) (mem.BufferSlice, error) {
	return mem.BufferSlice{mem.SliceBuffer(v.([]byte))}, nil
}
func (rawCodec) Unmarshal(data mem.BufferSlice, v any) error {
	*(v.(*[]byte)) = data.Materialize()
	return nil
}

type grpcSink struct {
	conn     *grpc.ClientConn
	rate     float64
	begin    time.Time
	sent     uint64
	accepted uint64
}

func newGRPCSink(endpoint string, rate float64) (*grpcSink, error) {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(64<<20)))
	if err != nil {
		return nil, err
	}
	return &grpcSink{conn: conn, rate: rate, begin: time.Now()}, nil
}

func (s *grpcSink) write(frame []byte, spans int) error {
	if s.rate > 0 {
		due := s.begin.Add(time.Duration(float64(s.sent) / s.rate * float64(time.Second)))
		time.Sleep(time.Until(due))
	}
	var resp []byte
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	err := s.conn.Invoke(ctx, "/opentelemetry.proto.collector.trace.v1.TraceService/Export",
		frame[4:], &resp, grpc.ForceCodecV2(rawCodec{}))
	if err != nil {
		return fmt.Errorf("otlp export: %w", err)
	}
	er := ptraceotlp.NewExportResponse()
	if err := er.UnmarshalProto(resp); err != nil {
		return fmt.Errorf("decode export response: %w", err)
	}
	rejected := uint64(er.PartialSuccess().RejectedSpans())
	if rejected > 0 {
		return fmt.Errorf("server rejected %d spans: %s", rejected, er.PartialSuccess().ErrorMessage())
	}
	s.sent += uint64(spans)
	s.accepted += uint64(spans)
	return nil
}

func (s *grpcSink) close() error { return s.conn.Close() }

// newBufFileSink wraps the file in a large write buffer.
func newBufFileSink(f *os.File) sink { return &bufFileSink{f: f, w: bufio.NewWriterSize(f, 4<<20)} }

type bufFileSink struct {
	f *os.File
	w *bufio.Writer
}

func (s *bufFileSink) write(frame []byte, _ int) error { _, err := s.w.Write(frame); return err }
func (s *bufFileSink) close() error {
	err := s.w.Flush()
	if cerr := s.f.Close(); err == nil {
		err = cerr
	}
	return err
}
