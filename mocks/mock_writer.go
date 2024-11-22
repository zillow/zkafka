// Code generated by MockGen. DO NOT EDIT.
// Source: kwriter.go
//
// Generated by this command:
//
//	mockgen -source=kwriter.go -package mock_zkafka -mock_names writer=MockWriter -destination=./mock_writer.go
//

// Package mock_zkafka is a generated GoMock package.
package mock_zkafka

import (
	context "context"
	reflect "reflect"

	zkafka "github.com/zillow/zkafka/v2"
	gomock "go.uber.org/mock/gomock"
)

// MockWriter is a mock of writer interface.
type MockWriter struct {
	ctrl     *gomock.Controller
	recorder *MockWriterMockRecorder
	isgomock struct{}
}

// MockWriterMockRecorder is the mock recorder for MockWriter.
type MockWriterMockRecorder struct {
	mock *MockWriter
}

// NewMockWriter creates a new mock instance.
func NewMockWriter(ctrl *gomock.Controller) *MockWriter {
	mock := &MockWriter{ctrl: ctrl}
	mock.recorder = &MockWriterMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockWriter) EXPECT() *MockWriterMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockWriter) Close() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Close")
}

// Close indicates an expected call of Close.
func (mr *MockWriterMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockWriter)(nil).Close))
}

// Write mocks base method.
func (m *MockWriter) Write(ctx context.Context, value any, opts ...zkafka.WriteOption) (zkafka.Response, error) {
	m.ctrl.T.Helper()
	varargs := []any{ctx, value}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Write", varargs...)
	ret0, _ := ret[0].(zkafka.Response)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Write indicates an expected call of Write.
func (mr *MockWriterMockRecorder) Write(ctx, value any, opts ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{ctx, value}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Write", reflect.TypeOf((*MockWriter)(nil).Write), varargs...)
}

// WriteKey mocks base method.
func (m *MockWriter) WriteKey(ctx context.Context, key string, value any, opts ...zkafka.WriteOption) (zkafka.Response, error) {
	m.ctrl.T.Helper()
	varargs := []any{ctx, key, value}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "WriteKey", varargs...)
	ret0, _ := ret[0].(zkafka.Response)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// WriteKey indicates an expected call of WriteKey.
func (mr *MockWriterMockRecorder) WriteKey(ctx, key, value any, opts ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{ctx, key, value}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WriteKey", reflect.TypeOf((*MockWriter)(nil).WriteKey), varargs...)
}

// WriteRaw mocks base method.
func (m *MockWriter) WriteRaw(ctx context.Context, key *string, value []byte, opts ...zkafka.WriteOption) (zkafka.Response, error) {
	m.ctrl.T.Helper()
	varargs := []any{ctx, key, value}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "WriteRaw", varargs...)
	ret0, _ := ret[0].(zkafka.Response)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// WriteRaw indicates an expected call of WriteRaw.
func (mr *MockWriterMockRecorder) WriteRaw(ctx, key, value any, opts ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{ctx, key, value}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WriteRaw", reflect.TypeOf((*MockWriter)(nil).WriteRaw), varargs...)
}
