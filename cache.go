// currently doesn't support rpc which:
// uses header
// or uses trailer
// or has client-side streaming
// or doesn't send/return response(s) at all
package cache

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"reflect"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrCacheMiss = errors.New("cache miss")

type Config struct {
	Enabled bool   // ReqConfig overrides Stream/UnaryConfig
	MaxSize int    // set by Stream/UnaryConfig in byte
	Key     string // set by ReqConfig
}
type Backend interface {
	Put(string, []byte) error
	Get(string) ([]byte, error)
}
type Option interface {
	StreamConfig(*grpc.StreamServerInfo) Config
	UnaryConfig(*grpc.UnaryServerInfo) Config
	ReqConfig(interface{}) Config
	Backend() Backend
}

func StreamServerInterceptor(opt Option) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		serverConf := opt.StreamConfig(info)
		if !serverConf.Enabled {
			return handler(srv, stream)
		}
		ws := newWrappedServerStream(stream)
		req := ReqFactories[info.FullMethod]()
		err := ws.PeekMsg(req)
		if err != nil {
			if err == io.EOF {
				return status.Errorf(codes.Internal, "require initial request to cache")
			}
			return status.Errorf(codes.Internal, "failed getting initial request from caller: %v", err)
		}
		reqConfig := opt.ReqConfig(req)
		if !reqConfig.Enabled {
			return handler(srv, ws)
		}
		cacheData, err := opt.Backend().Get(reqConfig.Key)
		resp := RespFactories[info.FullMethod]()
		if err == nil {
			return writeStream(stream, cacheData, resp)
		}
		if !errors.Is(err, ErrCacheMiss) {
			return err
		}
		ws.SetRecordResp(reqConfig.Enabled)
		ws.SetMaxSize(serverConf.MaxSize)
		err = handler(srv, ws)
		if err != nil {
			return err
		}
		if !ws.RespValid() {
			return nil
		}
		err = opt.Backend().Put(reqConfig.Key, ws.RespData())
		return err
	}
}

func UnaryServerInterceptor(opt Option) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		serverConf := opt.UnaryConfig(info)
		if !serverConf.Enabled {
			return handler(ctx, req)
		}
		reqConfig := opt.ReqConfig(req)
		if !reqConfig.Enabled {
			return handler(ctx, req)
		}
		cacheData, err := opt.Backend().Get(reqConfig.Key)
		if err == nil {
			resp := RespFactories[info.FullMethod]()
			err = deSerializeMsg(cacheData, resp)
			if err != nil {
				return nil, err
			}
			return resp, nil
		}
		if !errors.Is(err, ErrCacheMiss) {
			return nil, err
		}
		resp, err := handler(ctx, req)
		if err != nil {
			return resp, err
		}
		cacheData, err = serializeMsg(resp.(proto.Message))
		if err != nil {
			return nil, err
		}
		err = opt.Backend().Put(reqConfig.Key, cacheData)
		return resp, nil
	}
}

func newWrappedServerStream(stream grpc.ServerStream) *wrappedServerStream {
	return &wrappedServerStream{
		ServerStream: stream,
	}
}

type wrappedServerStream struct {
	grpc.ServerStream
	recordResp bool
	maxSize    int
	peekedMsg  []interface{}
	respData   []byte
	buffer     *bytes.Buffer
	respWriter *gzip.Writer
	truncated  bool
	err        error
}

func (s *wrappedServerStream) PeekMsg(m interface{}) error {
	err := s.ServerStream.RecvMsg(m)
	s.peekedMsg = append(s.peekedMsg, m)
	return err
}
func (s *wrappedServerStream) RecvMsg(m interface{}) error {
	if len(s.peekedMsg) != 0 {
		srcElem := reflect.ValueOf(s.peekedMsg[0]).Elem()
		targetElem := reflect.ValueOf(m).Elem()
		targetElem.Set(srcElem)
		s.peekedMsg = s.peekedMsg[1:]
		return nil
	}
	return s.RecvMsg(m)
}
func (s *wrappedServerStream) SendMsg(m interface{}) error {
	err := s.SendMsg(m)
	if err != nil {
		s.err = err
		return err
	}
	if !s.recordResp || s.truncated || s.err != nil {
		return nil
	}
	data, err := proto.Marshal(m.(proto.Message))
	if err != nil {
		s.err = err
		return err
	}
	if s.respWriter == nil {
		s.buffer = bytes.NewBuffer(nil)
		s.respWriter = gzip.NewWriter(s.buffer)
	}
	s.buffer.Reset()
	s.respWriter.Reset(s.buffer)
	_, err = s.respWriter.Write(data)
	if err != nil {
		s.err = err
		return err
	}
	err = s.respWriter.Flush()
	if err != nil {
		s.err = err
		return err
	}
	if len(s.respData)+s.buffer.Len() > s.maxSize {
		s.truncated = true
	} else {
		s.respData = append(s.respData, s.buffer.Bytes()...)
	}
	return err
}

func (s *wrappedServerStream) RespValid() bool {
	return !s.truncated && s.err == nil
}

func (s *wrappedServerStream) RespData() []byte {
	return s.buffer.Bytes()
}

// mustn't be called after SendMsg
func (s *wrappedServerStream) SetRecordResp(c bool) {
	s.recordResp = c
}

// mustn't be called after SendMsg
func (s *wrappedServerStream) SetMaxSize(m int) {
	s.maxSize = m
}

func writeStream(stream grpc.ServerStream, compressedData []byte, m proto.Message) error {
	reader, err := gzip.NewReader(bytes.NewBuffer(compressedData))
	if err != nil {
		return err
	}
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	pb := proto.NewBuffer(data)
	for i := 0; len(pb.Unread()) > 0; i++ {
		err = pb.DecodeMessage(m)
		if err != nil {
			return err
		}
		err = stream.SendMsg(m)
		if err != nil {
			return err
		}
	}
	return nil
}

func serializeMsg(m proto.Message) ([]byte, error) {
	data, err := proto.Marshal(m)
	if err != nil {
		return nil, err
	}
	buffer := bytes.NewBuffer(nil)
	writer := gzip.NewWriter(buffer)
	_, err = writer.Write(data)
	if err != nil {
		return nil, err
	}
	err = writer.Flush()
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func deSerializeMsg(compressedData []byte, m proto.Message) error {
	reader, err := gzip.NewReader(bytes.NewBuffer(compressedData))
	if err != nil {
		return err
	}
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	return proto.Unmarshal(data, m)
}
