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
	"fmt"
	"io"
	"io/ioutil"
	"reflect"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrCacheMiss = errors.New("cache miss")

type MethodConfig struct {
	Enabled bool // ReqConfig can override
	MaxSize int  // in byte
	MaxAge  int  // in seconds
}

type ReqConfig struct {
	Enabled bool // Overrides MethodConfig
	Key     string
}
type Backend interface {
	Put(string, []byte, MethodConfig) error
	Get(string) ([]byte, error)
}
type Option interface {
	MethodConfig(fullMethod string) MethodConfig
	ReqConfig(fullMethod string, req interface{}) ReqConfig
	Backend() Backend
}

func StreamServerInterceptor(opt Option) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		serverConf := opt.MethodConfig(info.FullMethod)
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
		reqConfig := opt.ReqConfig(info.FullMethod, req)
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
		err = ws.Finish()
		if err != nil {
			return err
		}
		if !ws.RespValid() {
			return nil
		}
		err = opt.Backend().Put(reqConfig.Key, ws.RespData(), serverConf)
		return err
	}
}

func UnaryServerInterceptor(opt Option) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		serverConf := opt.MethodConfig(info.FullMethod)
		if !serverConf.Enabled {
			return handler(ctx, req)
		}
		reqConfig := opt.ReqConfig(info.FullMethod, req)
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
		if len(cacheData) > serverConf.MaxSize {
			return resp, nil
		}
		err = opt.Backend().Put(reqConfig.Key, cacheData, serverConf)
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
	pb         *proto.Buffer
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
	return s.ServerStream.RecvMsg(m)
}
func (s *wrappedServerStream) SendMsg(m interface{}) error {
	err := s.ServerStream.SendMsg(m)
	if err != nil {
		s.err = err
		return err
	}
	if !s.recordResp || s.truncated || s.err != nil {
		return nil
	}
	if s.pb == nil {
		s.pb = proto.NewBuffer(nil)
	}
	s.pb.Reset()
	err = s.pb.EncodeMessage(m.(proto.Message))
	if err != nil {
		s.err = err
		return err
	}
	if s.respWriter == nil {
		s.buffer = bytes.NewBuffer(nil)
		s.respWriter = gzip.NewWriter(s.buffer)
	}
	_, err = s.respWriter.Write(s.pb.Bytes())
	if err != nil {
		s.err = err
		return err
	}
	err = s.respWriter.Flush()
	if err != nil {
		s.err = err
		return err
	}
	if s.buffer.Len() > s.maxSize {
		s.truncated = true
	}
	return nil
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

func (s *wrappedServerStream) Finish() error {
	if s.respWriter != nil {
		err := s.respWriter.Close()
		if err != nil {
			s.err = err
			return fmt.Errorf("compress finish failed: %w", err)
		}
		if s.buffer.Len() > s.maxSize {
			s.truncated = true
		}
	}
	return nil
}

func writeStream(stream grpc.ServerStream, compressedData []byte, m proto.Message) error {
	reader, err := gzip.NewReader(bytes.NewBuffer(compressedData))
	if err != nil {
		return fmt.Errorf("decompress init failed: %w", err)
	}
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("decompress failed: %w", err)
	}
	err = reader.Close()
	if err != nil {
		return fmt.Errorf("decompress finish failed: %w", err)
	}
	pb := proto.NewBuffer(data)
	for len(pb.Unread()) > 0 {
		err = pb.DecodeMessage(m)
		if err != nil {
			return fmt.Errorf("decode message failed: %w", err)
		}
		err = stream.SendMsg(m)
		if err != nil {
			return err
		}
	}
	return nil
}

func serializeMsg(m proto.Message) ([]byte, error) {
	pb := proto.NewBuffer(nil)
	err := pb.EncodeMessage(m)
	if err != nil {
		return nil, fmt.Errorf("encode message faild: %w", err)
	}
	buffer := bytes.NewBuffer(nil)
	writer := gzip.NewWriter(buffer)
	_, err = writer.Write(pb.Bytes())
	if err != nil {
		return nil, fmt.Errorf("compress message faild: %w", err)
	}
	err = writer.Flush()
	if err != nil {
		return nil, fmt.Errorf("compress message flush faild: %w", err)
	}
	err = writer.Close()
	if err != nil {
		return nil, fmt.Errorf("compress message finish faild: %w", err)
	}
	return buffer.Bytes(), nil
}

func deSerializeMsg(compressedData []byte, m proto.Message) error {
	reader, err := gzip.NewReader(bytes.NewBuffer(compressedData))
	if err != nil {
		return fmt.Errorf("decompress init failed: %w", err)
	}
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("decompress failed: %w", err)
	}
	err = reader.Close()
	if err != nil {
		return fmt.Errorf("decompress finish failed: %w", err)
	}
	pb := proto.NewBuffer(data)
	err = pb.DecodeMessage(m)
	if err != nil {
		return fmt.Errorf("decode message failed: %w", err)
	}
	return nil
}
