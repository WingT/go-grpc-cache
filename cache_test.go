package cache_test

import (
	"context"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"testing"

	"google.golang.org/grpc"

	cache "github.com/WingT/go-grpc-cache"
	"github.com/WingT/go-grpc-cache/test_pb"
)

func TestMain(m *testing.M) {
	cache.InitFactories()
	os.Exit(m.Run())
}

type testServer struct{}

func (testServer) TestStreamEnable(req *test_pb.TestRequest, stream test_pb.TestService_TestStreamEnableServer) error {
	for i := int64(0); i < req.Count; i++ {
		err := stream.Send(&test_pb.TestResponse{})
		if err != nil {
			return err
		}
	}
	return nil
}
func (testServer) TestUnaryEnable(ctx context.Context, req *test_pb.TestRequest) (*test_pb.TestResponse, error) {
	return &test_pb.TestResponse{
		Data: make([]byte, req.Count),
	}, nil
}
func (testServer) TestStreamDisable(req *test_pb.TestRequest, stream test_pb.TestService_TestStreamDisableServer) error {
	for i := int64(0); i < req.Count; i++ {
		err := stream.Send(&test_pb.TestResponse{})
		if err != nil {
			return err
		}
	}
	return nil
}
func (testServer) TestUnaryDisable(ctx context.Context, req *test_pb.TestRequest) (*test_pb.TestResponse, error) {
	return &test_pb.TestResponse{
		Data: make([]byte, req.Count),
	}, nil
}

type testCacheConfigOption struct {
	backend cache.Backend
}

func (testCacheConfigOption) StreamConfig(info *grpc.StreamServerInfo) cache.Config {
	return cache.Config{
		Enabled: strings.HasSuffix(info.FullMethod, "enable"),
		MaxSize: 100,
	}
}
func (testCacheConfigOption) UnaryConfig(info *grpc.UnaryServerInfo) cache.Config {
	return cache.Config{
		Enabled: strings.HasSuffix(info.FullMethod, "enable"),
		MaxSize: 100,
	}
}
func (testCacheConfigOption) ReqConfig(req interface{}) cache.Config {
	c := cache.Config{}
	switch r := req.(type) {
	case *test_pb.TestRequest:
		c.Enabled = r.Enable
		c.Key = r.Key
	default:
	}
	return c
}
func (o *testCacheConfigOption) Backend() cache.Backend {
	if o.backend != nil {
		o.backend = make(testCacheBackend)
	}
	return o.backend
}

type testCacheBackend map[string][]byte

func (b testCacheBackend) Put(k string, v []byte) error {
	b[k] = v
	return nil
}
func (b testCacheBackend) Get(k string) ([]byte, error) {
	v, ok := b[k]
	if !ok {
		return nil, cache.ErrCacheMiss
	}
	return v, nil
}

func runTestServer(t *testing.T, opt *testCacheConfigOption) (string, func()) {
	socketPath := getTempSocketFileName()
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	server := newTestServer(t, opt)
	go func() {
		_ = server.Serve(listener)
	}()
	cancel := func() {
		server.Stop()
	}
	return "unix://" + socketPath, cancel
}

func getTempSocketFileName() string {
	tmpFile, err := ioutil.TempFile("", "test.socket.")
	if err != nil {
		panic(err)
	}
	name := tmpFile.Name()
	_ = tmpFile.Close()
	_ = os.Remove(name)
	return name
}

func newTestServer(tb testing.TB, opt *testCacheConfigOption) *grpc.Server {
	opts := []grpc.ServerOption{
		grpc.StreamInterceptor(cache.StreamServerInterceptor(opt)),
		grpc.UnaryInterceptor(cache.UnaryServerInterceptor(opt)),
	}
	server := grpc.NewServer(opts...)
	test_pb.RegisterTestServiceServer(server, testServer{})
	return server
}

func TestStreamEnable(t *testing.T) {
	t.Fatal(1)
}

func TestStreamEnableReqDisable(t *testing.T) {
	t.Fatal(1)
}

func TestUnaryEnable(t *testing.T) {
	t.Fatal(1)
}

func TestUnaryEnableReqDisable(t *testing.T) {
	t.Fatal(1)
}

func TestStreamDisable(t *testing.T) {
	t.Fatal(1)
}

func TestUnaryDisable(t *testing.T) {
	t.Fatal(1)
}
