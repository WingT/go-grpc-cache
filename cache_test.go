package cache_test

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	cache "github.com/WingT/go-grpc-cache"
	"github.com/WingT/go-grpc-cache/test_pb"
)

func TestMain(m *testing.M) {
	cache.InitFactories()
	os.Exit(m.Run())
}

type testServer struct {
	callCounts map[string]int
}

func (s *testServer) TestStreamEnable(req *test_pb.TestRequest, stream test_pb.TestService_TestStreamEnableServer) error {
	s.callCounts["TestStreamEnable"]++
	count := s.callCounts["TestStreamEnable"]
	for i := int64(0); i < req.Count; i++ {
		err := stream.Send(&test_pb.TestResponse{CallCount: int64(count)})
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *testServer) TestUnaryEnable(ctx context.Context, req *test_pb.TestRequest) (*test_pb.TestResponse, error) {
	s.callCounts["TestUnaryEnable"]++
	count := s.callCounts["TestUnaryEnable"]
	data := make([]byte, req.Count)
	_, err := rand.Read(data)
	if err != nil {
		return nil, err
	}
	return &test_pb.TestResponse{
		Data:      data,
		CallCount: int64(count),
	}, nil
}
func (s *testServer) TestStreamDisable(req *test_pb.TestRequest, stream test_pb.TestService_TestStreamDisableServer) error {
	s.callCounts["TestStreamDisable"]++
	count := s.callCounts["TestStreamDisable"]
	for i := int64(0); i < req.Count; i++ {
		err := stream.Send(&test_pb.TestResponse{CallCount: int64(count)})
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *testServer) TestUnaryDisable(ctx context.Context, req *test_pb.TestRequest) (*test_pb.TestResponse, error) {
	s.callCounts["TestUnaryDisable"]++
	count := s.callCounts["TestUnaryDisable"]
	data := make([]byte, req.Count)
	_, err := rand.Read(data)
	if err != nil {
		return nil, err
	}
	return &test_pb.TestResponse{
		Data:      data,
		CallCount: int64(count),
	}, nil
}

const maxSize = 100

type testCacheConfigOption struct {
	backend cache.Backend
}

func (testCacheConfigOption) StreamConfig(info *grpc.StreamServerInfo) cache.Config {
	return cache.Config{
		Enabled: strings.HasSuffix(info.FullMethod, "Enable"),
		MaxSize: maxSize,
	}
}
func (testCacheConfigOption) UnaryConfig(info *grpc.UnaryServerInfo) cache.Config {
	return cache.Config{
		Enabled: strings.HasSuffix(info.FullMethod, "Enable"),
		MaxSize: maxSize,
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
	if o.backend == nil {
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
	test_pb.RegisterTestServiceServer(server, &testServer{callCounts: make(map[string]int)})
	return server
}

func newClient(t *testing.T, socketPath string) (test_pb.TestServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}

	conn, err := grpc.Dial(socketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}
	return test_pb.NewTestServiceClient(conn), conn
}

type stream interface {
	Recv() (*test_pb.TestResponse, error)
}

func consumeStream(t *testing.T, s stream) ([]*test_pb.TestResponse, error) {
	var resps []*test_pb.TestResponse
	for {
		resp, err := s.Recv()
		if errors.Is(err, io.EOF) {
			return resps, nil
		}
		if err != nil {
			return nil, err
		}
		resps = append(resps, resp)
	}
}

func TestStreamEnable(t *testing.T) {
	opt := new(testCacheConfigOption)
	socketPath, cancel := runTestServer(t, opt)
	defer cancel()
	client, con := newClient(t, socketPath)
	defer con.Close()

	req := &test_pb.TestRequest{
		Enable: true,
		Count:  5,
		Key:    "1",
	}
	c, err := client.TestStreamEnable(context.Background(), req)
	require.NoError(t, err)
	firstResps, err := consumeStream(t, c)
	require.NoError(t, err)
	data, err := opt.Backend().Get(req.Key)
	require.NoError(t, err)
	require.LessOrEqual(t, len(data), maxSize)

	c, err = client.TestStreamEnable(context.Background(), req)
	require.NoError(t, err)
	secondResps, err := consumeStream(t, c)
	require.NoError(t, err)

	require.Equal(t, firstResps, secondResps)
}

func TestStreamEnableReqDisable(t *testing.T) {
	opt := new(testCacheConfigOption)
	socketPath, cancel := runTestServer(t, opt)
	defer cancel()
	client, con := newClient(t, socketPath)
	defer con.Close()

	req := &test_pb.TestRequest{
		Enable: false,
		Count:  5,
		Key:    "1",
	}
	c, err := client.TestStreamEnable(context.Background(), req)
	require.NoError(t, err)
	firstResps, err := consumeStream(t, c)
	require.NoError(t, err)
	_, err = opt.Backend().Get(req.Key)
	require.Equal(t, cache.ErrCacheMiss, err)
	require.Equal(t, int64(1), firstResps[0].CallCount)

	c, err = client.TestStreamEnable(context.Background(), req)
	require.NoError(t, err)
	secondResps, err := consumeStream(t, c)
	require.NoError(t, err)

	require.Equal(t, int64(2), secondResps[0].CallCount)
}

func TestStreamEnableTooLarge(t *testing.T) {
	opt := new(testCacheConfigOption)
	socketPath, cancel := runTestServer(t, opt)
	defer cancel()
	client, con := newClient(t, socketPath)
	defer con.Close()

	req := &test_pb.TestRequest{
		Enable: true,
		Count:  100,
		Key:    "1",
	}
	c, err := client.TestStreamEnable(context.Background(), req)
	require.NoError(t, err)
	firstResps, err := consumeStream(t, c)
	require.NoError(t, err)
	_, err = opt.Backend().Get(req.Key)
	require.Equal(t, cache.ErrCacheMiss, err)
	require.Equal(t, int64(1), firstResps[0].CallCount)

	c, err = client.TestStreamEnable(context.Background(), req)
	require.NoError(t, err)
	secondResps, err := consumeStream(t, c)
	require.NoError(t, err)

	require.Equal(t, int64(2), secondResps[0].CallCount)
}

func TestStreamDisable(t *testing.T) {
	opt := new(testCacheConfigOption)
	socketPath, cancel := runTestServer(t, opt)
	defer cancel()
	client, con := newClient(t, socketPath)
	defer con.Close()

	req := &test_pb.TestRequest{
		Enable: true,
		Count:  5,
		Key:    "1",
	}
	c, err := client.TestStreamDisable(context.Background(), req)
	require.NoError(t, err)
	firstResps, err := consumeStream(t, c)
	require.NoError(t, err)
	_, err = opt.Backend().Get(req.Key)
	require.Equal(t, cache.ErrCacheMiss, err)
	require.Equal(t, int64(1), firstResps[0].CallCount)

	c, err = client.TestStreamDisable(context.Background(), req)
	require.NoError(t, err)
	secondResps, err := consumeStream(t, c)
	require.NoError(t, err)

	require.Equal(t, int64(2), secondResps[0].CallCount)
}

func TestUnaryEnable(t *testing.T) {
	opt := new(testCacheConfigOption)
	socketPath, cancel := runTestServer(t, opt)
	defer cancel()
	client, con := newClient(t, socketPath)
	defer con.Close()

	req := &test_pb.TestRequest{
		Enable: true,
		Count:  5,
		Key:    "1",
	}
	firstResp, err := client.TestUnaryEnable(context.Background(), req)
	require.NoError(t, err)
	data, err := opt.Backend().Get(req.Key)
	require.NoError(t, err)
	require.LessOrEqual(t, len(data), maxSize)

	secondResp, err := client.TestUnaryEnable(context.Background(), req)
	require.NoError(t, err)

	require.Equal(t, firstResp, secondResp)
}

func TestUnaryEnableReqDisable(t *testing.T) {
	opt := new(testCacheConfigOption)
	socketPath, cancel := runTestServer(t, opt)
	defer cancel()
	client, con := newClient(t, socketPath)
	defer con.Close()

	req := &test_pb.TestRequest{
		Enable: false,
		Count:  5,
		Key:    "1",
	}
	firstResp, err := client.TestUnaryEnable(context.Background(), req)
	require.NoError(t, err)
	_, err = opt.Backend().Get(req.Key)
	require.Equal(t, cache.ErrCacheMiss, err)
	require.Equal(t, int64(1), firstResp.CallCount)

	secondResp, err := client.TestUnaryEnable(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, int64(2), secondResp.CallCount)
}

func TestUnaryEnableTooLarge(t *testing.T) {
	opt := new(testCacheConfigOption)
	socketPath, cancel := runTestServer(t, opt)
	defer cancel()
	client, con := newClient(t, socketPath)
	defer con.Close()

	req := &test_pb.TestRequest{
		Enable: true,
		Count:  200,
		Key:    "1",
	}
	firstResp, err := client.TestUnaryEnable(context.Background(), req)
	require.NoError(t, err)
	_, err = opt.Backend().Get(req.Key)
	require.Equal(t, cache.ErrCacheMiss, err)
	require.Equal(t, int64(1), firstResp.CallCount)

	secondResp, err := client.TestUnaryEnable(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, int64(2), secondResp.CallCount)
}

func TestUnaryDisable(t *testing.T) {
	opt := new(testCacheConfigOption)
	socketPath, cancel := runTestServer(t, opt)
	defer cancel()
	client, con := newClient(t, socketPath)
	defer con.Close()

	req := &test_pb.TestRequest{
		Enable: false,
		Count:  5,
		Key:    "1",
	}
	firstResp, err := client.TestUnaryDisable(context.Background(), req)
	require.NoError(t, err)
	_, err = opt.Backend().Get(req.Key)
	require.Equal(t, cache.ErrCacheMiss, err)
	require.Equal(t, int64(1), firstResp.CallCount)

	secondResp, err := client.TestUnaryDisable(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, int64(2), secondResp.CallCount)
}
