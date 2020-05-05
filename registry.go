package cache

import (
	"fmt"
	"reflect"

	"github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

type msgFactory func() proto.Message

var (
	ReqFactories  = map[string]msgFactory{}
	RespFactories = map[string]msgFactory{}
)

// InitFactories must be called first
func InitFactories() {
	protoregistry.GlobalFiles.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		sds := fd.Services()
		for i := 0; i < sds.Len(); i++ {
			sd := sds.Get(i)
			mds := sd.Methods()
			for j := 0; j < mds.Len(); j++ {
				md := mds.Get(j)
				fullMethodName := fmt.Sprintf("/%s/%s", sd.FullName(), md.Name())
				input := md.Input()
				inputType := proto.MessageType(string(input.FullName()))
				ReqFactories[fullMethodName] = genMsgFactory(inputType)
				output := md.Output()
				outputType := proto.MessageType(string(output.FullName()))
				RespFactories[fullMethodName] = genMsgFactory(outputType)
			}
		}
		return true
	})
}

func genMsgFactory(t reflect.Type) msgFactory {
	return func() proto.Message {
		v := reflect.New(t.Elem())
		return v.Interface().(proto.Message)
	}
}
