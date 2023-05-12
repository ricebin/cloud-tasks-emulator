package cloud_task_emulator

import (
	"context"
	"net"
	"testing"

	. "cloud.google.com/go/cloudtasks/apiv2"
	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

func RunT(t *testing.T) *Client {
	grpcServ := grpc.NewServer()

	emulatorServer := NewServer()
	emulatorServer.Options = ServerOptions{}
	taskspb.RegisterCloudTasksServer(grpcServ, emulatorServer)

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		if err := grpcServ.Serve(lis); err != nil {
			t.Fatal(err)
		}
	}()

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	clientOpt := option.WithGRPCConn(conn)

	client, err := NewClient(context.Background(), clientOpt)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		grpcServ.Stop()
	})

	return client
}
