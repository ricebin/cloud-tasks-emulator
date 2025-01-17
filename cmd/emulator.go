package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"regexp"
	"strings"

	tasks "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/ricebin/cloud-tasks-emulator/pkg/cloud_task_emulator"
	"google.golang.org/grpc"
)

func main() {
	var initialQueues arrayFlags

	host := flag.String("host", "localhost", "The host name")
	port := flag.String("port", "8123", "The port")
	hardResetOnPurgeQueue := flag.Bool("hard-reset-on-purge-queue", false, "Set to force the 'Purge Queue' call to perform a hard reset of all state (differs from production)")

	flag.Var(&initialQueues, "queue", "A queue to create on startup (repeat as required)")

	flag.Parse()

	lis, err := net.Listen("tcp", fmt.Sprintf("%v:%v", *host, *port))
	if err != nil {
		panic(err)
	}

	print(fmt.Sprintf("Starting cloud tasks emulator, listening on %v:%v\n", *host, *port))

	grpcServer := grpc.NewServer()
	emulatorServer := cloud_task_emulator.NewServer()
	emulatorServer.Options.HardResetOnPurgeQueue = *hardResetOnPurgeQueue
	tasks.RegisterCloudTasksServer(grpcServer, emulatorServer)

	for i := 0; i < len(initialQueues); i++ {
		createInitialQueue(emulatorServer, initialQueues[i])
	}

	grpcServer.Serve(lis)
}

// arrayFlags used for parsing list of potentially repeated flags e.g. -queue $Q1 -queue $Q2
type arrayFlags []string

func (i *arrayFlags) String() string {
	return strings.Join(*i, ", ")
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

// Creates an initial queue on the emulator
func createInitialQueue(emulatorServer *cloud_task_emulator.Server, name string) {
	print(fmt.Sprintf("Creating initial queue %s\n", name))

	r := regexp.MustCompile("/queues/[A-Za-z0-9-]+$")
	parentName := r.ReplaceAllString(name, "")

	queue := &tasks.Queue{Name: name}
	req := &tasks.CreateQueueRequest{
		Parent: parentName,
		Queue:  queue,
	}

	_, err := emulatorServer.CreateQueue(context.TODO(), req)
	if err != nil {
		panic(err)
	}
}
