package cloud_task_emulator_test

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	. "cloud.google.com/go/cloudtasks/apiv2"
	taskspb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	. "github.com/ricebin/cloud-tasks-emulator/pkg/cloud_task_emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
)

var formattedParent = formatParent("TestProject", "TestLocation")

func TestCloudTasksCreateQueue(t *testing.T) {
	client := RunT(t)

	queue := newQueue(formattedParent, "testCloudTasksCreateQueue")
	request := taskspb.CreateQueueRequest{
		Parent: formattedParent,
		Queue:  queue,
	}

	resp, err := client.CreateQueue(context.Background(), &request)
	require.NoError(t, err)
	assert.Equal(t, request.GetQueue().Name, resp.Name)
	assert.Equal(t, taskspb.Queue_RUNNING, resp.State)
}

func TestCreateTask(t *testing.T) {
	client := RunT(t)

	createdQueue := createTestQueue(t, client)

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: createdQueue.GetName(),
		Task: &taskspb.Task{
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url: "http://www.google.com",
				},
			},
		},
	}

	createdTask, err := client.CreateTask(context.Background(), &createTaskRequest)
	require.NoError(t, err)
	assert.NotEmpty(t, createdTask.GetName())
	assert.Contains(t, createdTask.GetName(), "projects/TestProject/locations/TestLocation/queues/test/tasks/")
	assert.Equal(t, "http://www.google.com", createdTask.GetHttpRequest().GetUrl())
	assert.Equal(t, taskspb.HttpMethod_POST, createdTask.GetHttpRequest().GetHttpMethod())
	assert.EqualValues(t, 0, createdTask.GetDispatchCount())
}

func TestCreateTaskRejectsDuplicateName(t *testing.T) {
	client := RunT(t)

	createdQueue := createTestQueue(t, client)

	testServerUrl, receivedRequests := startTestServer(t)

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: createdQueue.GetName(),
		Task: &taskspb.Task{
			Name: createdQueue.GetName() + "/tasks/dedupe-this-task",
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url: testServerUrl + "/success",
				},
			},
		},
	}

	createdTask, err := client.CreateTask(context.Background(), &createTaskRequest)
	require.NoError(t, err)

	// First creation worked OK

	dupeTask, err := client.CreateTask(context.Background(), &createTaskRequest)

	assert.Nil(t, dupeTask)
	assertIsGrpcError(t, "^Requested entity already exists", grpcCodes.AlreadyExists, err)

	// Wait for it to perform the http request
	_, err = awaitHttpRequest(receivedRequests)
	require.NoError(t, err)

	// Check the task has been removed now (to ensure state is valid for the
	// recreate-even-after-executed-and-removed case following)
	getTaskRequest := taskspb.GetTaskRequest{
		Name: createdTask.GetName(),
	}
	gettedTask, err := client.GetTask(context.Background(), &getTaskRequest)
	assert.Error(t, err)
	assert.Nil(t, gettedTask)

	// Check still can't create even after removal
	_, err = client.CreateTask(context.Background(), &createTaskRequest)
	assertIsGrpcError(t, "^Requested entity already exists", grpcCodes.AlreadyExists, err)

	// Verify that it only sent the original HTTP request, nothing after that
	_, err = awaitHttpRequestWithTimeout(receivedRequests, 1*time.Second)
	assert.Error(t, err, "Should not receive any further HTTP requests within timeout")
}

func TestCreateTaskRejectsInvalidName(t *testing.T) {
	client := RunT(t)

	createdQueue := createTestQueue(t, client)

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: createdQueue.GetName(),
		Task: &taskspb.Task{
			Name: "is-this-a-name",
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url: "http://www.google.com",
				},
			},
		},
	}

	createdTask, err := client.CreateTask(context.Background(), &createTaskRequest)

	assert.Nil(t, createdTask)
	assertIsGrpcError(t, "^Task name must be formatted", grpcCodes.InvalidArgument, err)
}

func TestCreateTaskRejectsNameForOtherQueue(t *testing.T) {
	client := RunT(t)

	createdQueue := createTestQueue(t, client)

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: createdQueue.GetName(),
		Task: &taskspb.Task{
			Name: "projects/TestProject/locations/TestLocation/queues/SomeOtherQueue/tasks/valid-name",
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url: "http://www.google.com",
				},
			},
		},
	}

	createdTask, err := client.CreateTask(context.Background(), &createTaskRequest)

	assert.Nil(t, createdTask)
	assertIsGrpcError(t, "^The queue name from request", grpcCodes.InvalidArgument, err)
}

func TestGetQueueExists(t *testing.T) {
	client := RunT(t)
	createdQueue := createTestQueue(t, client)

	getQueueRequest := taskspb.GetQueueRequest{
		Name: createdQueue.GetName(),
	}

	gettedQueue, err := client.GetQueue(context.Background(), &getQueueRequest)

	assert.NoError(t, err)
	assert.Equal(t, createdQueue.GetName(), gettedQueue.GetName())
}

func TestGetQueueNeverExisted(t *testing.T) {
	client := RunT(t)

	getQueueRequest := taskspb.GetQueueRequest{
		Name: "hello_q",
	}

	gettedQueue, err := client.GetQueue(context.Background(), &getQueueRequest)

	assert.Nil(t, gettedQueue)
	st, _ := grpcStatus.FromError(err)
	assert.Equal(t, grpcCodes.NotFound, st.Code())
}

func TestGetQueuePreviouslyExisted(t *testing.T) {
	client := RunT(t)

	createdQueue := createTestQueue(t, client)

	deleteQueueRequest := taskspb.DeleteQueueRequest{
		Name: createdQueue.GetName(),
	}

	err := client.DeleteQueue(context.Background(), &deleteQueueRequest)

	assert.NoError(t, err)

	getQueueRequest := taskspb.GetQueueRequest{
		Name: createdQueue.GetName(),
	}

	gettedQueue, err := client.GetQueue(context.Background(), &getQueueRequest)

	assert.Nil(t, gettedQueue)
	st, _ := grpcStatus.FromError(err)
	assert.Equal(t, grpcCodes.NotFound, st.Code())
}

func TestPurgeQueueDoesNotReleaseTaskNamesByDefault(t *testing.T) {
	client := RunT(t)

	createdQueue := createTestQueue(t, client)

	testServerUrl, receivedRequests := startTestServer(t)

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: createdQueue.GetName(),
		Task: &taskspb.Task{
			Name: createdQueue.GetName() + "/tasks/any-task",
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					// Use the not_found handler to prove that purge stops any further retries
					Url: testServerUrl + "/not_found",
				},
			},
		},
	}
	createdTask, err := client.CreateTask(context.Background(), &createTaskRequest)
	require.NoError(t, err)

	// Task was created OK, verify that the first HTTP request was sent
	_, err = awaitHttpRequest(receivedRequests)
	require.NoError(t, err)

	// Now purge the queue
	purgeQueueRequest := taskspb.PurgeQueueRequest{
		Name: createdQueue.GetName(),
	}
	_, err = client.PurgeQueue(context.Background(), &purgeQueueRequest)
	require.NoError(t, err)

	// Wait a moment for that to work, then verify nothing in the list and cannot retrieve by name
	time.Sleep(100 * time.Millisecond)
	assertTaskListIsEmpty(t, client, createdQueue)
	assertGetTaskFails(t, grpcCodes.FailedPrecondition, client, createdTask.GetName())

	// BUT - Verify that the task name is still not available for new tasks
	_, err = client.CreateTask(context.Background(), &createTaskRequest)
	assertIsGrpcError(t, "^Requested entity already exists", grpcCodes.AlreadyExists, err)

	// Verify that it only sent the original HTTP request, it purged before the retries
	_, err = awaitHttpRequestWithTimeout(receivedRequests, 1*time.Second)
	assert.Error(t, err, "Should not receive any further HTTP requests within timeout")
}

func TestPurgeQueueOptionallyPerformsHardReset(t *testing.T) {
	client := RunT(t)

	createdQueue := createTestQueue(t, client)

	testServerUrl, receivedRequests := startTestServer(t)

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: createdQueue.GetName(),
		Task: &taskspb.Task{
			Name: createdQueue.GetName() + "/tasks/any-task",
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					// Use the not_found handler to prove that purge stops any further retries
					Url: testServerUrl + "/not_found",
				},
			},
		},
	}
	createdTask, err := client.CreateTask(context.Background(), &createTaskRequest)
	require.NoError(t, err)

	// Task was created OK, verify that the first HTTP request was sent
	_, err = awaitHttpRequest(receivedRequests)
	require.NoError(t, err)

	// Now purge the queue
	purgeQueueRequest := taskspb.PurgeQueueRequest{
		Name: createdQueue.GetName(),
	}
	_, err = client.PurgeQueue(context.Background(), &purgeQueueRequest)
	require.NoError(t, err)

	// In this mode, purging the queue is synchronous so we should be in the empty state straight away
	time.Sleep(1 * time.Second)
	assertTaskListIsEmpty(t, client, createdQueue)

	// task id is not immediately available for re-use
	// https://cloud.google.com/tasks/docs/reference/rest/v2beta3/projects.locations.queues.tasks/create#body.request_body.FIELDS.task
	assertGetTaskFails(t, grpcCodes.FailedPrecondition, client, createdTask.GetName())

	// TODO(ricebin) what should we test here?
	// And verify that we can now create the task with that name again and it will fire again
	//_, err = client.CreateTask(context.Background(), &createTaskRequest)
	//require.NoError(t, err)
	//
	//// Verify that it has now sent the request from the new task
	//receivedRequest, err := awaitHttpRequest(receivedRequests)
	//require.NotNil(t, receivedRequest, "Request was received")
	//require.NoError(t, err)
	//// Note that the execution count is reset to 0
	//assertHeadersMatch(
	//	t,
	//	map[string]string{
	//		"X-CloudTasks-TaskExecutionCount": "0",
	//		"X-CloudTasks-TaskRetryCount":     "0",
	//	},
	//	receivedRequest,
	//)
}

func TestListTasks(t *testing.T) {
	client := RunT(t)

	createdQueue := createTestQueue(t, client)

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: createdQueue.GetName(),
		Task: &taskspb.Task{
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url: "http://does.not.exist/",
				},
			},
		},
	}

	createdTask1, err := client.CreateTask(context.Background(), &createTaskRequest)
	require.NoError(t, err)

	{
		listTasksRequest := taskspb.ListTasksRequest{
			Parent: createdQueue.GetName(),
		}

		tasksIterator := client.ListTasks(context.Background(), &listTasksRequest)
		assert.NoError(t, err)

		listedTask, err := tasksIterator.Next()
		assert.NoError(t, err)
		assert.Equal(t, listedTask.GetName(), createdTask1.GetName())
		_, err = tasksIterator.Next()
		assert.EqualError(t, err, "no more items in iterator")
	}

	_, err = client.CreateTask(context.Background(), &createTaskRequest)
	require.NoError(t, err)

	{
		listTasksRequest := taskspb.ListTasksRequest{
			Parent:   createdQueue.GetName(),
			PageSize: 1,
		}

		page := client.ListTasks(context.Background(), &listTasksRequest)

		all, next := drainAsList(t, page)
		assert.Empty(t, next)
		assert.Len(t, all, 2)
	}
}

func TestSuccessTaskExecution(t *testing.T) {
	client := RunT(t)

	testServerUrl, receivedRequests := startTestServer(t)

	createdQueue := createTestQueue(t, client)

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: createdQueue.GetName(),
		Task: &taskspb.Task{
			Name: createdQueue.GetName() + "/tasks/my-test-task",
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url: testServerUrl + "/success",
				},
			},
		},
	}
	createdTask, err := client.CreateTask(context.Background(), &createTaskRequest)
	require.NoError(t, err)

	getTaskRequest := taskspb.GetTaskRequest{
		Name: createdTask.GetName(),
	}

	receivedRequest, err := awaitHttpRequest(receivedRequests)
	require.NoError(t, err)

	gettedTask, err := client.GetTask(context.Background(), &getTaskRequest)
	assert.Error(t, err)
	assert.Nil(t, gettedTask)

	// Validate that the call was actually made properly
	require.NotNil(t, receivedRequest, "Request was received")

	// Simple predictable headers
	assertHeadersMatch(
		t,
		map[string]string{
			"X-CloudTasks-TaskExecutionCount": "0",
			"X-CloudTasks-TaskRetryCount":     "0",
			"X-CloudTasks-TaskName":           "my-test-task",
			"X-CloudTasks-QueueName":          "test",
		},
		receivedRequest,
	)
	assertIsRecentTimestamp(t, receivedRequest.Header.Get("X-CloudTasks-TaskETA"))
}

func TestSuccessAppEngineTaskExecution(t *testing.T) {
	client := RunT(t)

	testServerUrl, receivedRequests := startTestServer(t)

	defer os.Unsetenv("APP_ENGINE_EMULATOR_HOST")
	os.Setenv("APP_ENGINE_EMULATOR_HOST", testServerUrl)

	createdQueue := createTestQueue(t, client)

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: createdQueue.GetName(),
		Task: &taskspb.Task{
			Name: createdQueue.GetName() + "/tasks/my-test-task",
			MessageType: &taskspb.Task_AppEngineHttpRequest{
				AppEngineHttpRequest: &taskspb.AppEngineHttpRequest{
					RelativeUri: "/success",
				},
			},
		},
	}

	createdTask, err := client.CreateTask(context.Background(), &createTaskRequest)
	require.NoError(t, err)
	assert.NotNil(t, createdTask)

	// Wait for it to perform the http request
	receivedRequest, err := awaitHttpRequest(receivedRequests)
	require.NoError(t, err)

	require.NotNil(t, receivedRequest, "Request was received")
	assertHeadersMatch(
		t,
		map[string]string{
			"X-AppEngine-TaskExecutionCount": "0",
			"X-AppEngine-TaskRetryCount":     "0",
			"X-AppEngine-TaskName":           "my-test-task",
			"X-AppEngine-QueueName":          "test",
		},
		receivedRequest,
	)

	assertIsRecentTimestamp(t, receivedRequest.Header.Get("X-AppEngine-TaskETA"))
}

func TestErrorTaskExecution(t *testing.T) {
	client := RunT(t)

	testServerUrl, receivedRequests := startTestServer(t)

	createdQueue := createTestQueue(t, client)

	createTaskRequest := taskspb.CreateTaskRequest{
		Parent: createdQueue.GetName(),
		Task: &taskspb.Task{
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Url: testServerUrl + "/not_found",
				},
			},
		},
	}

	start := time.Now()

	createdTask, err := client.CreateTask(context.Background(), &createTaskRequest)
	require.NoError(t, err)

	// With the default retry backoff, we expect 4 calls within the first second:
	// at t=0, 0.1, 0.3 (+0.2), 0.7 (+0.4) seconds (plus some buffer) ==> 4 calls
	receivedRequest, err := awaitHttpRequest(receivedRequests)
	require.NoError(t, err, "Should have received request 1")
	assertHeadersMatch(
		t,
		map[string]string{
			"X-CloudTasks-TaskExecutionCount": "0",
			"X-CloudTasks-TaskRetryCount":     "0",
		},
		receivedRequest,
	)

	receivedRequest, err = awaitHttpRequest(receivedRequests)
	require.NoError(t, err, "Should have received request 2")
	assertHeadersMatch(
		t,
		map[string]string{
			"X-CloudTasks-TaskExecutionCount": "1",
			"X-CloudTasks-TaskRetryCount":     "1",
		},
		receivedRequest,
	)

	receivedRequest, err = awaitHttpRequest(receivedRequests)
	require.NoError(t, err, "Should have received request 3")
	assertHeadersMatch(
		t,
		map[string]string{
			"X-CloudTasks-TaskExecutionCount": "2",
			"X-CloudTasks-TaskRetryCount":     "2",
		},
		receivedRequest,
	)

	receivedRequest, err = awaitHttpRequest(receivedRequests)
	require.NoError(t, err, "Should have received request 4")
	assertHeadersMatch(
		t,
		map[string]string{
			"X-CloudTasks-TaskExecutionCount": "3",
			"X-CloudTasks-TaskRetryCount":     "3",
		},
		receivedRequest,
	)

	expectedCompleteBy := start.Add(700 * time.Millisecond)
	assert.WithinDuration(
		t,
		expectedCompleteBy,
		time.Now(),
		200*time.Millisecond,
		"4 retries should take roughly 0.7 seconds",
	)

	// Check the state of the task has been updated with the number of dispatches
	getTaskRequest := taskspb.GetTaskRequest{
		Name: createdTask.GetName(),
	}
	gettedTask, err := client.GetTask(context.Background(), &getTaskRequest)
	require.NoError(t, err)
	assert.EqualValues(t, 4, gettedTask.GetDispatchCount())
}

func newQueue(formattedParent, name string) *taskspb.Queue {
	return &taskspb.Queue{Name: formatQueueName(formattedParent, name)}
}

func formatQueueName(formattedParent, name string) string {
	return fmt.Sprintf("%s/queues/%s", formattedParent, name)
}

func formatParent(project, location string) string {
	return fmt.Sprintf("projects/%s/locations/%s", project, location)
}

func assertHeadersMatch(t *testing.T, expectHeaders map[string]string, request *http.Request) {
	actualHeaders := make(map[string]string)

	for hdr := range expectHeaders {
		actualHeaders[hdr] = request.Header.Get(hdr)
	}

	assert.Equal(t, expectHeaders, actualHeaders)
}

func assertIsRecentTimestamp(t *testing.T, etaString string) {
	assert.Regexp(t, "^[0-9]+\\.[0-9]+$", etaString)
	float, err := strconv.ParseFloat(etaString, 64)
	require.NoError(t, err)
	seconds, fraction := math.Modf(float)
	etaTime := time.Unix(int64(seconds), int64(fraction*1e9))

	assert.WithinDuration(
		t,
		time.Now(),
		etaTime,
		2*time.Second,
		"task eta should be within last few seconds",
	)
}

func assertIsGrpcError(t *testing.T, expectMessageRegexp string, expectCode grpcCodes.Code, err error) {
	require.Error(t, err, "Should return error")
	rsp, ok := grpcStatus.FromError(err)
	require.True(t, ok, "Should be grpc error")
	assert.Regexp(t, expectMessageRegexp, rsp.Message())
	assert.Equal(t, expectCode, rsp.Code(), "Expected code %s, got %s", expectCode.String(), rsp.Code().String())
}

func assertTaskListIsEmpty(t *testing.T, client *Client, queue *taskspb.Queue) {
	listTasksRequest := taskspb.ListTasksRequest{
		Parent: queue.GetName(),
	}
	tasksIterator := client.ListTasks(context.Background(), &listTasksRequest)
	firstTask, err := tasksIterator.Next()
	assert.Nil(t, firstTask, "Should not get a task in the tasks list")
	assert.Same(t, iterator.Done, err, "task iterator should be done")
}

func assertGetTaskFails(t *testing.T, expectCode grpcCodes.Code, client *Client, name string) {
	getTaskRequest := taskspb.GetTaskRequest{
		Name: name,
	}
	gettedTask, err := client.GetTask(context.Background(), &getTaskRequest)
	if assert.Error(t, err) {
		rsp, ok := grpcStatus.FromError(err)
		assert.True(t, ok, "Should be grpc error")
		assert.Equal(t, expectCode, rsp.Code())
	}
	assert.Nil(t, gettedTask)
}

func createTestQueue(t *testing.T, client *Client) *taskspb.Queue {
	queue := newQueue(formattedParent, "test")

	createQueueRequest := taskspb.CreateQueueRequest{
		Parent: formattedParent,
		Queue:  queue,
	}

	createdQueue, err := client.CreateQueue(context.Background(), &createQueueRequest)
	require.NoError(t, err)

	return createdQueue
}

func awaitHttpRequest(receivedRequests <-chan *http.Request) (*http.Request, error) {
	return awaitHttpRequestWithTimeout(receivedRequests, 1*time.Second)
}

func awaitHttpRequestWithTimeout(receivedRequests <-chan *http.Request, timeout time.Duration) (*http.Request, error) {
	select {
	case request := <-receivedRequests:
		// Wait a few ticks for the emulator to receive & process the http response (the request
		// was written to the channel before we sent the response back)
		time.Sleep(20 * time.Millisecond)
		return request, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("Timed out waiting for HTTP request after %s", timeout)
	}
}

func startTestServer(t *testing.T) (string, <-chan *http.Request) {
	mux := http.NewServeMux()
	requestChannel := make(chan *http.Request, 1)
	mux.HandleFunc("/success", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		requestChannel <- r
	})
	mux.HandleFunc("/not_found", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
		requestChannel <- r
	})

	s := httptest.NewServer(mux)
	t.Cleanup(func() {
		s.Close()
	})
	return s.URL, requestChannel
}

func drainAsList(t *testing.T, it *TaskIterator) ([]*taskspb.Task, string) {
	var out []*taskspb.Task
	for {
		if n, err := it.Next(); err == iterator.Done {
			break
		} else if err != nil {
			t.Fatal(err)
		} else {
			out = append(out, n)
		}
	}
	var next string
	if it.PageInfo() != nil {
		next = it.PageInfo().Token
	}
	return out, next
}
