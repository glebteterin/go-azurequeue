package queue

import (
	"testing"
	"time"
	"net/http"
	"io/ioutil"
	"bytes"
	"reflect"
	"sync"
	"strconv"
	"os"
	"fmt"
)

var q = QueueClient{
	Namespace:  "test",
	KeyName:    "key",
	KeyValue:   "keyvalue",
	QueueName:  "test",
	Timeout:    0,
	mu:         sync.Mutex{},
	httpClient: nil}

var loc, _ = time.LoadLocation("GMT")

var testMsg = Message{
	Id:                      "{701332E1-B37B-4D29-AA0A-E367906C206E}",
	SessionId:               "{27729E1-B37B-4D29-AA0A-E367906C206E}",
	TimeToLive:              90,
	CorrelationId:           "{701332F3-B37B-4D29-AA0A-E367906C206E}",
	SequenceNumber:          int64(12345),
	DeliveryCount:           2,
	To:                      "http://contoso.com",
	ReplyTo:                 "http://fabrikam.com",
	EnqueuedTimeUtc:         time.Date(1994, 11, 6, 8, 49, 37, 0, loc),
	ScheduledEnqueueTimeUtc: time.Date(1994, 11, 6, 8, 49, 37, 0, loc),

	Properties: map[string]string{"Prop1": "Value1"},
}

var brokerProps = fmt.Sprintf("{ \"SessionId\": \"%s\", \"MessageId\": \"%s\", \"TimeToLive\" : %v, \"CorrelationId\": \"%s\", \"SequenceNumber\" : %v, \"DeliveryCount\" : %v, \"To\" : \"%s\", \"ReplyTo\" : \"%s\",  \"EnqueuedTimeUtc\" : \"%s\", \"ScheduledEnqueueTimeUtc\" : \"%s\"}",
	testMsg.SessionId,
	testMsg.Id,
	testMsg.TimeToLive,
	testMsg.CorrelationId,
	testMsg.SequenceNumber,
	testMsg.DeliveryCount,
	testMsg.To,
	testMsg.ReplyTo,
	"Sun, 06 Nov 1994 08:49:37 GMT",
	"Sun, 06 Nov 1994 08:49:37 GMT")

type errorCase struct {
	RespCode int
	Error    reflect.Type
	Body     string
}

var errorTestCases = []errorCase{
	errorCase{204, reflect.TypeOf(NoMessagesAvailableError{}), "204"},
	errorCase{400, reflect.TypeOf(BadRequestError{}), "400"},
	errorCase{401, reflect.TypeOf(NotAuthorizedError{}), "401"},
	errorCase{404, reflect.TypeOf(MessageDontExistError{}), "404"},
	errorCase{410, reflect.TypeOf(QueueDontExistError{}), "410"},
	errorCase{500, reflect.TypeOf(InternalError{}), "500"},
}

func TestMain(m *testing.M) {
	SetDebugLogger(nil)

	retCode := m.Run()
	os.Exit(retCode)
}

func Test_createRequest(t *testing.T) {

	host := "test.servicebus.windows.net:443"
	method := "POST"

	req, err := q.createRequest("messages/head?timeout=0", method)

	if err != nil {
		t.Fatal(err)
	}

	if req.Host != host {
		t.Fatalf("Expected host %s but got %s", host, req.Host)
	}

	if req.Method != method {
		t.Fatalf("Expected method %s but got %s", method, req.Method)
	}
}

func Test_createRequestFromMessage(t *testing.T) {

	host := "test.servicebus.windows.net:443"
	method := "POST"

	req, err := q.createRequestFromMessage("messages/abc/efg", method, &testMsg)

	if err != nil {
		t.Fatal(err)
	}

	if req.Host != host {
		t.Fatalf("Expected host %s but got %s", host, req.Host)
	}

	if req.Method != method {
		t.Fatalf("Expected method %s but got %s", method, req.Method)
	}

	for k, _ := range testMsg.Properties {
		if req.Header.Get(k) != testMsg.Properties[k] {
			t.Fatalf("Expected header %s value %s but got %s", k, testMsg.Properties[k], req.Header.Get(k))
		}
	}
}

func Test_parseMessage(t *testing.T) {

	resp := http.Response{
		Header: http.Header{
			"Brokerproperties": []string{brokerProps},
			"Prop1":            []string{"Value1"},
		},
		Body: ioutil.NopCloser(bytes.NewBufferString("Hello World")),
	}

	msg, err := parseMessage(&resp)

	if err != nil {
		t.Error(err)
	}

	compareMsg(t, &testMsg, msg, false)
}

func Test_parseBrokerProperties(t *testing.T) {

	msg := &Message{}

	parseBrokerProperties(msg, brokerProps)

	compareMsg(t, &testMsg, msg, true)
}

func Test_authentication(t *testing.T) {

	from := time.Date(2018, 1, 1, 1, 1, 1, 0, loc)
	expectedHeader := "SharedAccessSignature sig=7n5v6bQCFOLIameIxwGwxiNA14HzFn5Zztuv%2Fvsqp%2F8%3D&se=1514768761&skn=key&sr=https%3a%2f%2ftest.servicebus.windows.net%3a443%2ftest%2f"
	expectedSignature := "kdSuuUQda%2FPnrx%2BjPi5qaRCyclvMwUV89nYRlm8jlbc%3D"
	url := "https://test.servicebus.windows.net:443/test/"

	sig := q.makeSignatureString(url + "\n" + strconv.Itoa(int(from.Unix())))
	header := q.makeAuthHeader(url, from)

	if sig != expectedSignature {
		t.Fatalf("Expected signature %s but got %s", expectedSignature, sig)
	}

	if header != expectedHeader {
		t.Fatalf("Expected header %s but got %s", expectedHeader, header)
	}
}

func Test_handleStatusCode_error(t *testing.T) {
	for _, tCase := range errorTestCases {

		resp := http.Response{
			StatusCode: tCase.RespCode,
			Body:       ioutil.NopCloser(bytes.NewBufferString(tCase.Body)),
		}

		var err error
		if err = handleStatusCode(&resp); err == nil {
			t.Fatalf("Expected error type %s but got nil", tCase.Error)
		}

		if reflect.TypeOf(err) != tCase.Error {
			t.Fatalf("Expected error type %s but got %s", tCase.Error, reflect.TypeOf(err))
		}
	}
}

func Test_handleStatusCode_ok(t *testing.T) {

	resp := http.Response{
		StatusCode: 200,
		Body:       ioutil.NopCloser(bytes.NewBufferString("")),
	}

	var err error
	if err = handleStatusCode(&resp); err != nil {
		t.Fatal(err)
	}
}

func Test_handleStatusCode_unknown(t *testing.T) {

	resp := http.Response{
		StatusCode: 501,
		Body:       ioutil.NopCloser(bytes.NewBufferString("hello")),
	}

	var err error
	if err = handleStatusCode(&resp); err == nil {
		t.Fatalf("Expected error type %s but got nil", err)
	}

	if err.Error() != "Unknown status 501 with body hello" {
		t.Fatal(err)
	}
}

func Test_getClient_override(t *testing.T) {

	var clientOverride HttpClient
	clientOverride = &http.Client{}

	SetHttpClient(clientOverride)

	c := q.getClient()

	if c != clientOverride {
		t.Fatal("getClient() supposed to return client override")
	}
}

func Test_getClient_default(t *testing.T) {

	SetHttpClient(nil)

	c := q.getClient()

	if c == nil {
		t.Fatal("getClient() supposed to return default client")
	}
}

func Test_SendReceive(t *testing.T) {

	t.Skip("Real parameters required")

	namespace := ""
	keyName := ""
	keyValue := "="
	queueName := ""

	cli := QueueClient{
		Namespace:  namespace,
		KeyName:    keyName,
		KeyValue:   keyValue,
		QueueName:  queueName,
		Timeout:    60,
		mu:         sync.Mutex{},
		httpClient: &http.Client{},
	}

	msgSend := Message{}
	msgSend.Properties = map[string]string{
		"Prop1": "Value1",
		"Prop2": "Value2",
	}
	msgSend.Body = []byte("Hello!")

	err := cli.SendMessage(&msgSend)

	if err != nil {
		t.Fatal(err)
	}

	msgReceive, err := cli.GetMessage()

	if err != nil {
		t.Fatal(err)
	}

	err = cli.DeleteMessage(msgReceive)

	if err != nil {
		t.Fatal(err)
	}

	if string(msgReceive.Body) != string(msgSend.Body) {
		t.Fatalf("Expected body %s but got %s", string(msgSend.Body), string(msgReceive.Body))
	}

	for k, _ := range msgSend.Properties {
		if msgReceive.Properties[k] != msgSend.Properties[k] {
			t.Fatalf("Expected property %s value %s but got %s", k, msgSend.Properties[k], msgReceive.Properties[k])
		}
	}
}

func compareMsg(t *testing.T, expected *Message, actual *Message, skipProperties bool) {

	if actual.SessionId != expected.SessionId {
		t.Fatalf("Expected SessionId is %s but got %s", expected.SessionId, actual.SessionId)
	}

	if actual.Id != expected.Id {
		t.Fatalf("Expected MessageId is %s but got %s", expected.Id, actual.Id)
	}

	if actual.TimeToLive != expected.TimeToLive {
		t.Fatalf("Expected TimeToLive is %v but got %v", expected.TimeToLive, actual.TimeToLive)
	}

	if actual.CorrelationId != expected.CorrelationId {
		t.Fatalf("Expected CorrelationId is %s but got %s", expected.CorrelationId, actual.CorrelationId)
	}

	if actual.SequenceNumber != expected.SequenceNumber {
		t.Fatalf("Expected SequenceNumber is %v but got %v", expected.SequenceNumber, actual.SequenceNumber)
	}

	if actual.DeliveryCount != expected.DeliveryCount {
		t.Fatalf("Expected DeliveryCount is %v but got %v", expected.DeliveryCount, actual.DeliveryCount)
	}

	if actual.To != expected.To {
		t.Fatalf("Expected To is %s but got %s", expected.To, actual.To)
	}

	if actual.ReplyTo != expected.ReplyTo {
		t.Fatalf("Expected ReplyTo is %s but got %s", expected.ReplyTo, actual.ReplyTo)
	}

	if actual.EnqueuedTimeUtc.UTC() != expected.EnqueuedTimeUtc.UTC() {
		t.Fatalf("Expected EnqueuedTimeUtc is %s but got %s", expected.EnqueuedTimeUtc, actual.EnqueuedTimeUtc)
	}

	if actual.ScheduledEnqueueTimeUtc.UTC() != expected.ScheduledEnqueueTimeUtc.UTC() {
		t.Fatalf("Expected ScheduledEnqueueTimeUtc is %s but got %s", expected.ScheduledEnqueueTimeUtc, actual.ScheduledEnqueueTimeUtc)
	}

	if !skipProperties {
		for k := range expected.Properties {
			if actual.Properties[k] != expected.Properties[k] {
				t.Fatalf("Expected property[%s] is %s but got %s", testMsg.Properties[k], actual.Properties[k])
			}
		}
	}
}
