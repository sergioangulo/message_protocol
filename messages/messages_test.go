package messages

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestMessage(t *testing.T) {
	msg := Message{
		Route: Route{
			TrxId: "123456",
			Org: ProcessingInstance{
				Service: "service1",
				SrvId:   "srvId1",
				Node:    "node1",
				InstId:  "instId1",
			},
			Dst: ProcessingInstance{
				Service: "service2",
				SrvId:   "srvId2",
				Node:    "node2",
				InstId:  "instId2",
			},
		},
		Body: Body{
			Data: Data{
				Content: "content",
				Options: map[string]interface{}{
					"key1": "value1",
					"key2": "value2",
				},
			},
			Metadata: Metadata{
				RetryNumber:      1,
				ProcessingTimeNs: "1000000000",
				RequestStamp:     "2020-01-01T00:00:00Z",
			},
			ResponseStatus: ResponseStatus{
				Code: 200,
				Desc: "OK",
			},
		},
	}

	b, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	bStr := string(b)
	expected := `{"route":{"trxId":"123456","org":{"service":"service1","srvId":"srvId1","node":"node1","instId":"instId1"},"dst":{"service":"service2","srvId":"srvId2","node":"node2","instId":"instId2"}},"msg":{"data":{"content":"content","options":{"key1":"value1","key2":"value2"}},"metadata":{"retry_number":1,"processing_time_ns":"1000000000","request_stamp":"2020-01-01T00:00:00Z"},"response_status":{"code":200,"desc":"OK"}}}`
	if bStr != expected {
		t.Errorf("expected %s, got %s", expected, bStr)
	}

}

func TestMessageUnmarshal(t *testing.T) {
	b := []byte(`{"route":{"trxId":"123456","org":{"service":"service1","srvId":"srvId1","node":"node1","instId":"instId1"},"dst":{"service":"service2","srvId":"srvId2","node":"node2","instId":"instId2"}},"msg":{"data":{"content":"content","options":{"key1":"value1","key2":"value2"}},"metadata":{"retry_number":1,"processing_time_ns":"1000000000","request_stamp":"2020-01-01T00:00:00Z"},"response_status":{"code":200,"desc":"OK"}}}`)
	var msg Message
	err := json.Unmarshal(b, &msg)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Route.TrxId != "123456" {
		t.Errorf("expected 123456, got %s", msg.Route.TrxId)
	}
	if msg.Route.Org.Service != "service1" {
		t.Errorf("expected service1, got %s", msg.Route.Org.Service)
	}
	if msg.Route.Org.SrvId != "srvId1" {
		t.Errorf("expected srvId1, got %s", msg.Route.Org.SrvId)
	}
	if msg.Route.Org.Node != "node1" {
		t.Errorf("expected node1, got %s", msg.Route.Org.Node)
	}
	if msg.Route.Org.InstId != "instId1" {
		t.Errorf("expected instId1, got %s", msg.Route.Org.InstId)
	}
	if msg.Route.Dst.Service != "service2" {
		t.Errorf("expected service2, got %s", msg.Route.Dst.Service)
	}
	if msg.Route.Dst.SrvId != "srvId2" {
		t.Errorf("expected srvId2, got %s", msg.Route.Dst.SrvId)
	}
	if msg.Route.Dst.Node != "node2" {
		t.Errorf("expected node2, got %s", msg.Route.Dst.Node)
	}
	if msg.Route.Dst.InstId != "instId2" {
		t.Errorf("expected instId2, got %s", msg.Route.Dst.InstId)
	}
	if msg.Body.Data.Content != "content" {
		t.Errorf("expected content, got %s", msg.Body.Data.Content)
	}
	if msg.Body.Data.Options["key1"] != "value1" {
		t.Errorf("expected value1, got %s", msg.Body.Data.Options["key1"])
	}
	if msg.Body.Data.Options["key2"] != "value2" {
		t.Errorf("expected value2, got %s", msg.Body.Data.Options["key2"])
	}
	if msg.Body.Metadata.RetryNumber != 1 {
		t.Errorf("expected 1, got %d", msg.Body.Metadata.RetryNumber)
	}
	if msg.Body.Metadata.ProcessingTimeNs != "1000000000" {
		t.Errorf("expected 1000000000, got %s", msg.Body.Metadata.ProcessingTimeNs)
	}
	if msg.Body.Metadata.RequestStamp != "2020-01-01T00:00:00Z" {
		t.Errorf("expected 2020-01-01T00:00:00Z, got %s", msg.Body.Metadata.RequestStamp)
	}
	if msg.Body.ResponseStatus.Code != 200 {
		t.Errorf("expected 200, got %d", msg.Body.ResponseStatus.Code)
	}
	if msg.Body.ResponseStatus.Desc != "OK" {
		t.Errorf("expected OK, got %s", msg.Body.ResponseStatus.Desc)
	}
}

func TestMessageUnmarshalError(t *testing.T) {
	b := []byte(`{"route":{"trxId":"123456","org":{"service":"service1","srvId":"srvId1","node":"node1","instId":"instId1"},"dst":{"service":"service2","srvId":"srvId2","node":"node2","instId":"instId2"}},"msg":{"data":{"content":"content","options":{"key1":"value1","key2":"value2"}},"metadata":{"retry_number":1,"processing_time_ns":"1000000000","request_stamp":"2020-01-01T00:00:00Z"}}}`)
	var msg Message
	err := json.Unmarshal(b, &msg)
	if err != nil {
		t.Fatal("An error was not generated when it should have been")
	}
}

func TestEnsambleDownToUpStructure(t *testing.T) {
	responseStatus := ResponseStatus{Code: 200, Desc: "OK"}
	metadata := Metadata{RetryNumber: 1, ProcessingTimeNs: "1000000000", RequestStamp: "2020-01-01T00:00:00Z"}
	data := Data{Content: "content", Options: map[string]interface{}{"key1": "value1", "key2": "value2"}}
	processingInstanceOrg := ProcessingInstance{Service: "service1", SrvId: "srvId1", Node: "node1", InstId: "instId1"}
	ProcessingInstanceDst := ProcessingInstance{Service: "service2", SrvId: "srvId2", Node: "node2", InstId: "instId2"}
	route := Route{TrxId: "123456", Org: processingInstanceOrg, Dst: ProcessingInstanceDst}
	msg := Body{Data: data, Metadata: metadata, ResponseStatus: responseStatus}
	message := Message{Route: route, Body: msg}
	expected := `{"route":{"trxId":"123456","org":{"service":"service1","srvId":"srvId1","node":"node1","instId":"instId1"},"dst":{"service":"service2","srvId":"srvId2","node":"node2","instId":"instId2"}},"msg":{"data":{"content":"content","options":{"key1":"value1","key2":"value2"}},"metadata":{"retry_number":1,"processing_time_ns":"1000000000","request_stamp":"2020-01-01T00:00:00Z"},"response_status":{"code":200,"desc":"OK"}}}`
	b, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	bStr := string(b)
	if bStr != expected {
		t.Errorf("expected %s, got %s", expected, bStr)
	}
}

func TestSwapRouteSyncronous(t *testing.T) {

	processingInstanceOrg := ProcessingInstance{Service: "service1", SrvId: "srvId1", Node: "node1", InstId: "instId1"}
	ProcessingInstanceDst := ProcessingInstance{Service: "service2", SrvId: "srvId2", Node: "node2", InstId: "instId2"}
	route := Route{TrxId: "123456", Org: processingInstanceOrg, Dst: ProcessingInstanceDst}
	routeSwapped := Route{TrxId: "123456", Org: ProcessingInstanceDst, Dst: processingInstanceOrg}
	route.SwapRouteSyncronous()
	if route != routeSwapped {
		t.Errorf("expected %v, got %v", routeSwapped, route)
	}
}

func TestSwapRouteAsyncronous(t *testing.T) {
	processingInstanceOrg := ProcessingInstance{Service: "service1", SrvId: "srvId1", Node: "node1", InstId: "instId1"}
	ProcessingInstanceDst := ProcessingInstance{Service: "service2", SrvId: "srvId2", Node: "node2", InstId: "instId2"}
	route := Route{TrxId: "123456", Org: processingInstanceOrg, Dst: ProcessingInstanceDst}
	routeExpected := Route{TrxId: "123456", Org: ProcessingInstanceDst, Dst: ProcessingInstance{Service: "service1", SrvId: "srvId1", Node: "", InstId: ""}}
	route.swapRouteAsyncronous()
	fmt.Printf("route: %s\n", route.String())
	if route != routeExpected {
		t.Errorf("expected %v, got %v", routeExpected, route)
	}
}

func TestPrintMessage(t *testing.T) {
	processingInstanceOrg := ProcessingInstance{Service: "service1", SrvId: "srvId1", Node: "node1", InstId: "instId1"}
	ProcessingInstanceDst := ProcessingInstance{Service: "service2", SrvId: "srvId2", Node: "node2", InstId: "instId2"}
	route := Route{TrxId: "123456", Org: processingInstanceOrg, Dst: ProcessingInstanceDst}
	responseStatus := ResponseStatus{Code: 200, Desc: "OK"}
	metadata := Metadata{RetryNumber: 1, ProcessingTimeNs: "1000000000", RequestStamp: "2020-01-01T00:00:00Z"}
	data := Data{Content: "content", Options: map[string]interface{}{"key1": "value1", "key2": "value2"}}
	msg := Body{Data: data, Metadata: metadata, ResponseStatus: responseStatus}
	message := Message{Route: route, Body: msg}
	expected := "Message{Route: Route{TrxId: 123456, Org: ProcessingInstance{Service: service1, SrvId: srvId1, Node: node1, InstId: instId1},"
	expected += " Dst: ProcessingInstance{Service: service2, SrvId: srvId2, Node: node2, InstId: instId2}}, Msg: "
	expected += "Body{Data: Data{Content: content, Options: map[key1: value1, key2: value2]}, Metadata: Metadata{RetryNumber: 1, ProcessingTimeNs: 1000000000, RequestStamp: 2020-01-01T00:00:00Z}, ResponseStatus: ResponseStatus{Code: 200, Desc: OK}}}"

	if message.String() != expected {
		// search index of difference
		fmt.Printf("1): %s\n", message.String())
		fmt.Printf("2): %s\n", expected)
		for i := 0; i < len(expected); i++ {
			if expected[i] != message.String()[i] {
				t.Errorf("expected: \na)%s\nb)%s", expected[i:], message.String()[i:])
				break
			}
		}
	}
}
