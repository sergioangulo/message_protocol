package messages

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestMessage(t *testing.T) {
	msg := MessageRequest{
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
		Body: BodyRequest{
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
		},
	}

	b, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	bStr := string(b)
	expected := `{"route":{"trxId":"123456","org":{"service":"service1","srvId":"srvId1","node":"node1","instId":"instId1"},"dst":{"service":"service2","srvId":"srvId2","node":"node2","instId":"instId2"}},"msg":{"data":{"content":"content","options":{"key1":"value1","key2":"value2"}},"metadata":{"retry_number":1,"processing_time_ns":"1000000000","request_stamp":"2020-01-01T00:00:00Z"}}}`
	if bStr != expected {
		t.Errorf("expected %s, got %s", expected, bStr)
	}

}

func TestMessageUnmarshal(t *testing.T) {
	//We goint to embed a custom struct in the message.body.data
	type CustomData struct {
		Some      string `json:"some"`
		Random    string `json:"random"`
		Structure string `json:"structure"`
	}

	custom_data := CustomData{
		Some:      "some",
		Random:    "random",
		Structure: "structure",
	}

	//setrializamos data
	dataBytes, err := json.Marshal(custom_data)
	if err != nil {
		t.Fatal(err)
	}

	responseStatus := ResponseStatus{Code: 200, Desc: "OK"}
	metadata := Metadata{RetryNumber: 1, ProcessingTimeNs: "1000000000", RequestStamp: "2020-01-01T00:00:00Z"}
	// Agregamos data_input como RawMessage ([]bytes)
	data_input := dataBytes
	processingInstanceOrg := ProcessingInstance{Service: "service1", SrvId: "srvId1", Node: "node1", InstId: "instId1"}
	ProcessingInstanceDst := ProcessingInstance{Service: "service2", SrvId: "srvId2", Node: "node2", InstId: "instId2"}
	route := Route{TrxId: "123456", Org: processingInstanceOrg, Dst: ProcessingInstanceDst}
	output_body := BodyResponse{Data: data_input, Metadata: metadata, ResponseStatus: responseStatus}
	output_message := MessageResponse{Route: route, Body: output_body}
	serliazed_msg, err := json.Marshal(output_message)
	if err != nil {
		t.Fatal(err)
	}

	//serialized_msg es el mensaje serializado de salida
	//ahora vamos a deserializarlo

	var inputMessage MessageResponse

	input_msg := json.Unmarshal(serliazed_msg, &inputMessage)
	if input_msg != nil {
		t.Fatal(input_msg)
	}

	//Deserializamos data
	var input_data CustomData
	err = json.Unmarshal(inputMessage.Body.Data, &input_data)
	if err != nil {
		t.Fatal(err)
	}

	msg := inputMessage

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
	// create a new instance of CustomData

	if input_data.Some != "some" {
		t.Errorf("expected content, got %s", input_data.Some)
	}

	if input_data.Random != "random" {
		t.Errorf("expected content, got %s", input_data.Random)
	}

	if input_data.Structure != "structure" {
		t.Errorf("expected content, got %s", input_data.Structure)
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
	if msg.Route.Dst.Node != "node2" {
		t.Errorf("expected node2, got %s", msg.Route.Dst.Node)
	}

}

func TestMessageUnmarshalError(t *testing.T) {
	b := []byte(`{"route":{"trxId":"123456","org":{"service":"service1","srvId":"srvId1","node":"node1","instId":"instId1"},"dst":{"service":"service2","srvId":"srvId2","node":"node2","instId":"instId2"}},"msg":{"data":{"content":"content","options":{"key1":"value1","key2":"value2"}},"metadata":{"retry_number":1,"processing_time_ns":"1000000000","request_stamp":"2020-01-01T00:00:00Z"}}}`)
	var msg MessageResponse
	err := json.Unmarshal(b, &msg)
	if err != nil {
		t.Fatal("An error was not generated when it should have been")
	}
}

func TestEnsambleDownToUpStructure(t *testing.T) {
	responseStatus := ResponseStatus{Code: 200, Desc: "OK"}
	metadata := Metadata{RetryNumber: 1, ProcessingTimeNs: "1000000000", RequestStamp: "2020-01-01T00:00:00Z"}
	data := Data{Content: "content", Options: map[string]interface{}{"key1": "value1", "key2": "value2"}}
	dataBytes, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}

	processingInstanceOrg := ProcessingInstance{Service: "service1", SrvId: "srvId1", Node: "node1", InstId: "instId1"}
	ProcessingInstanceDst := ProcessingInstance{Service: "service2", SrvId: "srvId2", Node: "node2", InstId: "instId2"}
	route := Route{TrxId: "123456", Org: processingInstanceOrg, Dst: ProcessingInstanceDst}
	msg := BodyResponse{Data: dataBytes, Metadata: metadata, ResponseStatus: responseStatus}
	message := MessageResponse{Route: route, Body: msg}
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
	route.SwapRouteAsyncronous()
	fmt.Printf("route: %s\n", route.String())
	if route != routeExpected {
		t.Errorf("expected %v, got %v", routeExpected, route)
	}
}

func TestPrintMessageResponse(t *testing.T) {
	processingInstanceOrg := ProcessingInstance{Service: "service1", SrvId: "srvId1", Node: "node1", InstId: "instId1"}
	ProcessingInstanceDst := ProcessingInstance{Service: "service2", SrvId: "srvId2", Node: "node2", InstId: "instId2"}
	route := Route{TrxId: "123456", Org: processingInstanceOrg, Dst: ProcessingInstanceDst}
	responseStatus := ResponseStatus{Code: 200, Desc: "OK"}
	metadata := Metadata{RetryNumber: 1, ProcessingTimeNs: "1000000000", RequestStamp: "2020-01-01T00:00:00Z"}
	data := Data{Content: "content", Options: map[string]interface{}{"key1": "value1", "key2": "value2"}}
	dataBytes, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	msg := BodyResponse{Data: dataBytes, Metadata: metadata, ResponseStatus: responseStatus}
	message := MessageResponse{Route: route, Body: msg}
	expected := "Message{Route: Route{TrxId: 123456, Org: ProcessingInstance{Service: service1, SrvId: srvId1, Node: node1, InstId: instId1},"
	expected += " Dst: ProcessingInstance{Service: service2, SrvId: srvId2, Node: node2, InstId: instId2}}, Msg: "
	expected += "Body{Data: {\"content\":\"content\",\"options\":{\"key1\":\"value1\",\"key2\":\"value2\"}}, Metadata: Metadata{RetryNumber: 1, ProcessingTimeNs: 1000000000, RequestStamp: 2020-01-01T00:00:00Z}, ResponseStatus: ResponseStatus{Code: 200, Desc: OK}}}"

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

func TestPrintMessageRequest(t *testing.T) {
	processingInstanceOrg := ProcessingInstance{Service: "service1", SrvId: "srvId1", Node: "node1", InstId: "instId1"}
	ProcessingInstanceDst := ProcessingInstance{Service: "service2", SrvId: "srvId2", Node: "node2", InstId: "instId2"}
	route := Route{TrxId: "123456", Org: processingInstanceOrg, Dst: ProcessingInstanceDst}
	metadata := Metadata{RetryNumber: 1, ProcessingTimeNs: "1000000000", RequestStamp: "2020-01-01T00:00:00Z"}
	data := Data{Content: "content", Options: map[string]interface{}{"key1": "value1", "key2": "value2"}}
	msg := BodyRequest{Data: data, Metadata: metadata}
	message := MessageRequest{Route: route, Body: msg}
	expected := "Message{Route: Route{TrxId: 123456, Org: ProcessingInstance{Service: service1, SrvId: srvId1, Node: node1, InstId: instId1},"
	expected += " Dst: ProcessingInstance{Service: service2, SrvId: srvId2, Node: node2, InstId: instId2}}, Msg: "
	expected += "Body{Data: Data{Content: content, Options: map[key1: value1, key2: value2]}, Metadata: Metadata{RetryNumber: 1, ProcessingTimeNs: 1000000000, RequestStamp: 2020-01-01T00:00:00Z}}}"

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

func TestParseMessageRequest(t *testing.T) {
	received_msg := "{\"route\":{\"trxId\":\"2cf16549d989b25fdd1bb6251\",\"org\":{\"service\":\"content_validation\",\"srvId\":\"0\",\"node\":\"\",\"instId\":\"378df32521\"},\"dst\":{\"service\":\"language_validator\",\"srvId\":\"1\",\"node\":\"\",\"instId\":\"\"}},\"msg\":{\"data\":{\"content\":\"This is a new content.\",\"options\":{\"threshold\":\"98\"}},\"metadata\":{\"retry_number\":1,\"processing_time_ns\":\"13243438\",\"request_stamp\":\"2024-05-06T09:32:04Z\"}}}"
	var msg MessageRequest
	msg.ParseMessageRequest(received_msg)

	if msg.Body.Data.Content != "This is a new content." {
		t.Errorf("expected This is a new content., got %s", msg.Body.Data.Content)
	}
	if msg.Body.Data.Options["threshold"] != "98" {
		t.Errorf("expected 98, got %s", msg.Body.Data.Options["threshold"])
	}
	if msg.Body.Metadata.RetryNumber != 1 {
		t.Errorf("expected 1, got %d", msg.Body.Metadata.RetryNumber)
	}

}

func TestGenericMessage(t *testing.T) {
	type Test struct {
		Varaux  string `json:"varaux"`
		Vartrax string `json:"vartrax"`
	}

	testStruct := Test{
		Vartrax: "content",
		Varaux:  "content",
	}

	testBytes, err := json.Marshal(testStruct)

	msg := GenericMessage{
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
		Body: testBytes,
	}
	b, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	bStr := string(b)
	expected := `{"route":{"trxId":"123456","org":{"service":"service1","srvId":"srvId1","node":"node1","instId":"instId1"},"dst":{"service":"service2","srvId":"srvId2","node":"node2","instId":"instId2"}},"msg":{"varaux":"content","vartrax":"content"}}`
	if bStr != expected {
		t.Errorf("expected %s, got %s", expected, bStr)
	}
	//now, I will pass a b to convert it into a Generic Message with struct

}
