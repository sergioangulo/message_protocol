package messages

import "fmt"

type Message struct {
	Route Route `json:"route"`
	Body  Body  `json:"msg"`
}

type Route struct {
	TrxId string             `json:"trxId"`
	Org   ProcessingInstance `json:"org"`
	Dst   ProcessingInstance `json:"dst"`
}
type ProcessingInstance struct {
	Service string `json:"service"`
	SrvId   string `json:"srvId"`
	Node    string `json:"node"`
	InstId  string `json:"instId"`
}

type Body struct {
	Data           Data           `json:"data"`
	Metadata       Metadata       `json:"metadata"`
	ResponseStatus ResponseStatus `json:"response_status"`
}

type Data struct {
	Content string                 `json:"content"`
	Options map[string]interface{} `json:"options"`
}

type Metadata struct {
	RetryNumber      int    `json:"retry_number"`
	ProcessingTimeNs string `json:"processing_time_ns"`
	RequestStamp     string `json:"request_stamp"`
}

type ResponseStatus struct {
	Code int    `json:"code"`
	Desc string `json:"desc"`
}

func (m *Route) SwapRouteSyncronous() {
	m.Org, m.Dst = m.Dst, m.Org
}

func (m *Route) swapRouteAsyncronous() {
	m.SwapRouteSyncronous()
	m.Dst.Node = ""
	m.Dst.InstId = ""
}

// To string method for Structures

func (m *Message) String() string {
	return fmt.Sprintf("Message{Route: %s, Msg: %s}", m.Route.String(), m.Body.String())
}

func (m *Route) String() string {
	return fmt.Sprintf("Route{TrxId: %s, Org: %s, Dst: %s}", m.TrxId, m.Org.String(), m.Dst.String())
}

func (m *ProcessingInstance) String() string {
	return fmt.Sprintf("ProcessingInstance{Service: %s, SrvId: %s, Node: %s, InstId: %s}", m.Service, m.SrvId, m.Node, m.InstId)
}

func (m *Body) String() string {
	return fmt.Sprintf("Body{Data: %s, Metadata: %s, ResponseStatus: %s}", m.Data.String(), m.Metadata.String(), m.ResponseStatus.String())
}

func (m *Metadata) String() string {
	return fmt.Sprintf("Metadata{RetryNumber: %d, ProcessingTimeNs: %s, RequestStamp: %s}", m.RetryNumber, m.ProcessingTimeNs, m.RequestStamp)
}

func (m *ResponseStatus) String() string {
	return fmt.Sprintf("ResponseStatus{Code: %d, Desc: %s}", m.Code, m.Desc)
}

func (m Data) String() string {
	str := "Data{Content: " + m.Content + ", Options: map["
	i := 0
	for k, v := range m.Options {
		comma := ""
		if i == 1 {
			comma = ", "
		}
		str += fmt.Sprintf("%s%s: %v", comma, k, v)
		i++
	}
	str += "]}"
	return str
}
