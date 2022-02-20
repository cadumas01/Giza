package main

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
)

type ACUMessage struct {
	Id        uint64
	ObjectId  gocql.UUID
	Condition TestCondition
	Update    string
	Retry     uint
}

type ACUResponse struct {
	Id      uint64
	Applied bool
	Error   error
}

type RemoteNode struct {
	ip string

	prev_id           uint64
	incoming_queue    map[uint64]ACUResponse
	received_response map[uint64](chan struct{})
}

var _ = Node(&RemoteNode{})

func (n *RemoteNode) Connect() error {
	if n.ip == "" {
		return errors.New("IP address is not set.")
	}

	// TODO create a TCP connection

	return nil
}

func (n *RemoteNode) Disconnect() {

	// TODO send protobuf? messages

	// TODO close TCP connection

}

func (n *RemoteNode) AtomicConditionalUpdate(object_id gocql.UUID, condition TestCondition, update string, retry uint) (bool, error) {

	id := atomic.AddUint64(&n.prev_id, 1)

	wait := make(chan struct{}, 1)
	n.received_response[id] = wait

	// TODO send protobuf? messages

	select {
	case <-n.received_response[id]:
		result := n.incoming_queue[id]
		close(n.received_response[id])
		delete(n.received_response, id)
		delete(n.incoming_queue, id)

		return result.Applied, result.Error
	case <-time.After(TIMEOUT):
		return false, errors.New("Timed out.")
	}
}

func (n *RemoteNode) GetState(object_id gocql.UUID) (*State, error) {

	id := atomic.AddUint64(&n.prev_id, 1)

	wait := make(chan struct{}, 1)
	n.received_response[id] = wait

	// TODO send protobuf? messages

	select {
	case <-n.received_response[id]:
		result := n.incoming_queue[id] // TODO need to change
		close(n.received_response[id])
		delete(n.received_response, id)
		delete(n.incoming_queue, id)

		return nil, result.Error
	case <-time.After(TIMEOUT):
		return nil, errors.New("Timed out.")
	}
}

func (n *RemoteNode) onReceiveMessage(id uint64, message ACUResponse) {
	n.incoming_queue[id] = message
	n.received_response[id] <- struct{}{}
}
