package main

import (
	"errors"

	"github.com/gocql/gocql"
)

type LocalNode struct {
	cassandra Cassandra
	ip        string
}

var _ = Node(&LocalNode{})

func (n *LocalNode) Connect() error {
	if n.ip == "" {
		return errors.New("IP address is not set.")
	}

	return n.cassandra.Init(n.ip)
}

func (n *LocalNode) Disconnect() {
	n.cassandra.Close()
}

func (n *LocalNode) AtomicConditionalUpdate(object_id gocql.UUID, condition TestCondition, update string, retry uint) (bool, error) {
	if !condition.CheckArgs() {
		return false, errors.New("Invalid condition arguments.")
	}

	return n.cassandra.AtomicConditionalUpdate(object_id, condition.Test, update, retry)
}

func (n *LocalNode) GetState(object_id gocql.UUID) (*State, error) {
	return n.cassandra.GetState(object_id)
}
