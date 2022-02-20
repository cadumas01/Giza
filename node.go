package main

import (
	"github.com/gocql/gocql"
)

type Node interface {
	Connect() error
	Disconnect()
	AtomicConditionalUpdate(object_id gocql.UUID, condition TestCondition, update string, retry uint) (bool, error)
	GetState(object_id gocql.UUID) (*State, error)
}
