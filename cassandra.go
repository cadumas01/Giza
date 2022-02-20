package main

import (
	"errors"

	"github.com/gocql/gocql"
)

var NotInitialized = errors.New("Cassandra session is not initialized.")

type Cassandra struct {
	session *gocql.Session
}

// Init initializes the connection to cassandra at the given IP.
func (c *Cassandra) Init(ip string) error {
	if c.session != nil {
		return errors.New("Cassandra session is already initialized.")
	}

	cluster := gocql.NewCluster(ip)
	cluster.Keyspace = "giza"
	cluster.Consistency = gocql.Quorum
	session, err := cluster.CreateSession()

	if err != nil {
		return err
	}

	c.session = session

	return nil
}

// Close closes the cassandra connection.
func (c *Cassandra) Close() {
	if c.session == nil {
		return
	}

	c.session.Close()
	c.session = nil
}

// GetState returns the current state of an object, based on its ID.
func (c *Cassandra) GetState(object_id gocql.UUID) (*State, error) {
	if c.session == nil {
		return nil, NotInitialized
	}

	query := c.session.Query(`SELECT * FROM state WHERE object_id = ?`, object_id).Iter()
	numRows := query.NumRows()

	if numRows != 1 {
		return nil, errors.New("Could find object.")
	}

	res, err := query.SliceMap()

	if err != nil {
		return nil, err
	}

	return ParseState(res[0])
}

// AtomicConditonalUpdate performs an update, if:
// 1) The condition returns true.
// 2) There has been no writes since the state was read.
func (c *Cassandra) AtomicConditionalUpdate(object_id gocql.UUID, condition func(*State) bool, update string, retry uint) (bool, error) {
	if c.session == nil {
		return false, NotInitialized
	}

	state, err := c.GetState(object_id)
	if err != nil {
		return false, err
	}

	etag := state.Etag

	if !condition(state) {
		return false, nil
	}

	// m stores the row returned by MapScanCAS if the transaction failed.
	// We do not need to use this since we already read the state.
	m := make(map[string]interface{})

	// MapScanCAS is used to execute a LWT
	applied, err := c.session.Query("UPDATE state SET etag += 1, "+update+" WHERE object_id = ? IF etag = ?", object_id, etag).MapScanCAS(m)

	if err != nil {
		return false, err
	}

	if applied {
		return true, nil
	} else if retry > 0 {
		return c.AtomicConditionalUpdate(object_id, condition, update, retry-1)
	}

	return false, nil
}

// Create inserts a new row for the object_id.
func (c *Cassandra) Create(object_id gocql.UUID) (bool, error) {
	if c.session == nil {
		return false, NotInitialized
	}

	// According to https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/dml/dmlLtwtTransactions.html,
	// LWTs and normal operations should not be mixed.
	m := make(map[string]interface{})
	return c.session.Query(`INSERT INTO state (object_id, etag, known_committed_versions) VALUES (?, 0, {}) IF NOT EXISTS`, object_id).MapScanCAS(m)
}
