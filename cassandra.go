package main

import (
	"github.com/gocql/gocql"
)

// Init initializes the connection to cassandra at the given IP.
func OpenCassandra(ip string) (*gocql.Session, error) {
	cluster := gocql.NewCluster(ip)
	cluster.Keyspace = "giza"
	cluster.Consistency = gocql.Quorum
	session, err := cluster.CreateSession()

	if err != nil {
		return nil, err
	}

	return session, nil
}

func CreateTable(gocql *gocql.Session) error {
	return gocql.Query(`CREATE TABLE IF NOT EXISTS state (
		object_id uuid,
		version bigint,
		highest_ballot_seen bigint,
		highest_ballot_accepted bigint,
		highest_value_accepted blob,
		preaccepted bigint STATIC,
		preaccepted_value blob STATIC,
		known_committed_versions set<bigint> STATIC,
		PRIMARY KEY (object_id, version));`).Exec()
}
