package main

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"

	"github.com/pantherman594/giza/pkg/giza"
	// "github.com/pantherman594/giza/pkg/cassandra"
)

type Client interface {
	Write(uuid gocql.UUID, data string) (*response, error)
}

type GizaClient struct {
	giza.Giza
}
var _ = Client(&GizaClient{})

func (g *GizaClient) Write(uuid gocql.UUID, data string) (*response, error) {
	msg := &giza.Metadata{Value: data}
	m, err := msg.Marshal()
	if err != nil {
		return nil, err
	}

	before := time.Now()
	err = g.WriteFast(uuid, m, 0)
	after := time.Now()
	if err != nil {
		return nil, err
	}

	rtt := after.Sub(before).Seconds() * 1000

	return &response{
		after,
		rtt,
		0,
		false,
		0,
	}, nil
}

type CassandraClient struct {
	session *gocql.Session
}

func (c *CassandraClient) Init(ip string, numObjects int) ([]gocql.UUID, error) {
	cluster := gocql.NewCluster(ip)
	cluster.Keyspace = "cassandra"
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 1 * time.Minute
	session, err := cluster.CreateSession()
	
	if err != nil {
		return nil, err
	}

	c.session = session

	c.session.Query(`CREATE TABLE IF NOT EXISTS state (
		object_id uuid,
		version int,
		value blob,
		highest_version int STATIC,
		PRIMARY KEY (object_id, version));`).Exec()

	objectIds := make([]gocql.UUID, 0, numObjects)
	if numObjects > 0 {
		c.session.Query("TRUNCATE TABLE state").Exec()

		for i := 0; i < numObjects; i++ {
			id, err := gocql.RandomUUID()
			if err != nil {
				return nil, err
			}

			m := make(map[string]interface{})
			_, err = c.session.Query(`INSERT INTO state(object_id, version, value, highest_version)
				VALUES(?, ?, ?, ?) IF NOT EXISTS`, id, 0, "", 0).MapScanCAS(m)
			if err != nil {
				return nil, err
			}

			objectIds = append(objectIds, id)
		}
	} else {
		iter := c.session.Query("SELECT object_id, version FROM state").Iter()
		scanner := iter.Scanner()

		for scanner.Next() {
			var objectId gocql.UUID
			var version int

			err = scanner.Scan(&objectId, &version)
			if err != nil {
				return nil, err
			}

			if version == 0 {
				objectIds = append(objectIds, objectId)
			}
		}

		if err := scanner.Err(); err != nil {
			return nil, err
		}
	}
	return objectIds, nil
}

func (c *CassandraClient) Write(uuid gocql.UUID, data string) (*response, error) {
	var version uint

	fmt.Println("Writing")
	before := time.Now()
	err := c.session.Query(`SELECT highest_version
		FROM state
		WHERE object_id = ?`,
		uuid).SerialConsistency(gocql.Serial).Scan(&version)

	if err != nil {
		return nil, err
	}

	for {
		version += 1

		m := make(map[string]interface{})
		success, err := c.session.Query(`INSERT INTO state(object_id, version, value, highest_version)
			VALUES(?, ?, ?, ?) IF NOT EXISTS`, uuid, version, data, version).MapScanCAS(m)

		after := time.Now()

		if err != nil {
			return nil, err
		}

		rtt := after.Sub(before).Seconds() * 1000

		if success {
			return &response{
				after,
				rtt,
				0,
				false,
				0,
			}, nil
		}
	}
}
