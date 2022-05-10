package main

import (
	"flag"
	"fmt"
	"golang.org/x/sync/semaphore"
	"log"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/pantherman594/giza/pkg/poisson"
)

var mode *string = flag.String("m", "giza", "Mode, can be 'giza' or 'cassandra'. Defaults to 'giza'.")
var masterAddr *string = flag.String("maddr", "localhost", "Master address. Defaults to localhost")
var dbAddr *string = flag.String("addr", "localhost", "Local cassandra address. Defaults to localhost")
var numReplicas *int = flag.Int("n", 3, "Number of clients. Only used on the master. Defaults to 3.")
var numObjects *int = flag.Int("o", 10, "Number of objects to randomly write to. Higher numbers reduce likelihood of collisions. Only used on the master. Defaults to 10.")
var procs *int = flag.Int("p", 2, "GOMAXPROCS.")
var T = flag.Int("T", 16, "Number of threads (simulated clients).")
var poissonAvg = flag.Int("poisson", -1, "The average number of milliseconds between requests. -1 disables Poisson.")
var timeout *int = flag.Int("timeout", 30, "Duration in seconds of the timeout used when running the client")

// Information about the latency of an operation
type response struct {
	receivedAt    time.Time
	rtt           float64 // The operation latency, in ms
	commitLatency float64 // The operation's commit latency, in ms
	isRead        bool
	replicaID     int
}

// Information pertaining to operations that have been issued but that have not
// yet received responses
type outstandingRequestInfo struct {
	sync.Mutex
	sema       *semaphore.Weighted // Controls number of outstanding operations
	startTimes map[int32]time.Time // The time at which operations were sent out
	isRead     map[int32]bool
}

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(*procs)

	if *numObjects < 1 && *mode != "cassandra" {
		log.Fatalf("Must have at least 1 object.")
	}

	rand.Seed(time.Now().UnixNano())

	readings := make(chan *response, 100000)

	var client Client
	var objectIds []gocql.UUID
	var err error

	if *mode == "cassandra" {
		c := CassandraClient{}
		if *masterAddr != *dbAddr {
			*numObjects = 0
		}
		objectIds, err = c.Init(*dbAddr, *numObjects)
		if err != nil {
			log.Fatalln(err)
		}

		client = &c
	} else {
		g := GizaClient{}
		objectIds, err = g.Init(*dbAddr, *masterAddr, time.Duration(*timeout) * time.Second, *numReplicas, *numObjects)
		if err != nil {
			log.Fatalln(err)
		}

		client = &g
	}

	for i := 0; i < *T; i++ {
		go simulatedClient(client, objectIds, readings)
	}

	printer(readings)
}

func simulatedClient(client Client, objectIds []gocql.UUID, readings chan *response) {
	idRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	poissonGenerator := poisson.NewPoisson(*poissonAvg * 1000)
	numObjects := len(objectIds)
	fmt.Println(objectIds)

	for {
		objectId := objectIds[idRand.Intn(numObjects)]
		resp, err := client.Write(objectId, *dbAddr)
		if err != nil {
			log.Fatalln(err)
		}
		readings <- resp
		time.Sleep(poissonGenerator.NextArrival())
	}
}

func printer(readings chan *response) {
	lattputFile, err := os.Create("lattput.txt")
	if err != nil {
		log.Println("Error creating lattput file", err)
		return
	}

	latFile, err := os.Create("latency.txt")
	if err != nil {
		log.Println("Error creating latency file", err)
		return
	}

	startTime := time.Now()

	for {
		time.Sleep(time.Second)

		count := len(readings)
		var sum float64 = 0
		var commitSum float64 = 0
		endTime := time.Now() // Set to current time in case there are no readings
		for i := 0; i < count; i++ {
			resp := <-readings

			// Log all to latency file
			latFile.WriteString(fmt.Sprintf("%d %f %f\n", resp.receivedAt.UnixNano(), resp.rtt, resp.commitLatency))
			sum += resp.rtt
			commitSum += resp.commitLatency
			endTime = resp.receivedAt
		}

		var avg float64
		var avgCommit float64
		var tput float64
		if count > 0 {
			avg = sum / float64(count)
			avgCommit = commitSum / float64(count)
			tput = float64(count) / endTime.Sub(startTime).Seconds()
		}

		// Log summary to lattput file
		lattputFile.WriteString(fmt.Sprintf("%d %f %f %d %d %f\n", endTime.UnixNano(),
			avg, tput, count, 0, avgCommit))

		startTime = endTime
	}
}
