package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"
	// "github.com/gocql/gocql"
)

const (
	TIMEOUT         = 30 * time.Second
	DEFAULT_RETRIES = 5

	N              = 1
	FAST_QUORUM    = (3 * N / 4) + 1
	CLASSIC_QUORUM = (N / 2) + 1
)

func main() {
	rand.Seed(time.Now().UnixNano())
	ip := os.Args[1]

	var g Giza
	g.Init(ip)

	var g2 Giza
	g2.Init(ip)

	id, err := g.Create()
	// id, err := gocql.ParseUUID("2f8811a7-d826-4142-a326-b7371d5328d2")
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Printf("%s\n", id)

	fmt.Println("Write hi")

	m1 := &Metadata{"hi"}
	m1b, err := m1.Marshal()
	if err != nil {
		log.Fatalln(err)
	}

	err = g.WriteFast(id, m1b, -1)
	if err != nil {
		log.Fatalln(err)
	}

	time.Sleep(5 * time.Second)

	fmt.Println("Write bye")

	m2 := &Metadata{"bye"}
	m2b, err := m2.Marshal()
	if err != nil {
		log.Fatalln(err)
	}
	err = g2.WriteFast(id, m2b, -1)
	if err != nil {
		log.Fatalln(err)
	}

	time.Sleep(30 * time.Second)
}
