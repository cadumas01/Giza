package main

import (
	"math/rand"
	"time"
)

const (
	TIMEOUT         = 30 * time.Second
	DEFAULT_RETRIES = 5

	N              = 3
	FAST_QUORUM    = (3 * N / 4) + 1
	CLASSIC_QUORUM = (N / 2) + 1
)

func main() {
	rand.Seed(time.Now().UnixNano())
}
