package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
)

type Giza struct {
	id uint32

	waiting int32
	done []chan struct{}

	session *gocql.Session
	peers   []*gocql.Session // All the cassandra tables in the network. Should include self.
}

type CASResult struct {
	success bool
	value  map[string]interface{}
}

func (g *Giza) Init(ip string) error {
	// Set an id using the last 16 bits of the time in nanoseconds (repeats every
	// 1 min) and 16 random bits
	now := time.Now().UnixMilli()
	g.id = (uint32(uint16(now)) << 16) + uint32(rand.Int31()>>16)

	session, err := OpenCassandra(ip)
	if err != nil {
		return err
	}

	if err := CreateTable(session); err != nil {
		return err
	}

	g.session = session
	g.peers = make([]*gocql.Session, 0, 1)
	g.peers = append(g.peers, session)
	g.done = make([]chan struct{}, 0, 1)

	return nil
}

func (g *Giza) Flush() {
	if atomic.LoadInt32(&g.waiting) == 0 {
		return
	}

	ch := make(chan struct{})
	g.done = append(g.done, ch)

	<-ch
}

func (g *Giza) readBallot(ballot uint64) uint32 {
	return uint32(ballot >> 32)
}

func (g *Giza) createBallot(value uint32) uint64 {
	return (uint64(value) << 32) + uint64(g.id)
}

func (g *Giza) incrementBallot(ballot int64) int64 {
	return int64(g.createBallot(g.readBallot(uint64(ballot)) + 1))
}

func (g *Giza) CreateVersion(object_id gocql.UUID, version uint) {
	fmt.Println("versioncreate")
	wg, _, _ := g.ExecAll(`INSERT INTO state(object_id, version, committed, highest_ballot_accepted, highest_ballot_seen)
		VALUES(?, ?, False, 0, 0)
		IF NOT EXISTS`,
		object_id, version)
	wg.Wait()
	fmt.Println("versioncreated")
}

func (g *Giza) Commit(object_id gocql.UUID, version uint, metadata []byte, preaccepted_by uint) {
	fmt.Println("commit", version)

	wg, _, _ := g.ExecAll(`UPDATE state
		SET known_committed_versions = known_committed_versions + ?, value = ?, committed = True
		WHERE object_id = ? AND VERSION = ?`,
		[]int{int(version)}, metadata, object_id, version)

	wg.Wait()

	wg, _, _ = g.CASAll(nil, `UPDATE state
		SET highest_known_committed_version = ?
		WHERE object_id = ?
		IF highest_known_committed_version < ?`,
		version, object_id, version)

	wg.Wait()

	wg, _, _ = g.CASAll(nil, `UPDATE state
		SET preaccepted = null, preaccepted_value = null
		WHERE object_id = ?
		IF preaccepted = ?`,
		object_id, preaccepted_by)

	wg.Wait()

	fmt.Println("committed")

	if (atomic.AddInt32(&g.waiting, -1) == 0) {
		for _, ch := range g.done {
			ch <- struct{}{}
		}
	}
}

func (g *Giza) ExecAll(stmt string, values ...interface{}) (*sync.WaitGroup, context.Context, context.CancelFunc) {
	wg := &sync.WaitGroup{}
	parent_ctx, cancel := context.WithCancel(context.Background())
	for _, peer := range g.peers {
		wg.Add(1)
		go (func() {
			defer wg.Done()

			ctx, child_cancel := context.WithTimeout(parent_ctx, TIMEOUT)
			defer child_cancel()

			err := peer.Query(stmt, values...).WithContext(ctx).Exec()

			if err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
		})()
	}

	return wg, parent_ctx, cancel
}

func (g *Giza) SelectAll(results *chan []map[string]interface{}, stmt string, values ...interface{}) (*sync.WaitGroup, context.Context, context.CancelFunc) {
	wg := &sync.WaitGroup{}
	parent_ctx, cancel := context.WithCancel(context.Background())
	for _, peer := range g.peers {
		wg.Add(1)
		go (func() {
			defer wg.Done()

			ctx, child_cancel := context.WithTimeout(parent_ctx, TIMEOUT)
			defer child_cancel()

			// TODO check if SerialConsistency works
			m, err := peer.Query(stmt, values...).SerialConsistency(gocql.Serial).WithContext(ctx).Iter().SliceMap()

			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				m = nil
			}

			if results != nil && *results != nil {
				*results <- m
			}
		})()
	}

	return wg, parent_ctx, cancel
}

func (g *Giza) CASAll(results *chan CASResult, stmt string, values ...interface{}) (*sync.WaitGroup, context.Context, context.CancelFunc) {
	wg := &sync.WaitGroup{}
	parent_ctx, cancel := context.WithCancel(context.Background())
	for _, peer := range g.peers {
		wg.Add(1)
		go (func() {
			defer wg.Done()

			ctx, child_cancel := context.WithTimeout(parent_ctx, TIMEOUT)
			defer child_cancel()

			m := make(map[string]interface{})
			success, err := peer.Query(stmt, values...).WithContext(ctx).MapScanCAS(m)

			if err != nil {
				fmt.Fprintln(os.Stderr, err)
			}

			if results != nil && *results != nil {
				if err != nil {
					*results <- CASResult{
						success: false,
						value: nil,
					}
				} else {
					*results <- CASResult{
						success: success,
						value: m,
					}
				}
			}
		})()
	}

	return wg, parent_ctx, cancel
}

func (g *Giza) Create() (gocql.UUID, error) {
	id, err := gocql.RandomUUID()
	if err != nil {
		return gocql.UUID{}, err
	}

	err = g.session.Query(`INSERT INTO state(object_id, version, known_committed_versions, committed, highest_known_committed_version, highest_ballot_accepted, highest_ballot_seen)
		VALUES(?, 0, {0}, True, 0, 0, 0)`,
		id).Exec()

	if err != nil {
		return gocql.UUID{}, err
	}

	return id, nil
}

func (g *Giza) WriteFast(object_id gocql.UUID, metadata []byte, version uint) error {
	if version == 0 {
		var last_version uint

		err := g.session.Query(`SELECT highest_known_committed_version
  		FROM state
  		WHERE object_id = ?`,
			object_id).Scan(&last_version)

		if err != nil {
			return err
		}

		version = last_version + 1
	}

	fmt.Println("WRITE FAST", version)

	n_success := 0
	n_fail := 0
	highest_version := version
	if highest_version != 0 {
		highest_version -= 1
	}

	results := make(chan CASResult)
	results_p := &results

	_, ctx, _ := g.CASAll(results_p, `UPDATE state USING TTL 60
		SET preaccepted = ?, preaccepted_value = ?
		WHERE object_id = ?
		IF preaccepted = null AND highest_known_committed_version < ?`,
		g.id, metadata, object_id, version)

result_loop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result := <-results:
			if result.success {
				n_success += 1
			} else {
				n_fail += 1
				if result.value != nil {
					if raw, ok := result.value["highest_known_committed_version"]; ok {
						if i, ok := raw.(int); ok {
							u := uint(i)
							if u > highest_version {
								highest_version = u
							}
						}
					}
				}
			}

			if n_success >= FAST_QUORUM {
				// Successful write: commit the value
				break result_loop
			}

			if len(g.peers)-n_fail < FAST_QUORUM {
				// Too many peers failed to achieve fast quorum
				fmt.Println("FAILED")

				close(results)
				*results_p = nil

				return g.WriteSlow(object_id, highest_version + 1, metadata)
			}
		}
	}

	fmt.Println("DONE")

	close(results)
	*results_p = nil

	atomic.AddInt32(&g.waiting, 1)

	// Asynchronously commit and clean up
	go func() {
		g.Commit(object_id, version, metadata, uint(g.id))
	}()

	return nil
}

func (g *Giza) WriteSlow(object_id gocql.UUID, version uint, metadata []byte) error {
	var last_highest_ballot int64

	g.CreateVersion(object_id, version)

	err := g.session.Query(`SELECT highest_ballot_seen
		FROM state
		WHERE object_id = ? AND version = ?`,
		object_id, version).Scan(&last_highest_ballot)

	if err != nil {
		return err
	}

	ballot := g.incrementBallot(last_highest_ballot)
	fmt.Println("write slow", last_highest_ballot, ballot, version)

	if success, err := g.writeSlowPrepare(object_id, version, ballot); err != nil {
		return err
	} else if !success {
		// Try again, with a higher ballot number
		fmt.Println("FAIL 1")
		return g.WriteSlow(object_id, version, metadata)
	}

	preaccepted_max_count, preaccepted_max_val, highest_accepted_max, highest_accepted_val, err := g.writeSlowQuery(object_id, version)
	if err != nil {
		return nil
	}

	var value RawMetadata
	send_self := false

	if highest_accepted_max > 0 {
		// Case 1: accept the value with the highest accepted ballot
		fmt.Println("case 1")
		value = highest_accepted_val
		ballot = int64(highest_accepted_max)
	} else if preaccepted_max_count > 0 {
		fmt.Println("case 2")
		// Case 2: accept the most popular pre-accepted value
		// version = int64(preaccepted_max_ver)
		value = preaccepted_max_val
		fmt.Println(preaccepted_max_count, value)
	} else {
		fmt.Println("case 3")
		// Case 3: no real contention, send accept request with own value
		value = metadata
		send_self = true
	}

	if success, already_committed, err := g.writeSlowCommit(object_id, version, ballot, &value); err != nil {
		return err
	} else if !success {
		// Try again, with a higher ballot number
		fmt.Println("FAIL 2")

		if already_committed {
			fmt.Println("ALREADY COMMITTED")
			return g.WriteSlow(object_id, version + 1, metadata)
		}

		return g.WriteSlow(object_id, version, metadata)
	}

	// Asynchronously commit

	atomic.AddInt32(&g.waiting, 1)
	if !send_self {
		go g.Commit(object_id, version, value, 0)
		fmt.Println("COMMITTED WRITE FAST")
		return g.WriteFast(object_id, metadata, 0)
	} else {
		go g.Commit(object_id, version, metadata, 0)
		return nil
	}
}

func (g *Giza) writeSlowPrepare(object_id gocql.UUID, version uint, ballot int64) (bool, error) {
	n_success := 0
	n_fail := 0

	results := make(chan CASResult)
	results_p := &results

	defer func() {
		close(results)
		*results_p = nil
	}()


	// Conditionally send the Prepare request
	_, ctx, _ := g.CASAll(results_p, `UPDATE state
		SET highest_ballot_seen = ?
		WHERE object_id = ? AND version = ?
		IF highest_ballot_seen < ?`,
		ballot, object_id, version, ballot)

	for {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case result := <-results:
			if result.success {
				n_success += 1
			} else {
				n_fail += 1
				fmt.Println(result.value)
			}

			if n_success >= CLASSIC_QUORUM {
				// Successful write
				return true, nil
			}

			if len(g.peers)-n_fail < CLASSIC_QUORUM {
				// Too many peers failed to achieve fast quorum
				return false, nil
			}
		}
	}
}

func (g *Giza) writeSlowQuery(object_id gocql.UUID, version uint) (preaccepted_max_count int, preaccepted_max_val RawMetadata, highest_accepted_max uint64, highest_accepted_val RawMetadata, err error) {
	// Retrieve the preaccepted and highest ballot values.
	// Unlike Giza, this will require a second request because Cassandra does not
	// read values when writing.
	preaccepted_max_count = -1
	preaccepted_counts := make(map[uint]int)

	results := make(chan []map[string]interface{})
	results_p := &results

	defer func() {
		close(results)
		*results_p = nil
	}()

	_, ctx, _ := g.SelectAll(results_p, `SELECT
		preaccepted, preaccepted_value, highest_ballot_accepted, highest_value_accepted
		FROM state 
		WHERE object_id = ? AND version = ?`,
		object_id, version)

	n_success := 0
	n_fail := 0

	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		case result := <-results:
			if result != nil && len(result) == 1 {
				fmt.Println(result)
				n_success += 1
				data := result[0]
				pd := parsePaxosData(data)
				fmt.Println(pd)

				if pd.preaccepted != nil && pd.preaccepted_value != nil {
					prev, ok := preaccepted_counts[*pd.preaccepted]
					if !ok {
						prev = 0
					}

					preaccepted_counts[*pd.preaccepted] = prev + 1

					if prev+1 > preaccepted_max_count {
						preaccepted_max_count = prev + 1
						fmt.Println("PD", pd)
						preaccepted_max_val = *pd.preaccepted_value
					}
				}

				if pd.highest_ballot_accepted != nil && pd.highest_value_accepted != nil {
					if *pd.highest_ballot_accepted > highest_accepted_max {
						highest_accepted_max = *pd.highest_ballot_accepted
						highest_accepted_val = *pd.highest_value_accepted
					}
				}
			} else {
				// There should only be 1 result
				n_fail += 1
			}

			if n_success >= CLASSIC_QUORUM {
				// Received more than a majority of responses
				return
			}

			if len(g.peers)-n_fail < CLASSIC_QUORUM {
				// More than half the peers failed to respond.
				err = errors.New("More than half the peers failed to respond.")
				return
			}
		}
	}
}

func (g *Giza) writeSlowCommit(object_id gocql.UUID, version uint, ballot int64, value *RawMetadata) (bool, bool, error) {
	n_success := 0
	n_fail := 0

	results := make(chan CASResult)
	results_p := &results

	defer func() {
		close(results)
		*results_p = nil
	}()

	fmt.Println("slow paxos commit", version, ballot, *value)

	_, ctx, _ := g.CASAll(results_p, `UPDATE state
		SET highest_ballot_accepted = ?, highest_value_accepted = ?
		WHERE object_id = ? AND version = ?
		IF highest_ballot_seen <= ? AND highest_ballot_accepted <= ? AND committed = false`,
		ballot, *value, object_id, version, ballot, ballot)

	for {
		select {
		case <-ctx.Done():
			return false, false, ctx.Err()
		case result := <-results:
			if result.success {
				n_success += 1
			} else {
				if result.value["committed"].(bool) == true {
					return false, true, nil
				}

				n_fail += 1
			}

			if n_success >= CLASSIC_QUORUM {
				// Successful write: commit the value
				return true, false, nil
			}

			if len(g.peers)-n_fail < CLASSIC_QUORUM {
				// Too many peers failed to achieve fast quorum
				return false, false, nil
			}
		}
	}

}

// Values can be null if missing
type paxosData struct {
	preaccepted             *uint
	preaccepted_value       *RawMetadata
	highest_ballot_seen     *uint64
	highest_ballot_accepted *uint64
	highest_value_accepted  *RawMetadata
}

func parsePaxosData(data map[string]interface{}) (p paxosData) {
	fmt.Println("DATA", data)
	if raw, ok := data["preaccepted"]; ok {
		if i, ok := raw.(int); ok {
			u := uint(i)
			p.preaccepted = &u
		}
	}

	if raw, ok := data["preaccepted_value"]; ok {
		if bytes, ok := raw.([]byte); ok {
			var meta RawMetadata = bytes
			if len(bytes) > 0 {
				p.preaccepted_value = &meta
			}
		}
	}

	if raw, ok := data["highest_ballot_seen"]; ok {
		if i64, ok := raw.(int64); ok {
			u64 := uint64(i64)
			p.highest_ballot_seen = &u64
		}
	}

	if raw, ok := data["highest_ballot_accepted"]; ok {
		if i64, ok := raw.(int64); ok {
			u64 := uint64(i64)
			p.highest_ballot_accepted = &u64
		}
	}

	if raw, ok := data["highest_value_accepted"]; ok {
		if bytes, ok := raw.(RawMetadata); ok {
			var meta RawMetadata = bytes
			p.highest_value_accepted = &meta
		}
	}

	return
}
