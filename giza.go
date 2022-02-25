package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/gocql/gocql"
)

type Giza struct {
	id uint32

	session *gocql.Session
	peers   []*gocql.Session // All the cassandra tables in the network. Should include self.
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

	return nil
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

func (g *Giza) Commit(object_id gocql.UUID, version int64, metadata Metadata) {
	g.ExecAll(`UPDATE state
		SET known_committed_versions = known_committed_versions + ?, highest_value_accepted = ?
		WHERE object_id = ? AND VERSION = ?`,
		version, metadata, object_id, version)
}

func (g *Giza) CleanUp(object_id gocql.UUID, version int64) {
	g.CASAll(nil, `DELETE preaccepted, preaccepted_value
		FROM state
		WHERE object_id = ?
		IF preaccepted = ?`,
		version, object_id, version)
}

func (g *Giza) ExecAll(stmt string, values ...interface{}) (context.Context, context.CancelFunc) {
	parent_ctx, cancel := context.WithCancel(context.Background())
	for _, peer := range g.peers {
		go (func() {
			ctx, child_cancel := context.WithTimeout(parent_ctx, TIMEOUT)
			defer child_cancel()

			err := peer.Query(stmt, values...).WithContext(ctx).Exec()

			if err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
		})()
	}

	return parent_ctx, cancel
}

func (g *Giza) SelectAll(results *chan []map[string]interface{}, stmt string, values ...interface{}) (context.Context, context.CancelFunc) {
	parent_ctx, cancel := context.WithCancel(context.Background())
	for _, peer := range g.peers {
		go (func() {
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

	return parent_ctx, cancel
}

func (g *Giza) CASAll(results *chan bool, stmt string, values ...interface{}) (context.Context, context.CancelFunc) {
	parent_ctx, cancel := context.WithCancel(context.Background())
	for _, peer := range g.peers {
		go (func() {
			ctx, child_cancel := context.WithTimeout(parent_ctx, TIMEOUT)
			defer child_cancel()

			m := make(map[string]interface{})
			success, err := peer.Query(stmt, values...).WithContext(ctx).MapScanCAS(m)

			if err != nil {
				fmt.Fprintln(os.Stderr, err)
			}

			if results != nil && *results != nil {
				*results <- err == nil && success
			}
		})()
	}

	return parent_ctx, cancel
}

func (g *Giza) WriteFast(object_id gocql.UUID, metadata Metadata) error {
	var version int64

	err := g.session.Query(`SELECT version
		FROM state
		WHERE object_id = ?
		ORDER BY version DESC
		LIMIT 1`,
		object_id).Scan(&version)

	if err != nil {
		return err
	}

	version = g.incrementBallot(version)

	n_success := 0
	n_fail := 0

	results := make(chan bool)
	results_p := &results

	ctx, _ := g.CASAll(results_p, `UPDATE state USING TTL 30
		SET preaccepted = ?, preaccepted_value = ?
		WHERE object_id = ?
		IF preaccepted == NULL`,
		version, metadata, object_id)

result_loop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case success := <-results:
			if success {
				n_success += 1
			} else {
				n_fail += 1
			}

			if n_success >= FAST_QUORUM {
				// Successful write: commit the value
				break result_loop
			}

			if len(g.peers)-n_fail < FAST_QUORUM {
				// Too many peers failed to achieve fast quorum

				*results_p = nil
				close(results)

				return g.WriteSlow(object_id, version, metadata)
			}
		}
	}

	*results_p = nil
	close(results)

	// Asynchronously commit and clean up
	go func() {
		g.Commit(object_id, version, metadata)
		g.CleanUp(object_id, version)
	}()

	return nil
}

func (g *Giza) WriteSlow(object_id gocql.UUID, version int64, metadata Metadata) error {
	var last_highest_ballot int64

	err := g.session.Query(`SELECT highest_ballot_seen
		FROM state
		WHERE object_id = ? AND version = ?`,
		object_id, version).Scan(&last_highest_ballot)

	if err != nil {
		return err
	}

	ballot := g.incrementBallot(last_highest_ballot)

	n_success := 0
	n_fail := 0

	cas_results := make(chan bool)
	cas_results_p := &cas_results

	// Conditionally send the Prepare request
	ctx, _ := g.CASAll(cas_results_p, `UPDATE state
		SET highest_ballot_seen = ?
		WHERE object_id = ? AND version = ?
		IF highest_ballot_seen < ?`,
		ballot, object_id, version, ballot)

cas_result_loop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case success := <-cas_results:
			if success {
				n_success += 1
			} else {
				n_fail += 1
			}

			if n_success >= CLASSIC_QUORUM {
				// Successful write: commit the value
				break cas_result_loop
			}

			if len(g.peers)-n_fail < CLASSIC_QUORUM {
				// Too many peers failed to achieve fast quorum

				*cas_results_p = nil
				close(cas_results)

				// Try again, with a higher ballot number
				return g.WriteSlow(object_id, version, metadata)
			}
		}
	}

	*cas_results_p = nil
	close(cas_results)

	// Retrieve the preaccepted and highest ballot values.
	// Unlike Giza, this will require a second request because Cassandra does not
	// read values when writing.
	preaccepted_max_count := -1
	var preaccepted_max_val Metadata
	preaccepted_counts := make(map[uint64]int)

	highest_accepted_max := uint64(0)
	var highest_accepted_val Metadata

	select_results := make(chan []map[string]interface{})
	select_results_p := &select_results

	g.SelectAll(select_results_p, `SELECT
		preaccepted, preaccepted_value, highest_ballot_accepted, highest_accepted_value 
		FROM state 
		WHERE object_id = ? AND version = ?`,
		object_id, version)

	n_success = 0
	n_fail = 0

select_result_loop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result := <-select_results:
			if result != nil && len(result) == 1 {
				n_success += 1
				data := result[0]

				var preaccepted uint64
				var preaccepted_value Metadata
				var highest_ballot_accepted uint64
				var highest_accepted_value Metadata

				p_ok := false
				pv_ok := false
				h_ok := false
				hv_ok := false

				if preaccepted_raw, ok := data["preaccepted"]; ok {
					if preaccepted_int64, ok := preaccepted_raw.(int64); ok {
						p_ok = true
						preaccepted = uint64(preaccepted_int64)
					}
				}

				if preaccepted_value_raw, ok := data["preaccepted_value"]; ok {
					if preaccepted_value_metadata, ok := preaccepted_value_raw.(Metadata); ok {
						pv_ok = true
						preaccepted_value = preaccepted_value_metadata
					}
				}

				if highest_ballot_accepted_raw, ok := data["highest_ballot_accepted"]; ok {
					if highest_ballot_accepted_int64, ok := highest_ballot_accepted_raw.(int64); ok {
						h_ok = true
						highest_ballot_accepted = uint64(highest_ballot_accepted_int64)
					}
				}

				if highest_accepted_value_raw, ok := data["highest_accepted_value"]; ok {
					if highest_accepted_value_metadata, ok := highest_accepted_value_raw.(Metadata); ok {
						hv_ok = true
						highest_accepted_value = highest_accepted_value_metadata
					}
				}

				if p_ok && pv_ok {
					prev, ok := preaccepted_counts[preaccepted]
					if !ok {
						prev = 0
					}

					preaccepted_counts[preaccepted] = prev + 1

					if prev+1 > preaccepted_max_count {
						preaccepted_max_count = prev + 1
						preaccepted_max_val = preaccepted_value
					}
				}

				if h_ok && hv_ok {
					if highest_ballot_accepted > highest_accepted_max {
						highest_accepted_max = highest_ballot_accepted
						highest_accepted_val = highest_accepted_value
					}
				}
			} else {
				// There should only be 1 result
				n_fail += 1
			}

			if n_success >= CLASSIC_QUORUM {
				// Received more than a majority of responses
				break select_result_loop
			}

			if len(g.peers)-n_fail < CLASSIC_QUORUM {
				// More than half the peers failed to respond.

				*select_results_p = nil
				close(select_results)

				return errors.New("More than half the peers failed to respond.")
			}
		}
	}

	var value Metadata
	send_self := false

	if highest_accepted_max > 0 {
		// Case 1: accept the value with the highest accepted ballot
		value = highest_accepted_val
		ballot = int64(highest_accepted_max)
	} else if preaccepted_max_count > 0 {
		// Case 2: accept the most popular pre-accepted value
		value = preaccepted_max_val
	} else {
		// Case 3: no real contention, send accept request with own value
		value = metadata
		send_self = true
	}

	cas_results = make(chan bool)
	cas_results_p = &cas_results

	ctx, _ = g.CASAll(cas_results_p, `UPDATE state
		SET highest_ballot_accepted = ?, highest_value_accepted = ?
		WHERE object_id = ? AND version = ?
		IF highest_ballot_seen <= ? AND highest_ballot_accepted <= ?`,
		ballot, value, object_id, version, ballot, ballot)

cas_accept_result_loop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case success := <-cas_results:
			if success {
				n_success += 1
			} else {
				n_fail += 1
			}

			if n_success >= CLASSIC_QUORUM {
				// Successful write: commit the value
				break cas_accept_result_loop
			}

			if len(g.peers)-n_fail < CLASSIC_QUORUM {
				// Too many peers failed to achieve fast quorum

				*cas_results_p = nil
				close(cas_results)

				// Try again, ballot will be incremented
				return g.WriteSlow(object_id, version, metadata)
			}
		}
	}

	*cas_results_p = nil
	close(cas_results)

	// Asynchronously commit
	go g.Commit(object_id, version, metadata)

	if !send_self {
		return g.WriteFast(object_id, metadata)
	}

	return nil
}
