package giza

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

	commits chan CommitValue

	session *gocql.Session
	peers   []*gocql.Session // All the cassandra tables in the network. Should include self.

	fastQuorum int
	classicQuorum int

	timeout time.Duration
}

type CommitValue struct {
	object_id gocql.UUID
	version uint
	metadata []byte
	preaccepted_by uint
}

type CASResult struct {
	success bool
	value  map[string]interface{}
}

func (g *Giza) Init(ip string, masterIp string, timeout time.Duration, numReplicas int, numObjects int) ([]gocql.UUID, error) {
	var err error


	// Set an id using the last 16 bits of the time in nanoseconds (repeats every
	// 1 min) and 16 random bits
	now := time.Now().UnixMilli()
	g.id = (uint32(uint16(now)) << 16) + (uint32(rand.Int31())>>16)

	isMaster := ip == masterIp

	g.session, err = OpenCassandra(ip)
	if err != nil {
		return nil, err
	}

	if err := CreateTable(g.session); err != nil {
		return nil, err
	}

	g.session.Query("TRUNCATE TABLE state").Exec()

	var masterSession *gocql.Session

	if isMaster {
		if err := CreatePeersTable(g.session); err != nil {
			return nil, err
		}

		g.session.Query("TRUNCATE TABLE peers").Exec()
		g.session.Query(`INSERT INTO peers(ip, num_replicas, num_objects)
			VALUES(?, ?, ?)`, ip, numReplicas, numObjects).Exec()

		masterSession = g.session
		fmt.Printf("Waiting for %d replicas.\n", numReplicas)
	} else {
		masterSession, err = OpenCassandra(masterIp)
		if err != nil {
			return nil, err
		}

		masterSession.Query("INSERT INTO peers(ip) VALUES(?)", ip).Exec()

		// Pull the number of records from the master table.
		numReplicas = -1
		numObjects = -1
		for numReplicas == -1 || numObjects == -1 {
			err = masterSession.Query(`SELECT num_replicas, num_objects
				FROM peers
				WHERE ip = ?`,
				masterIp).Scan(&numReplicas, &numObjects)

			if err != nil && err != gocql.ErrNotFound {
				return nil, err
			}

			time.Sleep(100 * time.Millisecond)
		}
	}

	var scanner gocql.Scanner

	for {
		iter := masterSession.Query("SELECT ip FROM peers").PageSize(numReplicas).Iter()

		if iter.NumRows() == numReplicas {
			scanner = iter.Scanner()
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	g.peers = make([]*gocql.Session, 0, numReplicas)
	g.peers = append(g.peers, g.session)
	if !isMaster {
		g.peers = append(g.peers, masterSession)
	} else {
		fmt.Println("Received all replicas, connecting...")
	}

	for scanner.Next() {
		var peerIp string

		err = scanner.Scan(&peerIp)
		if err != nil {
			return nil, err
		}

		if peerIp == ip || peerIp == masterIp {
			continue
		}
		
		peerSession, err := OpenCassandra(peerIp)
		if err != nil {
			return nil, err
		}

		g.peers = append(g.peers, peerSession)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	g.timeout = timeout

	if isMaster {
		for i := 0; i < numObjects; i++ {
			id, err := gocql.RandomUUID()
			if err != nil {
				return nil, err
			}
			g.Create(id)
		}

		fmt.Printf("Creating %d objects.\n", numObjects)
	}

	for {
		iter := masterSession.Query("SELECT object_id FROM state").PageSize(numObjects).Iter()

		if iter.NumRows() == numObjects {
			scanner = iter.Scanner()
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	var objectIds = make([]gocql.UUID, 0, numObjects)

	for scanner.Next() {
		var objectId gocql.UUID

		err = scanner.Scan(&objectId)
		if err != nil {
			return nil, err
		}

		objectIds = append(objectIds, objectId)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	g.done = make([]chan struct{}, 0, 1)
	g.fastQuorum = (3 * numReplicas / 4) + 1
	g.classicQuorum = (numReplicas / 2) + 1
	g.commits = make(chan CommitValue, 100)

	fmt.Println("Ready.")

	go g.StartCommitWorker()

	return objectIds, nil
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
	wg, _, _ := g.CASAll(nil, `INSERT INTO state(object_id, version, committed, highest_ballot_accepted, highest_ballot_seen)
		VALUES(?, ?, False, 0, 0)
		IF NOT EXISTS`,
		object_id, version)
	wg.Wait()

	wg, _, _ = g.CASAll(nil, `UPDATE state
		SET highest_ballot_accepted = 0, highest_ballot_seen = 0
		WHERE object_id = ? AND version = ?
		IF highest_ballot_seen = null`,
		object_id, version)
	wg.Wait()
}

func (g *Giza) Commit(object_id gocql.UUID, version uint, metadata []byte, preaccepted_by uint) {
	g.commits <- CommitValue{ object_id, version, metadata, preaccepted_by }
}

func (g *Giza) StartCommitWorker() {
	for {
		c := <- g.commits 

		wg, _, _ := g.ExecAll(`UPDATE state
			SET known_committed_versions = known_committed_versions + ?, value = ?, committed = True
			WHERE object_id = ? AND version = ?`,
			[]int{int(c.version)}, c.metadata, c.object_id, int(c.version))

		wg.Wait()

		wg, _, _ = g.CASAll(nil, `UPDATE state
			SET highest_known_committed_version = ?
			WHERE object_id = ?
			IF highest_known_committed_version < ?`,
			c.version, c.object_id, c.version)

		wg.Wait()

		if c.preaccepted_by != 0 {
			g.ClearPreaccept(c.object_id, c.preaccepted_by)
		}

		if (atomic.AddInt32(&g.waiting, -1) == 0) {
			for _, ch := range g.done {
				ch <- struct{}{}
			}
		}
	}
}

func (g *Giza) ClearPreaccept(object_id gocql.UUID, preaccepted_by uint) {
	wg, _, _ := g.CASAll(nil, `UPDATE state
		SET preaccepted = null, preaccepted_value = null
		WHERE object_id = ?
		IF preaccepted = ?`,
		object_id, preaccepted_by)

	wg.Wait()
}

func (g *Giza) ExecAll(stmt string, values ...interface{}) (*sync.WaitGroup, context.Context, context.CancelFunc) {
	wg := &sync.WaitGroup{}
	parent_ctx, cancel := context.WithCancel(context.Background())
	for _, peer := range g.peers {
		wg.Add(1)
		go (func(peer *gocql.Session) {
			defer wg.Done()

			ctx, child_cancel := context.WithTimeout(parent_ctx, g.timeout)
			defer child_cancel()

			err := peer.Query(stmt, values...).WithContext(ctx).Exec()

			if err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
		})(peer)
	}

	return wg, parent_ctx, cancel
}

func (g *Giza) SelectAll(results *chan []map[string]interface{}, stmt string, values ...interface{}) (*sync.WaitGroup, context.Context, context.CancelFunc) {
	wg := &sync.WaitGroup{}
	parent_ctx, cancel := context.WithCancel(context.Background())
	for _, peer := range g.peers {
		wg.Add(1)
		go (func(peer *gocql.Session) {
			defer wg.Done()

			ctx, child_cancel := context.WithTimeout(parent_ctx, g.timeout)
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
		})(peer)
	}

	return wg, parent_ctx, cancel
}

func (g *Giza) CASAll(results *chan CASResult, stmt string, values ...interface{}) (*sync.WaitGroup, context.Context, context.CancelFunc) {
	wg := &sync.WaitGroup{}
	parent_ctx, cancel := context.WithCancel(context.Background())
	for _, peer := range g.peers {
		wg.Add(1)
		go (func(peer *gocql.Session) {
			defer wg.Done()

			ctx, child_cancel := context.WithTimeout(parent_ctx, g.timeout)
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
		})(peer)
	}

	return wg, parent_ctx, cancel
}

func (g *Giza) Create(id gocql.UUID) {
	wg, _, _ := g.CASAll(nil, `INSERT INTO state(object_id, version, known_committed_versions, committed, highest_known_committed_version, highest_ballot_accepted, highest_ballot_seen)
		VALUES(?, 0, {0}, True, 0, 0, 0)
		IF NOT EXISTS`,
		id)
	wg.Wait()
}

func (g *Giza) getHighestCommittedVersion(object_id gocql.UUID) (uint, error) {
	var last_version uint

	err := g.session.Query(`SELECT highest_known_committed_version
		FROM state
		WHERE object_id = ?`,
		object_id).Scan(&last_version)

	if err != nil {
		return 0, err
	}

	return last_version, nil
}

func (g *Giza) WriteFast(object_id gocql.UUID, metadata []byte, version uint) error {
	if version == 0 {
		last_version, err := g.getHighestCommittedVersion(object_id)
		if err != nil {
			return err
		}

		version = last_version + 1
	}
	fmt.Println("TARGET", version)

	n_success := 0
	n_fail := 0
	highest_version := version
	if highest_version != 0 {
		highest_version -= 1
	}

	results := make(chan CASResult)
	results_p := &results

	_, ctx, _ := g.CASAll(results_p, `UPDATE state USING TTL 60
		SET preaccepted = ?, preaccepted_value = ?, committed = false
		WHERE object_id = ? AND version = ?
		IF preaccepted = null AND highest_known_committed_version < ?`,
		g.id, metadata, object_id, version, version)

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

			if n_success >= g.fastQuorum {
				// Successful write: commit the value
				break result_loop
			}

			if len(g.peers)-n_fail < g.fastQuorum {
				// Too many peers failed to achieve fast quorum

				*results_p = nil
				g.ClearPreaccept(object_id, uint(g.id))

				return g.WriteSlow(object_id, highest_version + 1, metadata)
			}
		}
	}

	*results_p = nil

	atomic.AddInt32(&g.waiting, 1)

	// Asynchronously commit and clean up
	go func() {
		fmt.Println("FAST", object_id, version)
		g.Commit(object_id, version, metadata, uint(g.id))
	}()

	return nil
}

func (g *Giza) WriteSlow(object_id gocql.UUID, version uint, metadata []byte) error {
	var last_highest_ballot int64

	if version == 0 {
		last_version, err := g.getHighestCommittedVersion(object_id)
		if err != nil {
			return err
		}

		version = last_version + 1
	}

	g.CreateVersion(object_id, version)

	err := g.session.Query(`SELECT highest_ballot_seen
		FROM state
		WHERE object_id = ? AND version = ?`,
		object_id, version).Scan(&last_highest_ballot)

	if err != nil {
		return err
	}

	ballot := g.incrementBallot(last_highest_ballot)

	fmt.Println("TRY SLOW", object_id, version, ballot)

	if success, err := g.writeSlowPrepare(object_id, version, ballot); err != nil {
		return err
	} else if !success {
		// Try again, with a higher ballot number
		return g.WriteSlow(object_id, 0, metadata)
	}

	preaccepted_max_count, preaccepted_max_val, highest_accepted_max, highest_accepted_val, err := g.writeSlowQuery(object_id, version)
	if err != nil {
		return nil
	}

	var value RawMetadata
	send_self := false

	if highest_accepted_max > 0 {
		// Case 1: accept the value with the highest accepted ballot
		value = highest_accepted_val
		ballot = int64(highest_accepted_max)
	} else if preaccepted_max_count > 0 {
		// Case 2: accept the most popular pre-accepted value
		// version = int64(preaccepted_max_ver)
		value = preaccepted_max_val
	} else {
		// Case 3: no real contention, send accept request with own value
		value = metadata
		send_self = true
	}

	if success, already_committed, err := g.writeSlowCommit(object_id, version, ballot, &value); err != nil {
		return err
	} else if !success {
		// Try again, with a higher ballot number

		if already_committed {
			return g.WriteFast(object_id, metadata, version + 1)
		}

		return g.WriteSlow(object_id, 0, metadata)
	}

	// Asynchronously commit

	atomic.AddInt32(&g.waiting, 1)
	fmt.Println("SLOW", object_id, version)
	if !send_self {
		go g.Commit(object_id, version, value, 0)
		return g.WriteFast(object_id, metadata, version + 1)
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
			}

			if n_success >= g.classicQuorum {
				// Successful write
				return true, nil
			}

			if len(g.peers)-n_fail < g.classicQuorum {
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
				n_success += 1
				data := result[0]
				pd := parsePaxosData(data)

				if pd.preaccepted != nil && pd.preaccepted_value != nil {
					prev, ok := preaccepted_counts[*pd.preaccepted]
					if !ok {
						prev = 0
					}

					preaccepted_counts[*pd.preaccepted] = prev + 1

					if prev+1 > preaccepted_max_count {
						preaccepted_max_count = prev + 1
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

			if n_success >= g.classicQuorum {
				// Received more than a majority of responses
				return
			}

			if len(g.peers)-n_fail < g.classicQuorum {
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
		*results_p = nil
	}()


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

			if n_success >= g.classicQuorum {
				// Successful write: commit the value
				return true, false, nil
			}

			if len(g.peers)-n_fail < g.classicQuorum {
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
