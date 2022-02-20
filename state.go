package main

import (
	"errors"
	"strconv"
	"strings"

	"github.com/gocql/gocql"
)

type Metadata struct {
}

type PaxosState struct {
	HighestBallotSeen     int64
	HighestBallotAccepted int64
	HighestValueAccepted  Metadata
}

type State struct {
	ObjectId               gocql.UUID
	Etag                   int64
	KnownCommittedVersions []int
	PaxosStates            map[int]*PaxosState
}

// ParseState converts a map[string]interface{} as returned by queries into a
// State struct.
func ParseState(raw map[string]interface{}) (*State, error) {
	var ok bool
	var id gocql.UUID
	var etag int64
	var committed_versions []int
	paxos_states := make(map[int]*PaxosState)

	for key, value := range raw {
		switch key {
		// Parse the fixed values
		case "object_id":
			id, ok = value.(gocql.UUID)
			if !ok {
				return nil, errors.New("Invalid " + key + " type")
			}
		case "etag":
			etag, ok = value.(int64)
			if !ok {
				return nil, errors.New("Invalid " + key + " type")
			}
		case "known_committed_versions":
			committed_versions, ok = value.([]int)
			if !ok {
				return nil, errors.New("Invalid " + key + " type")
			}

		// Parse the paxos states for each version
		default:
			// All paxos states are of the format version_key, so we find the first
			// "_" and use it to extract the version number and key.
			divider := strings.Index(key, "_")
			if divider < 0 {
				return nil, errors.New("Invalid paxos state key: " + key)
			}

			version, err := strconv.Atoi(key[:divider])
			if err != nil {
				return nil, errors.New("Invalid paxos state key: " + key)
			}

			state_key := key[divider:]

			// Create a new entry in the states map if the version does not exist.
			if _, ok := paxos_states[version]; !ok {
				paxos_states[version] = &PaxosState{}
			}

			switch state_key {
			case "highest_ballot_seen":
				val, ok := value.(int64)
				if !ok {
					return nil, errors.New("Invalid " + key + " type")
				}
				paxos_states[version].HighestBallotSeen = val
			case "highest_ballot_accepted":
				val, ok := value.(int64)
				if !ok {
					return nil, errors.New("Invalid " + key + " type")
				}
				paxos_states[version].HighestBallotAccepted = val
			case "highest_value_accepted":
				val, ok := value.(Metadata)
				if !ok {
					return nil, errors.New("Invalid " + key + " type")
				}
				paxos_states[version].HighestValueAccepted = val
			default:
				return nil, errors.New("Invalid key: " + key)
			}
		}
	}

	state := &State{
		ObjectId:               id,
		Etag:                   etag,
		KnownCommittedVersions: committed_versions,
		PaxosStates:            paxos_states,
	}

	return state, nil
}
