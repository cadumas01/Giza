package main

type Condition uint64

const (
	CondTrue                  Condition = iota // no args
	CondNotCommitted                           // args: version int
	CondHighestBallotSeen                      // args: ballot int64, version int
	CondHighestBallotAccepted                  // args: ballot int64, version int
)

type TestCondition struct {
	Condition Condition
	Args      []interface{}
}

func (c TestCondition) CheckArgs() bool {
	switch c.Condition {
	case CondTrue:
		return len(c.Args) == 0

	case CondNotCommitted:
		if len(c.Args) != 1 {
			return false
		}

		_, ok := c.Args[0].(int)
		if !ok {
			return false
		}

		return true

	case CondHighestBallotSeen:
		if len(c.Args) != 2 {
			return false
		}

		_, ok := c.Args[0].(int64)
		if !ok {
			return false
		}
		_, ok = c.Args[1].(int)
		if !ok {
			return false
		}

		return true

	case CondHighestBallotAccepted:
		if len(c.Args) != 2 {
			return false
		}

		_, ok := c.Args[0].(int64)
		if !ok {
			return false
		}
		_, ok = c.Args[1].(int)
		if !ok {
			return false
		}

		return true
	}

	return false
}

func (c TestCondition) Test(state *State) bool {
	switch c.Condition {
	case CondTrue:
		return true

	case CondNotCommitted:
		version := c.Args[0].(int)

		for _, v := range state.KnownCommittedVersions {
			if v == version {
				return false
			}
		}

		return true

	case CondHighestBallotSeen:
		ballot := c.Args[0].(int64)
		version := c.Args[1].(int)

		paxos_state, ok := state.PaxosStates[version]
		if !ok {
			return true
		}

		return ballot >= paxos_state.HighestBallotSeen

	case CondHighestBallotAccepted:
		ballot := c.Args[0].(int64)
		version := c.Args[1].(int)

		paxos_state, ok := state.PaxosStates[version]
		if !ok {
			return true
		}

		return ballot >= paxos_state.HighestBallotSeen && ballot >= paxos_state.HighestBallotAccepted
	}

	return false
}
