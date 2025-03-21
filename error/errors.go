package error

import "fmt"

type NodeError struct {
	Err        error
	LeaderAddr string
	LeaderID   string
	Term       uint64
}

func (e *NodeError) Error() string {
	return fmt.Sprintf("not leader, current leader %v at %s: %v", e.LeaderID, e.LeaderAddr, e.Err)
}

func (e *NodeError) Unwrap() error {
	return e.Err
}

func NewNodeError(err error, addr, id string, term uint64) error {
	return &NodeError{
		Err:        err,
		LeaderAddr: addr,
		LeaderID:   id,
		Term:       term,
	}
}
