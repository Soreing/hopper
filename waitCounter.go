package hopper

import "sync"

// waitCounter has exposed counter and an internal wait group.
type waitCounter struct {
	ctr int
	wg  *sync.WaitGroup
}

// newWaitCounter creates a new wait counter.
func newWaitCounter() *waitCounter {
	return &waitCounter{
		ctr: 0,
		wg:  &sync.WaitGroup{},
	}
}

// add increments the counter and calls add on the internal waitgroup.
func (wg *waitCounter) add(n int) {
	wg.wg.Add(n)
}

// done decrements the counter and calls done on the internal waitgroup.
func (wg *waitCounter) done() {
	wg.wg.Done()
}

// wait calls wait on the internal waitgroup.
func (wg *waitCounter) wait() {
	wg.wg.Wait()
}
