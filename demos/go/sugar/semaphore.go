package sugar

type Semaphore struct {
	ch chan struct{}
}

func newSemaphore(n int) *Semaphore {
	return &Semaphore{
		ch: make(chan struct{}, n),
	}
}

func (s *Semaphore) Acquire() {
	s.ch <- struct{}{}
}
func (s *Semaphore) Release() {
	<-s.ch
}
func (s *Semaphore) Used() int {
	return len(s.ch)
}
func (s *Semaphore) Free() int {
	return len(s.ch) - cap(s.ch)
}
func (s *Semaphore) Cap() int {
	return cap(s.ch)
}
