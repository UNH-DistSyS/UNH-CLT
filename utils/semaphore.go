package utils

type empty struct {}
type Semaphore chan empty

func NewSemaphore(capacity int) Semaphore{
	s := make(Semaphore, capacity)
	return s
}

func (s Semaphore) Acquire(numResources int) {
	e := empty{}
	for i := 0; i < numResources; i++ {
		s <- e
	}
}

func (s Semaphore) Release(numResources int) {
	for i := 0; i < numResources; i++ {
		<-s
	}
}

func (s Semaphore) GetCapacity() int {
	return cap(s)
}

func (s Semaphore) GetUtilization() int {
	return len(s)
}
