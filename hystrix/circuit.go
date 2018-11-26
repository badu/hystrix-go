package hystrix

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type (
	// CircuitBreaker is created for each ExecutorPool to track whether requests
	// should be attempted, or rejected if the Health of the circuit is too low.
	CircuitBreaker struct {
		Name                   string
		open                   bool
		forceOpen              bool
		mutex                  *sync.RWMutex
		openedOrLastTestedTime int64

		executorPool *executorPool
		metrics      *metricExchange
	}
)

var (
	circuitBreakersMutex *sync.RWMutex
	circuitBreakers      map[string]*CircuitBreaker
)

func init() {
	circuitBreakersMutex = &sync.RWMutex{}
	circuitBreakers = make(map[string]*CircuitBreaker)
}

// GetCircuit returns the circuit for the given command and whether this call created it.
func GetCircuit(name string) (*CircuitBreaker, bool, error) {
	circuitBreakersMutex.RLock()
	_, ok := circuitBreakers[name]
	if !ok {
		circuitBreakersMutex.RUnlock()
		circuitBreakersMutex.Lock()
		defer circuitBreakersMutex.Unlock()
		// because we released the rlock before we obtained the exclusive lock,
		// we need to double check that some other thread didn't beat us to
		// creation.
		if cb, ok := circuitBreakers[name]; ok {
			return cb, false, nil
		}
		circuitBreakers[name] = newCircuitBreaker(name)
	} else {
		defer circuitBreakersMutex.RUnlock()
	}

	return circuitBreakers[name], !ok, nil
}

// Flush purges all circuit and metric information from memory.
func Flush() {
	circuitBreakersMutex.Lock()
	defer circuitBreakersMutex.Unlock()

	for name, cb := range circuitBreakers {
		cb.metrics.Reset()
		cb.executorPool.Metrics.Reset()
		delete(circuitBreakers, name)
	}
}

// newCircuitBreaker creates a CircuitBreaker with associated Health
func newCircuitBreaker(name string) *CircuitBreaker {
	c := &CircuitBreaker{}
	c.Name = name
	c.metrics = newMetricExchange(name)
	c.executorPool = newExecutorPool(name)
	c.mutex = &sync.RWMutex{}

	return c
}

// toggleForceOpen allows manually causing the fallback logic for all instances
// of a given command.
func (c *CircuitBreaker) toggleForceOpen(toggle bool) error {
	c, _, err := GetCircuit(c.Name)
	if err != nil {
		return err
	}

	c.forceOpen = toggle
	return nil
}

// IsOpen is called before any Command execution to check whether or
// not it should be attempted. An "open" circuit means it is disabled.
func (c *CircuitBreaker) IsOpen() bool {
	c.mutex.RLock()
	o := c.forceOpen || c.open
	c.mutex.RUnlock()

	if o {
		return true
	}

	if uint64(c.metrics.Requests().Sum(time.Now())) < getSettings(c.Name).RequestVolumeThreshold {
		return false
	}

	if !c.metrics.IsHealthy(time.Now()) {
		// too many failures, open the circuit
		c.setOpen()
		return true
	}

	return false
}

// AllowRequest is checked before a command executes, ensuring that circuit state and metric health allow it.
// When the circuit is open, this call will occasionally return true to measure whether the external service
// has recovered.
func (c *CircuitBreaker) AllowRequest() bool {
	return !c.IsOpen() || c.allowSingleTest()
}

func (c *CircuitBreaker) allowSingleTest() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	now := time.Now().UnixNano()
	openedOrLastTestedTime := atomic.LoadInt64(&c.openedOrLastTestedTime)
	if c.open && now > openedOrLastTestedTime+getSettings(c.Name).SleepWindow.Nanoseconds() {
		swapped := atomic.CompareAndSwapInt64(&c.openedOrLastTestedTime, openedOrLastTestedTime, now)
		if swapped {
			log.Printf("hystrix-go: allowing single test to possibly close circuit %v", c.Name)
		}
		return swapped
	}

	return false
}

func (c *CircuitBreaker) setOpen() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.open {
		return
	}

	log.Printf("hystrix-go: opening circuit %v", c.Name)

	c.openedOrLastTestedTime = time.Now().UnixNano()
	c.open = true
}

func (c *CircuitBreaker) setClose() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.open {
		return
	}

	log.Printf("hystrix-go: closing circuit %v", c.Name)

	c.open = false
	c.metrics.Reset()
}

// ReportEvent records command metrics for tracking recent error rates and exposing data to the dashboard.
func (c *CircuitBreaker) ReportEvent(eventTypes []string, start time.Time, runDuration time.Duration) error {
	if len(eventTypes) == 0 {
		return fmt.Errorf("no event types sent for metrics")
	}

	c.mutex.RLock()
	o := c.open
	c.mutex.RUnlock()
	if eventTypes[0] == "success" && o {
		c.setClose()
	}

	var concurrencyInUse float64
	if c.executorPool.Max > 0 {
		concurrencyInUse = float64(c.executorPool.ActiveCount()) / float64(c.executorPool.Max)
	}

	select {
	case c.metrics.Updates <- &commandExecution{
		Types:            eventTypes,
		Start:            start,
		RunDuration:      runDuration,
		ConcurrencyInUse: concurrencyInUse,
	}:
	default:
		return CircuitError{Message: fmt.Sprintf("metrics channel (%v) is at capacity", c.Name)}
	}

	return nil
}
