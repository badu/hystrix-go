package hystrix

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type (
	runFunc       func() error
	fallbackFunc  func(error) error
	runFuncC      func(context.Context) error
	fallbackFuncC func(context.Context, error) error

	// A CircuitError is an error which models various failure states of execution,
	// such as the circuit being open or a timeout.
	CircuitError struct {
		Message string
	}
	// command models the state used for a single execution on a circuit. "hystrix command" is commonly
	// used to describe the pairing of your run/fallback functions with a circuit.
	command struct {
		sync.Mutex

		ticket      *struct{}
		start       time.Time
		errChan     chan error
		finished    chan bool
		circuit     *CircuitBreaker
		run         runFuncC
		fallback    fallbackFuncC
		runDuration time.Duration
		events      []string

		ticketChecked bool
		// Shared by the two goroutines. It ensures only the faster
		// goroutine runs errWithFallback() and reportAllEvent().
		returnOnce *sync.Once
	}
)

var (
	// ErrMaxConcurrency occurs when too many of the same named command are executed at the same time.
	ErrMaxConcurrency = CircuitError{Message: "max concurrency"}
	// ErrCircuitOpen returns when an execution attempt "short circuits". This happens due to the circuit being measured as unhealthy.
	ErrCircuitOpen = CircuitError{Message: "circuit open"}
	// ErrTimeout occurs when the provided function takes too long to execute.
	ErrTimeout = CircuitError{Message: "timeout"}
)

func (e CircuitError) Error() string {
	return "hystrix: " + e.Message
}

// Go runs your function while tracking the health of previous calls to it.
// If your function begins slowing down or failing repeatedly, we will block
// new calls to it for you to give the dependent service time to repair.
//
// Define a fallback function if you want to define some code to execute during outages.
func Go(name string, run runFunc, fallback fallbackFunc) chan error {
	runC := func(ctx context.Context) error {
		return run()
	}
	var fallbackC fallbackFuncC
	if fallback != nil {
		fallbackC = func(ctx context.Context, err error) error {
			return fallback(err)
		}
	}

	return GoWithContext(context.WithValue(context.Background(), "name", name), runC, fallbackC)
}

// GoWithContext runs your function while tracking the health of previous calls to it.
// If your function begins slowing down or failing repeatedly, we will block
// new calls to it for you to give the dependent service time to repair.
//
// Define a fallback function if you want to define some code to execute during outages.
func GoWithContext(ctx context.Context, run runFuncC, fallback fallbackFuncC) chan error {
	cmd := &command{
		run:        run,
		fallback:   fallback,
		start:      time.Now(),
		errChan:    make(chan error, 1),
		finished:   make(chan bool, 1),
		returnOnce: &sync.Once{},
	}

	// don't have methods with explicit params and returns
	// let data come in and out naturally, like with any closure
	// explicit error return to give place for us to kill switch the operation (fallback)
	circuit, _, err := GetCircuit(ctx.Value("name").(string))
	if err != nil {
		cmd.errChan <- err
		return cmd.errChan
	}
	cmd.circuit = circuit
	ticketCond := sync.NewCond(cmd)

	go cmd.goRun(ticketCond, ctx)
	go cmd.goWait(ticketCond, ctx)

	return cmd.errChan
}

// Do runs your function in a synchronous manner, blocking until either your function succeeds
// or an error is returned, including hystrix circuit errors
func Do(name string, run runFunc, fallback fallbackFunc) error {
	runC := func(ctx context.Context) error {
		return run()
	}
	var fallbackC fallbackFuncC
	if fallback != nil {
		fallbackC = func(ctx context.Context, err error) error {
			return fallback(err)
		}
	}

	return DoWithContext(context.WithValue(context.Background(), "name", name), runC, fallbackC)
}

// DoWithContext runs your function in a synchronous manner, blocking until either your function succeeds
// or an error is returned, including hystrix circuit errors
func DoWithContext(ctx context.Context, run runFuncC, fallback fallbackFuncC) error {
	done := make(chan struct{}, 1)

	r := func(ctx context.Context) error {
		err := run(ctx)
		if err != nil {
			return err
		}

		done <- struct{}{}
		return nil
	}

	f := func(ctx context.Context, e error) error {
		err := fallback(ctx, e)
		if err != nil {
			return err
		}

		done <- struct{}{}
		return nil
	}

	var errChan chan error
	if fallback == nil {
		errChan = GoWithContext(ctx, r, nil)
	} else {
		errChan = GoWithContext(ctx, r, f)
	}

	select {
	case <-done:
		return nil
	case err := <-errChan:
		return err
	}
}

func (c *command) reportAllEvent() {
	err := c.circuit.ReportEvent(c.events, c.start, c.runDuration)
	if err != nil {
		log.Printf(err.Error())
	}
}

// When the caller extracts error from returned errChan, it's assumed that
// the ticket's been returned to executorPool. Therefore, returnTicket() can
// not run after cmd.errorWithFallback().
func (c *command) returnTicket(ticketCond *sync.Cond) {
	c.Lock()
	// Avoid releasing before a ticket is acquired.
	for !c.ticketChecked {
		ticketCond.Wait()
	}
	c.circuit.executorPool.Return(c.ticket)
	c.Unlock()
}

func (c *command) goWait(ticketCond *sync.Cond, ctx context.Context) {
	timer := time.NewTimer(getSettings(ctx.Value("name").(string)).Timeout)
	defer timer.Stop()

	select {
	case <-c.finished:
		// returnOnce has been executed in another goroutine
	case <-ctx.Done():
		c.returnOnce.Do(func() {
			c.returnTicket(ticketCond)
			c.errorWithFallback(ctx, ctx.Err())
			c.reportAllEvent()
		})
		return
	case <-timer.C:
		c.returnOnce.Do(func() {
			c.returnTicket(ticketCond)
			c.errorWithFallback(ctx, ErrTimeout)
			c.reportAllEvent()
		})
		return
	}
}

func (c *command) goRun(ticketCond *sync.Cond, ctx context.Context) {
	defer func() { c.finished <- true }()

	// Circuits get opened when recent executions have shown to have a high error rate.
	// Rejecting new executions allows backends to recover, and the circuit will allow
	// new traffic when it feels a healthly state has returned.
	if !c.circuit.AllowRequest() {
		c.Lock()
		// It's safe for another goroutine to go ahead releasing a nil ticket.
		c.ticketChecked = true
		ticketCond.Signal()
		c.Unlock()
		c.returnOnce.Do(func() {
			c.returnTicket(ticketCond)
			c.errorWithFallback(ctx, ErrCircuitOpen)
			c.reportAllEvent()
		})
		return
	}

	// As backends falter, requests take longer but don't always fail.
	//
	// When requests slow down but the incoming rate of requests stays the same, you have to
	// run more at a time to keep up. By controlling concurrency during these situations, you can
	// shed load which accumulates due to the increasing ratio of active commands to incoming requests.
	c.Lock()
	select {
	case c.ticket = <-c.circuit.executorPool.Tickets:
		c.ticketChecked = true
		ticketCond.Signal()
		c.Unlock()
	default:
		c.ticketChecked = true
		ticketCond.Signal()
		c.Unlock()
		c.returnOnce.Do(func() {
			c.returnTicket(ticketCond)
			c.errorWithFallback(ctx, ErrMaxConcurrency)
			c.reportAllEvent()
		})
		return
	}

	runStart := time.Now()
	runErr := c.run(ctx)
	c.returnOnce.Do(func() {
		defer c.reportAllEvent()
		c.runDuration = time.Since(runStart)
		c.returnTicket(ticketCond)
		if runErr != nil {
			c.errorWithFallback(ctx, runErr)
			return
		}
		c.reportEvent("success")
	})
}

func (c *command) reportEvent(eventType string) {
	c.Lock()
	defer c.Unlock()

	c.events = append(c.events, eventType)
}

// errorWithFallback triggers the fallback while reporting the appropriate metric events.
func (c *command) errorWithFallback(ctx context.Context, err error) {
	eventType := "failure"
	if err == ErrCircuitOpen {
		eventType = "short-circuit"
	} else if err == ErrMaxConcurrency {
		eventType = "rejected"
	} else if err == ErrTimeout {
		eventType = "timeout"
	} else if err == context.Canceled {
		eventType = "context_canceled"
	} else if err == context.DeadlineExceeded {
		eventType = "context_deadline_exceeded"
	}

	c.reportEvent(eventType)
	fallbackErr := c.tryFallback(ctx, err)
	if fallbackErr != nil {
		c.errChan <- fallbackErr
	}
}

func (c *command) tryFallback(ctx context.Context, err error) error {
	if c.fallback == nil {
		// If we don't have a fallback return the original error.
		return err
	}

	fallbackErr := c.fallback(ctx, err)
	if fallbackErr != nil {
		c.reportEvent("fallback-failure")
		return fmt.Errorf("fallback failed with '%v'. run error was '%v'", fallbackErr, err)
	}

	c.reportEvent("fallback-success")

	return nil
}
