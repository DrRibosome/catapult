// Package launch handles running internal servers
package launcher

import "sync"

type Runner interface {
	Run() error
}

// RunAll runs all runners, returning the first error
func RunAll(runners ...Runner) error {
	firstErr := make(chan error, 1)
	once := new(sync.Once)
	record := func(err error) {
		once.Do(func() {
			firstErr <- err
		})
	}

	wg := new(sync.WaitGroup)
	wg.Add(len(runners))

	for _, r := range runners {
		go func(r Runner) {
			defer wg.Done()
			err := r.Run()
			record(err)
		}(r)
	}

	wg.Wait()
	return <-firstErr
}

type lambdaRunner struct {
	f func() error
}

func (r lambdaRunner) Run() error {
	return r.f()
}

// AsRunner converts lambda func into runner
func AsRunner(f func() error) Runner {
	return lambdaRunner{f}
}
