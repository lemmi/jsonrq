package jsonrq

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/pkg/errors"
)

// JSONRequest is the Interface the Poolworkers work on.
type JSONRequest interface {
	Request() *http.Request
	Data() interface{}
	SetErr(err error)
	Err() error
	Done()
}

// BasicRequest is a type to simplify satisfying the JSONRequest by embedding
// common functionality.
type BasicRequest struct {
	err error
	url string
}

func NewBasicRequest(url string) BasicRequest {
	return BasicRequest{url: url}
}

// Err returns the latest error
func (r *BasicRequest) Err() error {
	return r.err
}

// SetErr overwrites the last error with err. Never clears an error.
func (r *BasicRequest) SetErr(err error) {
	// Don't discard Errors
	if r.err == nil || err != nil {
		r.err = err
	}
}

// Request prepares an *http.Request for the workers to fetch and decode
func (r *BasicRequest) Request() *http.Request {
	request, err := http.NewRequest("GET", r.url, nil)
	r.SetErr(errors.Wrap(err, "Error creating BasicRequest"))
	return request
}

// Worker drains the in channel and processes all JSONRequests. If in is
// closed, it calls Done() on the supplied *sync.WorkGroup
func Worker(in <-chan JSONRequest, wg *sync.WaitGroup) {
	for r := range in {
		func(r JSONRequest) {
			resp, err := http.DefaultClient.Do(r.Request())
			if err != nil {
				r.SetErr(errors.Wrap(err, "HTTP: Error performing request"))
				return
			}

			defer func() {
				r.SetErr(resp.Body.Close())
			}()

			err = json.NewDecoder(resp.Body).Decode(r.Data())
			if err != nil {
				r.SetErr(errors.Wrap(err, "JSON: Error decoding response"))
				return
			}
		}(r)
		r.Done()
	}
	wg.Done()
}

// Pool manages a set of workers and provides an interface to schedule new
// JSONRequest.
type Pool struct {
	in chan JSONRequest
	wg *sync.WaitGroup
}

// Stop closes the input queue and waits for the the workers to finish.
func (p Pool) Stop() {
	close(p.in)
	p.wg.Wait()
}

// Do schedules new JSONRequest for the workers.
func (p Pool) Do(rqs ...JSONRequest) {
	for _, rq := range rqs {
		p.in <- rq
	}
}

// NewPool creates a new Pool with n workers.
func NewPool(n uint) Pool {
	p := Pool{
		in: make(chan JSONRequest),
		wg: new(sync.WaitGroup),
	}

	p.wg.Add(int(n))
	for i := uint(0); i < n; i++ {
		go Worker(p.in, p.wg)
	}

	return p
}

// Do will start a pool, schedule all provided JSONRequest and waits for their
// completion
func Do(rqs ...JSONRequest) {
	p := NewPool(uint(len(rqs)))
	p.Do(rqs...)
	p.Stop()
}

// DoN will schedule at most n requests at a time
func DoN(n uint, rqs ...JSONRequest) {
	if n < 1 {
		n = 1
	}
	p := NewPool(n)
	p.Do(rqs...)
	p.Stop()
}
