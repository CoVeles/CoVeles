package notifier

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
)

var (
	// ErrQueueFull indicates that the notification queue is full and the caller
	// should retry later. This keeps Notify non-blocking while protecting the
	// HTTP client from unbounded bursts.
	ErrQueueFull = errors.New("notifier: queue is full")

	// ErrClosed is returned when Notify is called after the client has been
	// closed.
	ErrClosed = errors.New("notifier: client is closed")
)

// ErrorHandler receives failed notification attempts. Implementations should be
// non-blocking because they are invoked from the worker goroutines.
type ErrorHandler func(message string, err error)

// Option configures the client.
type Option func(*config)

type config struct {
	queueSize   int
	workerCount int
	httpClient  *http.Client
	onError     ErrorHandler
}

// WithQueueSize configures the queue capacity used to buffer notifications.
// It must be greater than zero.
func WithQueueSize(size int) Option {
	return func(c *config) {
		if size > 0 {
			c.queueSize = size
		}
	}
}

// WithWorkerCount sets the number of goroutines that will send notifications
// concurrently. It must be greater than zero.
func WithWorkerCount(count int) Option {
	return func(c *config) {
		if count > 0 {
			c.workerCount = count
		}
	}
}

// WithHTTPClient allows providing a custom http.Client. When nil the default
// http.Client is used.
func WithHTTPClient(client *http.Client) Option {
	return func(c *config) {
		if client != nil {
			c.httpClient = client
		}
	}
}

// WithErrorHandler registers a handler that is called whenever sending a
// notification fails.
func WithErrorHandler(handler ErrorHandler) Option {
	return func(c *config) {
		if handler != nil {
			c.onError = handler
		}
	}
}

// Client asynchronously delivers notifications to an HTTP endpoint.
type Client struct {
	url        string
	queue      chan string
	httpClient *http.Client
	onError    ErrorHandler

	wg        sync.WaitGroup
	closeOnce sync.Once
	done      chan struct{}
}

// NewClient creates a new Client that will POST notifications to the provided
// URL. The client immediately starts the background workers used to send
// requests.
func NewClient(url string, opts ...Option) (*Client, error) {
	if url == "" {
		return nil, errors.New("notifier: url must not be empty")
	}

	cfg := config{
		queueSize:   128,
		workerCount: 4,
		httpClient:  http.DefaultClient,
		onError:     func(string, error) {},
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	client := &Client{
		url:        url,
		queue:      make(chan string, cfg.queueSize),
		httpClient: cfg.httpClient,
		onError:    cfg.onError,
		done:       make(chan struct{}),
	}

	client.wg.Add(cfg.workerCount)
	for i := 0; i < cfg.workerCount; i++ {
		go client.worker()
	}

	return client, nil
}

// Notify schedules the message to be sent asynchronously. It is non-blocking;
// when the internal queue is full the caller receives ErrQueueFull and may
// retry later. Calling Notify after Close returns ErrClosed.
func (c *Client) Notify(message string) error {
	select {
	case <-c.done:
		return ErrClosed
	default:
	}

	select {
	case c.queue <- message:
		return nil
	default:
		return ErrQueueFull
	}
}

// Close stops accepting new notifications and waits for outstanding workers to
// finish processing already enqueued messages. It blocks until either all
// workers have finished or the context is cancelled.
func (c *Client) Close(ctx context.Context) error {
	c.closeOnce.Do(func() {
		close(c.done)
		close(c.queue)
	})

	finished := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(finished)
	}()

	select {
	case <-finished:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) worker() {
	defer c.wg.Done()

	for msg := range c.queue {
		if err := c.send(msg); err != nil {
			c.onError(msg, err)
		}
	}
}

func (c *Client) send(message string) error {
	req, err := http.NewRequest(http.MethodPost, c.url, bytes.NewBufferString(message))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "text/plain; charset=utf-8")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return errors.New("notifier: unexpected response status: " + resp.Status)
	}

	// Drain the body to allow the connection to be reused.
	_, _ = io.Copy(io.Discard, resp.Body)

	return nil
}
