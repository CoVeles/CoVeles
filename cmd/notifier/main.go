package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/CoVeles/notifier/notifier"
)

func main() {
	url := flag.String("url", "", "HTTP endpoint to post notifications to")
	interval := flag.Duration("interval", time.Second, "Interval between notification batches")
	flag.Parse()

	if *url == "" {
		fmt.Fprintln(os.Stderr, "-url flag is required")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client, err := notifier.NewClient(*url, notifier.WithErrorHandler(func(msg string, err error) {
		log.Printf("failed to deliver message %q: %v", msg, err)
	}))
	if err != nil {
		log.Fatalf("creating notifier client: %v", err)
	}

	if err := run(ctx, client, *interval); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("notifier stopped with error: %v", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Close(shutdownCtx); err != nil {
		log.Printf("graceful shutdown incomplete: %v", err)
	}
}

func run(ctx context.Context, client *notifier.Client, interval time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("interval must be positive, got %s", interval)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	messages := make([]string, 0)
	scanner := bufio.NewScanner(os.Stdin)

	inputCh := make(chan string)
	errCh := make(chan error, 1)

	go func() {
		defer close(inputCh)
		defer close(errCh)

		for scanner.Scan() {
			select {
			case inputCh <- scanner.Text():
			case <-ctx.Done():
				return
			}
		}

		if err := scanner.Err(); err != nil {
			errCh <- err
		}
	}()

	for {
		select {
		case <-ctx.Done():
			drain(inputCh, &messages)
			dispatch(ctx, client, messages)
			return ctx.Err()
		case msg, ok := <-inputCh:
			if !ok {
				dispatch(ctx, client, messages)
				return collectError(errCh)
			}
			messages = append(messages, msg)
		case <-ticker.C:
			if len(messages) == 0 {
				continue
			}
			dispatch(ctx, client, messages)
			messages = messages[:0]
		case err, ok := <-errCh:
			if !ok {
				errCh = nil
				continue
			}
			if err != nil {
				return err
			}
		}
	}
}

func collectError(errCh <-chan error) error {
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func drain(input <-chan string, buffer *[]string) {
	for {
		select {
		case msg, ok := <-input:
			if !ok {
				return
			}
			*buffer = append(*buffer, msg)
		default:
			return
		}
	}
}

func dispatch(ctx context.Context, client *notifier.Client, messages []string) {
	for _, msg := range messages {
		deliver(ctx, client, msg)
	}
}

func deliver(ctx context.Context, client *notifier.Client, message string) {
	backoff := 50 * time.Millisecond
	for {
		err := client.Notify(message)
		if err == nil {
			return
		}

		if errors.Is(err, notifier.ErrClosed) {
			log.Printf("dropping message because client is closed: %q", message)
			return
		}

		if !errors.Is(err, notifier.ErrQueueFull) {
			log.Printf("failed to enqueue message %q: %v", message, err)
			return
		}

		select {
		case <-ctx.Done():
			log.Printf("context cancelled before message could be enqueued: %q", message)
			return
		case <-time.After(backoff):
			if backoff < time.Second {
				backoff *= 2
			}
		}
	}
}
